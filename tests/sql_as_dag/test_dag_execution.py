# Copyright 2026 Hussein Awala
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
End-to-end tests that execute the DAG's own task callables.

Other tests compose the runtime helpers directly, which leaves the code inside the mapped
``read`` / ``compute`` / ``write`` tasks — and the Parquet round-trips between them — untested.
Here the real callables are pulled off the built DAG and driven in dependency order, so this is
the same code a worker runs, minus the scheduler.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.utils.trigger_rule import TriggerRule

from sql_as_dag.compiler import compile_sql
from sql_as_dag.dag import dag_from_stages
from sql_as_dag.ir import Sink, Source
from sql_as_dag.runtime.executor import execute_sql

if TYPE_CHECKING:
    from sql_as_dag.ir import StageGraph

_PARTITIONS = [
    {"customer_id": ["a", "b", "a", "c", "b"], "amount": [10, 20, 30, 40, 50]},
    {"customer_id": ["a", "d", "c", "b", "a"], "amount": [60, 70, 80, 90, 100]},
]


def _materialize(tmp_path: Path, name: str, partitions: list[dict]) -> list[str]:
    d = tmp_path / name
    d.mkdir(parents=True, exist_ok=True)
    uris = []
    for i, data in enumerate(partitions):
        p = d / f"{name}_{i}.parquet"
        pq.write_table(pa.table(data), p)
        uris.append(p.resolve().as_uri())
    return uris


def _run_dag_tasks(dag: Any, graph: StageGraph) -> tuple[dict, dict[str, list[dict]]]:
    """
    Execute the DAG's task callables in dependency order, like a minimal scheduler.

    Returns ``(finalize_result, stage_outputs)``. Only static bucketing is driven here, so there
    is no ``plan_shuffle_width_*`` task to run.
    """

    def callable_for(task_id: str):
        return dag.get_task(task_id).python_callable

    work_dir = callable_for("make_work_dir")()
    stage_outputs: dict[str, list[dict]] = {}

    for stage in graph.stages:
        sid = stage.stage_id
        if stage.inputs[0].source_id is not None:
            kwargs_list = callable_for(f"plan_partitions_{sid}")(work_dir, stage.output_exchange.num_buckets)
        else:
            upstream = [stage_outputs[i.upstream_stage_id] for i in stage.inputs]
            kwargs_list = callable_for(f"gather_shuffle_{sid}")(upstream, work_dir)

        read = callable_for(f"stage_{sid}.read")
        compute = callable_for(f"stage_{sid}.compute")
        write = callable_for(f"stage_{sid}.write")

        outputs = []
        for kwargs in kwargs_list:
            scratch = read(kwargs["input_spec"], kwargs["output_dir"])
            computed = compute(scratch, kwargs["output_dir"])
            outputs.append(
                write(
                    computed,
                    kwargs["output_dir"],
                    kwargs["partition_id"],
                    kwargs["num_buckets"],
                    kwargs["run_key"],
                )
            )
        stage_outputs[sid] = outputs

    result = callable_for("finalize")(stage_outputs[graph.sink_stage_id], work_dir)
    return result, stage_outputs


def _latest_run_dir(base_uri: str) -> Path:
    """The run directory the Parquet sink's ``_LATEST`` pointer names."""
    base = Path(base_uri.removeprefix("file://"))
    return base / (base / "_LATEST").read_text()


def _read_sink(base_uri: str) -> pa.Table | None:
    """Read every ``p*/data.parquet`` of the latest run, or None when it wrote nothing."""
    files = sorted(_latest_run_dir(base_uri).glob("p*/data.parquet"))
    if not files:
        return None
    return pa.concat_tables([pq.read_table(f) for f in files])


def _sorted_rows(table: pa.Table) -> list[tuple]:
    return sorted(tuple(row[name] for name in table.column_names) for row in table.to_pylist())


def _build_dag(sql: str, sources: list[Source], out_dir: Path, *, dag_id: str, num_buckets: int = 3):
    graph = compile_sql(sql, sources, num_buckets=num_buckets)
    dag = dag_from_stages(
        graph,
        dag_id=dag_id,
        sink=Sink("parquet", {"base_uri": out_dir.resolve().as_uri()}),
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
    )
    return graph, dag


def test_mapped_tasks_execute_end_to_end(tmp_path: Path) -> None:
    """The real read->compute->write tasks must produce the single-node answer."""
    sql = "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
    source = Source(
        source_id="orders_src",
        table_name="orders",
        options={"uris": _materialize(tmp_path, "orders", _PARTITIONS)},
    )
    out = tmp_path / "out"
    graph, dag = _build_dag(sql, [source], out, dag_id="exec_groupby")

    result, _ = _run_dag_tasks(dag, graph)

    written = _read_sink(out.resolve().as_uri())
    whole = pa.concat_tables([pa.table(p) for p in _PARTITIONS])
    reference = execute_sql({"orders": whole}, sql)

    assert written is not None
    assert written.column_names == reference.column_names
    assert _sorted_rows(written) == _sorted_rows(reference)
    assert result["rows"] == reference.num_rows
    assert (_latest_run_dir(out.resolve().as_uri()) / "_SUCCESS").exists()


def test_join_mapped_tasks_execute_end_to_end(tmp_path: Path) -> None:
    """Same, for the two-scan join shape, which also exercises the reduce-side gather."""
    orders = [
        {"customer_id": ["a", "b", "c"], "amount": [10, 20, 30]},
        {"customer_id": ["a", "d"], "amount": [40, 50]},
    ]
    customers = [
        {"customer_id": ["a", "b"], "name": ["Ann", "Bob"]},
        {"customer_id": ["c", "e"], "name": ["Cid", "Eve"]},
    ]
    sql = "SELECT o.customer_id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.customer_id"
    sources = [
        Source(source_id="orders_src", table_name="orders", options={"uris": _materialize(tmp_path, "orders", orders)}),
        Source(
            source_id="customers_src",
            table_name="customers",
            options={"uris": _materialize(tmp_path, "customers", customers)},
        ),
    ]
    out = tmp_path / "out"
    graph, dag = _build_dag(sql, sources, out, dag_id="exec_join")

    _run_dag_tasks(dag, graph)

    written = _read_sink(out.resolve().as_uri())
    reference = execute_sql(
        {
            "orders": pa.concat_tables([pa.table(p) for p in orders]),
            "customers": pa.concat_tables([pa.table(p) for p in customers]),
        },
        sql,
    )
    assert written is not None
    assert _sorted_rows(written) == _sorted_rows(reference)


def test_empty_source_partition_does_not_crash(tmp_path: Path) -> None:
    """
    A source file with zero rows must flow through the mapped tasks.

    A zero-row Parquet file round-trips to an Arrow table with no record batches, which used to
    make DataFusion abort the task with a Rust panic instead of returning an empty result. Date
    partitioned tables produce empty files routinely.
    """
    # An empty file from a real table still carries the table's schema, so build it explicitly
    # rather than from empty Python lists (which would infer null-typed columns).
    schema = pa.schema([("customer_id", pa.string()), ("amount", pa.int64())])
    tables = [
        pa.table({"customer_id": ["a", "b"], "amount": [10, 20]}, schema=schema),
        pa.table({"customer_id": [], "amount": []}, schema=schema),  # empty partition
        pa.table({"customer_id": ["a", "c"], "amount": [30, 40]}, schema=schema),
    ]
    d = tmp_path / "orders"
    d.mkdir()
    uris = []
    for i, table in enumerate(tables):
        p = d / f"orders_{i}.parquet"
        pq.write_table(table, p)
        uris.append(p.resolve().as_uri())

    sql = "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
    source = Source(source_id="orders_src", table_name="orders", options={"uris": uris})
    out = tmp_path / "out"
    graph, dag = _build_dag(sql, [source], out, dag_id="exec_empty_partition")

    _run_dag_tasks(dag, graph)

    written = _read_sink(out.resolve().as_uri())
    assert written is not None
    assert _sorted_rows(written) == [("a", 40), ("b", 20), ("c", 40)]


def test_empty_result_still_finalizes(tmp_path: Path) -> None:
    """
    A query matching no rows must still commit, reporting zero rows.

    With nothing to write, the terminal stage expands to zero mapped instances and Airflow marks
    it skipped; finalize must still run so that "returned nothing" is distinguishable from
    "never ran".
    """
    sql = "SELECT customer_id, SUM(amount) AS total FROM orders WHERE amount > 100000 GROUP BY customer_id"
    source = Source(
        source_id="orders_src",
        table_name="orders",
        options={"uris": _materialize(tmp_path, "orders", _PARTITIONS)},
    )
    out = tmp_path / "out"
    graph, dag = _build_dag(sql, [source], out, dag_id="exec_empty_result")

    result, stage_outputs = _run_dag_tasks(dag, graph)

    # Nothing to fan out to on the reduce side, hence the skipped expansion in a real run.
    assert stage_outputs[graph.sink_stage_id] == []
    assert result["rows"] == 0
    assert result["partitions"] == 0
    assert (_latest_run_dir(out.resolve().as_uri()) / "_SUCCESS").exists()
    assert _read_sink(out.resolve().as_uri()) is None


def test_finalize_trigger_rule_survives_a_skipped_expansion(tmp_path: Path) -> None:
    """
    finalize must not use the default all_success rule.

    A zero-length expansion is SKIPPED, which would cascade and skip finalize, leaving no commit
    while the DAG run still reported success. 'none_failed' runs after a skip but still refuses
    to commit when an upstream failed.
    """
    source = Source(
        source_id="orders_src",
        table_name="orders",
        options={"uris": _materialize(tmp_path, "orders", _PARTITIONS)},
    )
    _, dag = _build_dag(
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
        [source],
        tmp_path / "out",
        dag_id="exec_trigger_rule",
    )
    assert dag.get_task("finalize").trigger_rule == TriggerRule.NONE_FAILED
