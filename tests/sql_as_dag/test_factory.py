# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sql_as_dag.connectors.registry import get_sink, get_source
from sql_as_dag.dag import dag_from_stages
from sql_as_dag.dag.factory import _mint_work_dir
from sql_as_dag.ir import Exchange, Sink, Source, Stage, StageGraph, StageInput
from sql_as_dag.runtime.coordinator import gather_shuffle
from sql_as_dag.runtime.executor import execute_sql
from sql_as_dag.runtime.shuffle import hash_partition

_SINK = Sink("parquet", {"base_uri": "file:///tmp/sql_as_dag_unused"})


def _single_stage_graph(*, uris: list[str], exchange: Exchange | None = None) -> StageGraph:
    stage = Stage(
        stage_id="main",
        sql="SELECT customer_id, amount FROM orders WHERE amount > 25",
        inputs=[StageInput(table_name="orders", source_id="orders_src")],
        output_exchange=exchange or Exchange(),
    )
    src = Source(source_id="orders_src", table_name="orders", connector="parquet", options={"uris": uris})
    return StageGraph(sources=[src], stages=[stage], sink_stage_id="main")


def _two_stage_groupby_graph(*, uris: list[str], num_buckets: int = 3) -> StageGraph:
    partial = Stage(
        stage_id="partial",
        sql="SELECT customer_id, SUM(amount) AS __p_0 FROM orders GROUP BY customer_id",
        inputs=[StageInput(table_name="orders", source_id="orders_src")],
        output_exchange=Exchange(kind="hash_shuffle", keys=["customer_id"], num_buckets=num_buckets),
    )
    final = Stage(
        stage_id="final",
        sql="SELECT customer_id, SUM(__p_0) AS total FROM partials GROUP BY customer_id",
        inputs=[StageInput(table_name="partials", upstream_stage_id="partial")],
    )
    src = Source(source_id="orders_src", table_name="orders", options={"uris": uris})
    return StageGraph(sources=[src], stages=[partial, final], sink_stage_id="final")


def test_builds_expected_structure() -> None:
    dag = dag_from_stages(
        _single_stage_graph(uris=["file:///tmp/x.parquet"]),
        dag_id="struct_dag",
        sink=_SINK,
        schedule=None,
        start_date=datetime(2026, 1, 1),
    )
    assert set(dag.task_ids) >= {
        "make_work_dir",
        "plan_partitions_main",
        "stage_main.read",
        "stage_main.compute",
        "stage_main.write",
        "finalize",
    }


def test_builds_two_stage_groupby_structure() -> None:
    dag = dag_from_stages(
        _two_stage_groupby_graph(uris=["file:///tmp/x.parquet"]),
        dag_id="groupby_struct",
        sink=_SINK,
        schedule=None,
        start_date=datetime(2026, 1, 1),
    )
    assert set(dag.task_ids) >= {
        "make_work_dir",
        "plan_partitions_partial",
        "stage_partial.read",
        "stage_partial.compute",
        "stage_partial.write",
        "gather_shuffle_final",
        "stage_final.read",
        "stage_final.compute",
        "stage_final.write",
        "finalize",
    }


def test_rejects_shuffle_exchange() -> None:
    graph = _single_stage_graph(
        uris=["x"], exchange=Exchange(kind="hash_shuffle", keys=["customer_id"], num_buckets=2)
    )
    with pytest.raises(NotImplementedError, match="pipeline"):
        dag_from_stages(graph, dag_id="shuf", sink=_SINK, schedule=None, start_date=datetime(2026, 1, 1))


def test_rejects_multi_input_stage() -> None:
    stage = Stage(
        stage_id="main",
        sql="SELECT 1",
        inputs=[
            StageInput(table_name="a", source_id="s_a"),
            StageInput(table_name="b", source_id="s_b"),
        ],
    )
    sources = [Source(source_id="s_a", table_name="a"), Source(source_id="s_b", table_name="b")]
    graph = StageGraph(sources=sources, stages=[stage], sink_stage_id="main")
    with pytest.raises(NotImplementedError, match="sources directly"):
        dag_from_stages(graph, dag_id="multi_in", sink=_SINK, schedule=None, start_date=datetime(2026, 1, 1))


def test_mint_work_dir_uses_base_uri(tmp_path: Path) -> None:
    base = (tmp_path / "wh").resolve().as_uri()
    work_dir = _mint_work_dir(base, "my_dag", "manual__2026-01-01T00:00:00+00:00")
    assert work_dir.startswith(base)
    assert "/my_dag/" in work_dir
    # run_id is sanitized for path safety (no ':' or '+')
    assert ":" not in work_dir.removeprefix("file://")
    assert (tmp_path / "wh" / "my_dag").exists()


def test_mint_work_dir_defaults_to_tempdir() -> None:
    work_dir = _mint_work_dir(None, "my_dag", "")
    assert work_dir.startswith("file://")
    assert "my_dag_" in work_dir


def test_runtime_pipeline_end_to_end(tmp_path: Path) -> None:
    """
    Exercise the exact data path the mapped read->compute->write tasks run (source
    connector -> DataFusion -> sink -> finalize), proving the partitioned scan+filter
    matches the single-node result. DAG-level orchestration is covered by the system test
    example; here we validate the runtime composition without a scheduler.
    """
    inputs_dir = tmp_path / "orders"
    inputs_dir.mkdir()
    part0 = inputs_dir / "p0.parquet"
    part1 = inputs_dir / "p1.parquet"
    pq.write_table(pa.table({"customer_id": ["a", "b", "a", "c"], "amount": [10, 30, 50, 20]}), part0)
    pq.write_table(pa.table({"customer_id": ["b", "d", "a"], "amount": [40, 5, 90]}), part1)
    uris = [part0.resolve().as_uri(), part1.resolve().as_uri()]

    out_uri = (tmp_path / "out").resolve().as_uri()
    source = get_source("parquet")(uris=uris)
    sink = get_sink("parquet")(base_uri=out_uri)
    sql = "SELECT customer_id, amount FROM orders WHERE amount > 25"

    metas = []
    for partition_id, ref in enumerate(source.list_partitions()):
        table = source.read_partition(ref)
        result = execute_sql({"orders": table}, sql)
        metas.append(sink.write(result, partition_id=partition_id))
    summary = sink.finalize(metas)

    out_files = sorted((tmp_path / "out").glob("p*/data.parquet"))
    assert len(out_files) == 2
    rows: set[tuple[str, int]] = set()
    for f in out_files:
        t = pq.read_table(f)
        rows.update(zip(t.column("customer_id").to_pylist(), t.column("amount").to_pylist()))
    assert rows == {("b", 30), ("a", 50), ("b", 40), ("a", 90)}
    assert summary["rows"] == 4
    assert (tmp_path / "out" / "_SUCCESS").exists()


def test_groupby_runtime_pipeline(tmp_path: Path) -> None:
    """
    Exercise the full two-stage GROUP BY runtime path across the shuffle boundary:
    partial aggregate + hash_partition (map side) -> gather_shuffle -> final aggregate,
    and assert the per-customer totals match a single-node group-by.
    """
    partitions = [
        {"customer_id": ["a", "b", "a", "c", "b"], "amount": [10, 20, 30, 40, 50]},
        {"customer_id": ["a", "d", "c", "b", "a"], "amount": [60, 70, 80, 90, 100]},
    ]
    num_buckets = 3
    partial_sql = "SELECT customer_id, SUM(amount) AS __p_0 FROM orders GROUP BY customer_id"
    final_sql = "SELECT customer_id, SUM(__p_0) AS total FROM partials GROUP BY customer_id"

    # Map side: per source partition, partial-aggregate then hash-partition by customer_id.
    stage_outputs: list[dict] = []
    for pid, data in enumerate(partitions):
        result = execute_sql({"orders": pa.table(data)}, partial_sql)
        files: list[dict] = []
        for bucket, sub in hash_partition(result, ["customer_id"], num_buckets):
            path = tmp_path / f"partial_p{pid}_b{bucket}.parquet"
            pq.write_table(sub, path)
            files.append({"path": path.resolve().as_uri(), "bucket": bucket, "rows": sub.num_rows})
        stage_outputs.append({"partition_id": pid, "files": files, "rows_in": 0, "rows_out": 0})

    # Reduce side: gather files per bucket, final-aggregate each bucket.
    plan = gather_shuffle(stage_outputs, num_shuffle_buckets=num_buckets, output_base=tmp_path.as_uri())
    totals: dict[str, int] = {}
    for entry in plan:
        tables = [pq.read_table(p.removeprefix("file://")) for p in entry["input_paths"]]
        combined = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
        final = execute_sql({"partials": combined}, final_sql)
        for cid, total in zip(final.column("customer_id").to_pylist(), final.column("total").to_pylist()):
            totals[cid] = total

    # Single-node ground truth.
    assert totals == {"a": 200, "b": 160, "c": 120, "d": 70}
