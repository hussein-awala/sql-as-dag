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
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sql_as_dag.compiler import UnsupportedSQLError, compile_sql
from sql_as_dag.dag import dag_from_stages
from sql_as_dag.ir import Sink, Source
from sql_as_dag.runtime.coordinator import choose_join_strategy, gather_multi
from sql_as_dag.runtime.executor import execute_sql
from sql_as_dag.runtime.shuffle import hash_partition

_EXPECTED_JOIN = [
    ("a", 10, "Alice"),
    ("a", 30, "Alice"),
    ("a", 60, "Alice"),
    ("b", 20, "Bob"),
    ("b", 50, "Bob"),
    ("c", 40, "Carol"),
]


def _run_scan(tmp_path: Path, stage, table_name: str, partitions: list[dict], num_buckets: int) -> list[dict]:
    outputs = []
    for pid, data in enumerate(partitions):
        result = execute_sql({table_name: pa.table(data)}, stage.sql)
        files = []
        for bucket, sub in hash_partition(result, stage.output_exchange.keys, num_buckets):
            path = tmp_path / f"{stage.stage_id}_p{pid}_b{bucket}.parquet"
            pq.write_table(sub, path)
            files.append({"path": path.resolve().as_uri(), "bucket": bucket, "rows": sub.num_rows})
        outputs.append({"partition_id": pid, "files": files, "rows_in": 0, "rows_out": 0})
    return outputs


def _run_join_plan(plan: list[dict], join_sql: str) -> list[tuple]:
    rows: list[tuple] = []
    for entry in plan:
        tables = {}
        for table, paths in entry["input_paths_by_table"].items():
            ts = [pq.read_table(p.removeprefix("file://")) for p in paths]
            tables[table] = ts[0] if len(ts) == 1 else pa.concat_tables(ts)
        out = execute_sql(tables, join_sql)
        rows.extend(
            zip(
                out.column("customer_id").to_pylist(),
                out.column("amount").to_pylist(),
                out.column("name").to_pylist(),
            )
        )
    return rows


_JOIN_SQL = (
    "SELECT orders.customer_id, orders.amount, customers.name "
    "FROM orders JOIN customers ON orders.customer_id = customers.id"
)
_ORDERS = [
    {"customer_id": ["a", "b", "a"], "amount": [10, 20, 30]},
    {"customer_id": ["c", "b", "a"], "amount": [40, 50, 60]},
]
_CUSTOMERS = [
    {"id": ["a", "b"], "name": ["Alice", "Bob"]},
    {"id": ["c", "d"], "name": ["Carol", "Dave"]},
]


def _materialize(tmp_path: Path, name: str, partitions: list[dict]) -> list[str]:
    d = tmp_path / name
    d.mkdir()
    uris = []
    for i, data in enumerate(partitions):
        p = d / f"{name}_{i}.parquet"
        pq.write_table(pa.table(data), p)
        uris.append(p.resolve().as_uri())
    return uris


def _sources(tmp_path: Path) -> list[Source]:
    return [
        Source(
            source_id="orders_src",
            table_name="orders",
            options={"uris": _materialize(tmp_path, "orders", _ORDERS)},
        ),
        Source(
            source_id="customers_src",
            table_name="customers",
            options={"uris": _materialize(tmp_path, "customers", _CUSTOMERS)},
        ),
    ]


def test_join_compiles_to_scan_scan_join(tmp_path: Path) -> None:
    graph = compile_sql(_JOIN_SQL, _sources(tmp_path), num_buckets=3, stage_id_prefix="oc")
    assert [s.stage_id for s in graph.stages] == ["oc_scan_orders", "oc_scan_customers", "oc_join"]
    scan_o, scan_c, join = graph.stages
    assert scan_o.output_exchange.kind == "hash_shuffle"
    assert scan_o.output_exchange.keys == ["customer_id"]
    assert scan_c.output_exchange.kind == "hash_shuffle"
    assert scan_c.output_exchange.keys == ["id"]
    assert join.output_exchange.kind == "pipeline"
    assert {i.table_name for i in join.inputs} == {"orders", "customers"}
    assert graph.sink_stage_id == "oc_join"


def test_rejects_non_inner_join(tmp_path: Path) -> None:
    sql = _JOIN_SQL.replace("JOIN", "LEFT JOIN")
    with pytest.raises(UnsupportedSQLError, match="INNER"):
        compile_sql(sql, _sources(tmp_path))


def test_rejects_cross_type_join_keys(tmp_path: Path) -> None:
    # orders.customer_id is a string; customers.id is an int -> different repr -> would silently
    # drop matches under independent hashing, so the compiler must reject it.
    orders = Source(
        source_id="orders_src",
        table_name="orders",
        options={"uris": _materialize(tmp_path, "orders", [{"customer_id": ["1", "2"], "amount": [10, 20]}])},
    )
    customers = Source(
        source_id="customers_src",
        table_name="customers",
        options={"uris": _materialize(tmp_path, "customers", [{"id": [1, 2], "name": ["A", "B"]}])},
    )
    with pytest.raises(UnsupportedSQLError, match="co-partition compatible"):
        compile_sql(_JOIN_SQL, [orders, customers])


def test_matching_type_join_keys_compile(tmp_path: Path) -> None:
    # Both keys int64 (exact same type) -> no CAST -> compiles.
    op = tmp_path / "o.parquet"
    cp = tmp_path / "c.parquet"
    pq.write_table(pa.table({"customer_id": pa.array([1, 2], pa.int64()), "amount": [10, 20]}), op)
    pq.write_table(pa.table({"id": pa.array([1, 2], pa.int64()), "name": ["A", "B"]}), cp)
    orders = Source(source_id="orders_src", table_name="orders", options={"uris": [op.resolve().as_uri()]})
    customers = Source(
        source_id="customers_src", table_name="customers", options={"uris": [cp.resolve().as_uri()]}
    )
    graph = compile_sql(_JOIN_SQL, [orders, customers], stage_id_prefix="oc")
    assert [s.stage_id for s in graph.stages] == ["oc_scan_orders", "oc_scan_customers", "oc_join"]


def test_builds_join_dag_structure(tmp_path: Path) -> None:
    graph = compile_sql(_JOIN_SQL, _sources(tmp_path), num_buckets=3, stage_id_prefix="oc")
    dag = dag_from_stages(
        graph,
        dag_id="join_struct",
        sink=Sink("parquet", {"base_uri": (tmp_path / "out").resolve().as_uri()}),
        schedule=None,
        start_date=datetime(2026, 1, 1),
    )
    assert set(dag.task_ids) >= {
        "make_work_dir",
        "plan_partitions_oc_scan_orders",
        "plan_partitions_oc_scan_customers",
        "stage_oc_scan_orders.write",
        "stage_oc_scan_customers.write",
        "gather_shuffle_oc_join",
        "stage_oc_join.read",
        "stage_oc_join.write",
        "finalize",
    }


def test_join_runtime_pipeline_matches_single_node(tmp_path: Path) -> None:
    """Run the compiled join end-to-end through the shuffle and assert the joined rows."""
    num_buckets = 3
    graph = compile_sql(_JOIN_SQL, _sources(tmp_path), num_buckets=num_buckets, stage_id_prefix="oc")
    orders_out = _run_scan(tmp_path, graph.get_stage("oc_scan_orders"), "orders", _ORDERS, num_buckets)
    customers_out = _run_scan(
        tmp_path, graph.get_stage("oc_scan_customers"), "customers", _CUSTOMERS, num_buckets
    )

    plan = gather_multi(
        {"orders": orders_out, "customers": customers_out},
        num_shuffle_buckets=num_buckets,
        output_base=tmp_path.as_uri(),
    )
    rows = _run_join_plan(plan, graph.get_stage("oc_join").sql)
    assert sorted(rows) == _EXPECTED_JOIN


def test_broadcast_join_runtime_matches_shuffle(tmp_path: Path) -> None:
    """With a low threshold the small side broadcasts; the joined rows must still be correct."""
    num_buckets = 3
    graph = compile_sql(_JOIN_SQL, _sources(tmp_path), num_buckets=num_buckets, stage_id_prefix="oc")
    orders_out = _run_scan(tmp_path, graph.get_stage("oc_scan_orders"), "orders", _ORDERS, num_buckets)
    customers_out = _run_scan(
        tmp_path, graph.get_stage("oc_scan_customers"), "customers", _CUSTOMERS, num_buckets
    )

    plan = choose_join_strategy(
        orders_out,
        customers_out,
        left_table="orders",
        right_table="customers",
        num_buckets=num_buckets,
        output_base=tmp_path.as_uri(),
        broadcast_threshold=100,
    )
    assert {entry["strategy"] for entry in plan} == {"broadcast"}
    rows = _run_join_plan(plan, graph.get_stage("oc_join").sql)
    assert sorted(rows) == _EXPECTED_JOIN


def test_choose_join_strategy_shuffle_above_threshold() -> None:
    left = [
        {"partition_id": 0, "files": [{"path": "l0", "bucket": 0, "rows": 100}], "rows_in": 0, "rows_out": 0}
    ]
    right = [
        {"partition_id": 0, "files": [{"path": "r0", "bucket": 0, "rows": 50}], "rows_in": 0, "rows_out": 0}
    ]
    plan = choose_join_strategy(
        left,
        right,
        left_table="a",
        right_table="b",
        num_buckets=2,
        output_base="file:///wd",
        broadcast_threshold=10,
    )
    assert {e["strategy"] for e in plan} == {"shuffle"}


def test_choose_join_strategy_broadcasts_small_side() -> None:
    left = [
        {
            "partition_id": 0,
            "files": [{"path": "l0", "bucket": 0, "rows": 100}, {"path": "l1", "bucket": 1, "rows": 100}],
            "rows_in": 0,
            "rows_out": 0,
        }
    ]
    right = [
        {
            "partition_id": 0,
            "files": [{"path": "r0", "bucket": 0, "rows": 3}, {"path": "r1", "bucket": 1, "rows": 2}],
            "rows_in": 0,
            "rows_out": 0,
        }
    ]
    plan = choose_join_strategy(
        left,
        right,
        left_table="a",
        right_table="b",
        num_buckets=2,
        output_base="file:///wd",
        broadcast_threshold=10,
    )
    assert {e["strategy"] for e in plan} == {"broadcast"}
    # every large-side bucket sees the full small side (both small files).
    for entry in plan:
        assert sorted(entry["input_paths_by_table"]["b"]) == ["r0", "r1"]
