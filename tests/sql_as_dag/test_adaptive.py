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
"""Tests for runtime-adaptive shuffle width (the plan_shuffle_width planner + policy)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sql_as_dag.compiler import compile_sql
from sql_as_dag.connectors.registry import get_source
from sql_as_dag.dag import dag_from_stages
from sql_as_dag.ir import BucketingPolicy, Sink, Source
from sql_as_dag.runtime.coordinator import gather_shuffle
from sql_as_dag.runtime.executor import execute_sql
from sql_as_dag.runtime.shuffle import hash_partition

_GROUPBY_SQL = "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
_JOIN_SQL = (
    "SELECT orders.customer_id, orders.amount, customers.name "
    "FROM orders JOIN customers ON orders.customer_id = customers.id"
)
_ORDERS = [
    {"customer_id": ["a", "b", "a", "c", "b"], "amount": [10, 20, 30, 40, 50]},
    {"customer_id": ["a", "d", "c", "b", "a"], "amount": [60, 70, 80, 90, 100]},
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


def _orders(tmp_path: Path) -> Source:
    return Source(
        source_id="orders_src",
        table_name="orders",
        options={"uris": _materialize(tmp_path, "orders", _ORDERS)},
    )


def _sink(tmp_path: Path) -> Sink:
    return Sink("parquet", {"base_uri": (tmp_path / "out").resolve().as_uri()})


def test_groupby_adaptive_adds_width_planner(tmp_path: Path) -> None:
    graph = compile_sql(_GROUPBY_SQL, [_orders(tmp_path)], num_buckets=8, stage_id_prefix="g")
    dag = dag_from_stages(
        graph,
        dag_id="adaptive_gb",
        sink=_sink(tmp_path),
        bucketing=BucketingPolicy(strategy="rows", num_buckets=8, target_rows_per_bucket=3),
        schedule=None,
        start_date=datetime(2026, 1, 1),
    )
    assert "plan_shuffle_width_g_final" in dag.task_ids


def test_join_adaptive_uses_one_shared_width_planner(tmp_path: Path) -> None:
    customers = Source(
        source_id="customers_src",
        table_name="customers",
        options={"uris": _materialize(tmp_path, "customers", [{"id": ["a", "b"], "name": ["A", "B"]}])},
    )
    graph = compile_sql(_JOIN_SQL, [_orders(tmp_path), customers], num_buckets=8, stage_id_prefix="oc")
    dag = dag_from_stages(
        graph,
        dag_id="adaptive_join",
        sink=_sink(tmp_path),
        bucketing=BucketingPolicy(strategy="partitions", num_buckets=8),
        schedule=None,
        start_date=datetime(2026, 1, 1),
    )
    width_tasks = sorted(t for t in dag.task_ids if t.startswith("plan_shuffle_width_"))
    # both scans share a single planner keyed by their common consumer (the join stage).
    assert width_tasks == ["plan_shuffle_width_oc_join"]


def test_static_policy_has_no_width_planner(tmp_path: Path) -> None:
    graph = compile_sql(_GROUPBY_SQL, [_orders(tmp_path)], stage_id_prefix="g")
    dag = dag_from_stages(
        graph,
        dag_id="static_gb",
        sink=_sink(tmp_path),
        schedule=None,
        start_date=datetime(2026, 1, 1),
    )
    assert not any(t.startswith("plan_shuffle_width_") for t in dag.task_ids)


def test_adaptive_width_drives_groupby_runtime(tmp_path: Path) -> None:
    """The planner's width (from metadata) flows into the map side; results stay correct."""
    source = _orders(tmp_path)
    graph = compile_sql(_GROUPBY_SQL, [source], num_buckets=8, stage_id_prefix="g")
    partial, final = graph.stages

    # Reproduce what plan_shuffle_width computes for the 'rows' policy.
    policy = BucketingPolicy(strategy="rows", num_buckets=8, target_rows_per_bucket=3)
    connector = get_source("parquet")(**source.options)
    width = policy.compute(
        total_rows=connector.estimate_total_rows(),
        num_partitions=len(connector.list_partitions()),
    )
    assert width == 4  # ceil(10 rows / 3), capped at 8

    # Map side at the chosen width, then reduce.
    stage_outputs = []
    for pid, data in enumerate(_ORDERS):
        result = execute_sql({"orders": pa.table(data)}, partial.sql)
        files = []
        for bucket, sub in hash_partition(result, partial.output_exchange.keys, width):
            path = tmp_path / f"p{pid}_b{bucket}.parquet"
            pq.write_table(sub, path)
            files.append({"path": path.resolve().as_uri(), "bucket": bucket, "rows": sub.num_rows})
        stage_outputs.append({"partition_id": pid, "files": files, "rows_in": 0, "rows_out": 0})

    plan = gather_shuffle(stage_outputs, output_base=tmp_path.as_uri())
    totals: dict[str, int] = {}
    for entry in plan:
        tables = [pq.read_table(p.removeprefix("file://")) for p in entry["input_paths"]]
        combined = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
        out = execute_sql({final.inputs[0].table_name: combined}, final.sql)
        for cid, total in zip(out.column("customer_id").to_pylist(), out.column("total").to_pylist()):
            totals[cid] = total
    assert totals == {"a": 200, "b": 160, "c": 120, "d": 70}
