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
Demo: the same wide GROUP BY, with the shuffle width decided at run time.

Identical data and SQL to ``example_groupby_demo``, but ``num_buckets`` is no longer the
compiler's fixed choice. A ``plan_shuffle_width_orders_final`` task runs first, reads the
source's row count from Parquet metadata, and resolves the width before the map side starts.

Point at the extra task in the grid: it is the one node here that has no equivalent in a
static plan, and it is what lets the same query fan out wider on a bigger input.

``BucketingPolicy(strategy="rows", num_buckets=8, target_rows_per_bucket=9)`` over 36 demo rows
resolves to ``ceil(36 / 9) = 4`` buckets, capped at 8. Four is deliberate: it matches
``example_groupby_demo``, so running both back to back shows the same final fan-out reached two
different ways, and all four buckets are non-empty (see that module's docstring for the
customer-to-bucket map). Targets that resolve to 5 or more leave some buckets empty on data
this small, and ``hash_partition`` skips empty buckets — the grid would then show fewer mapped
instances than the width the planner just announced, which is a distracting thing to explain.

Run it locally with::

    SQL_AS_DAG_ENABLE_DEMOS=true airflow dags test sql_as_dag_adaptive_groupby_demo
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sql_as_dag.compiler import compile_sql
from sql_as_dag.dag import dag_from_stages
from sql_as_dag.ir import BucketingPolicy, Sink, Source

DAG_ID = "sql_as_dag_adaptive_groupby_demo"

_DEMO_DIR = Path(tempfile.gettempdir()) / "sql_as_dag_adaptive_groupby_demo"
_INPUT_PARTITIONS: list[dict[str, list]] = [
    {
        "customer_id": ["alice", "bob", "carol", "erin", "frank", "grace"],
        "amount": [10, 20, 30, 40, 50, 60],
    },
    {
        "customer_id": ["heidi", "ivan", "judy", "ken", "alice", "bob"],
        "amount": [15, 25, 35, 45, 55, 65],
    },
    {
        "customer_id": ["carol", "erin", "frank", "grace", "heidi", "ivan"],
        "amount": [12, 22, 32, 42, 52, 62],
    },
    {
        "customer_id": ["judy", "ken", "alice", "bob", "carol", "erin"],
        "amount": [18, 28, 38, 48, 58, 68],
    },
    {
        "customer_id": ["frank", "grace", "heidi", "ivan", "judy", "ken"],
        "amount": [14, 24, 34, 44, 54, 64],
    },
    {
        "customer_id": ["alice", "carol", "frank", "heidi", "judy", "ken"],
        "amount": [16, 26, 36, 46, 56, 66],
    },
]
_SQL = "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"


def _materialize_inputs() -> list[str]:
    inputs_dir = _DEMO_DIR / "orders"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    uris: list[str] = []
    for i, data in enumerate(_INPUT_PARTITIONS):
        p = inputs_dir / f"orders_{i}.parquet"
        pq.write_table(pa.table(data), p)
        uris.append(p.resolve().as_uri())
    return uris


if os.getenv("SQL_AS_DAG_ENABLE_DEMOS", "").lower() in {"1", "true", "yes", "on"}:
    _SOURCE = Source(
        source_id="orders_src",
        table_name="orders",
        connector="parquet",
        options={"uris": _materialize_inputs()},
    )
    _GRAPH = compile_sql(_SQL, [_SOURCE], stage_id_prefix="orders")

    dag = dag_from_stages(
        _GRAPH,
        dag_id=DAG_ID,
        sink=Sink("parquet", {"base_uri": (_DEMO_DIR / "out").resolve().as_uri()}),
        bucketing=BucketingPolicy(strategy="rows", num_buckets=8, target_rows_per_bucket=9),
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["sql_as_dag", "demo"],
    )
