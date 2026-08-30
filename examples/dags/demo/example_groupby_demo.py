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
Demo: the two-phase GROUP BY, sized for a live audience.

Same compiled query as ``example_compiled_groupby``, widened so both halves of the shuffle are
visible at once: six source partitions on the map side, four buckets on the reduce side.

    make_work_dir
      -> plan_partitions_orders_partial
      -> stage_orders_partial x6      (one per source file)
      -> gather_shuffle_orders_final
      -> stage_orders_final x4        (one per bucket)
      -> finalize

The two fan-outs have different causes, which is the point worth narrating: the six comes from
the input (one file, one mapped instance), while the four comes from ``num_buckets`` — the
compiler's choice, not the data's.

The ten customer ids are chosen so that all four buckets are non-empty; ``hash_partition``
skips empty buckets, so a poorly chosen set would quietly expand the final stage to fewer than
four instances. Their blake2b hashes land 3/2/2/3 across buckets 0-3::

    bucket 0: frank, grace, ivan
    bucket 1: erin, judy
    bucket 2: alice, ken
    bucket 3: bob, carol, heidi

Every customer appears in three or four different partitions, so each one really is summed
from partials computed by different mapped tasks rather than falling out of a single file.

Run it locally with::

    SQL_AS_DAG_ENABLE_DEMOS=true airflow dags test sql_as_dag_groupby_demo
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
from sql_as_dag.ir import Sink, Source

DAG_ID = "sql_as_dag_groupby_demo"

_DEMO_DIR = Path(tempfile.gettempdir()) / "sql_as_dag_groupby_demo"
_NUM_BUCKETS = 4

# Six partitions of six rows. The customer ids rotate through the files so that every customer
# is split across several partitions and the partial sums have to be recombined.
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
    _GRAPH = compile_sql(_SQL, [_SOURCE], num_buckets=_NUM_BUCKETS, stage_id_prefix="orders")

    dag = dag_from_stages(
        _GRAPH,
        dag_id=DAG_ID,
        sink=Sink("parquet", {"base_uri": (_DEMO_DIR / "out").resolve().as_uri()}),
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["sql_as_dag", "demo"],
    )
