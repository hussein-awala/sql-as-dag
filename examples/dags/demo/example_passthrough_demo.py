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
Demo: a wide single-stage scan / filter, sized for a live audience.

Same shape as ``example_passthrough`` — one stage, a ``pipeline`` exchange, no shuffle — but
fed six source partitions instead of two, so the mapped ``read -> compute -> write`` group
fans out six ways and the map side is legible from the back of the room.

One source file becomes one mapped instance, so the fan-out here is decided entirely by the
input: no shuffle is involved. ``example_groupby_demo`` adds the shuffle.

The filter keeps ``amount > 25``. Every partition holds a mix of kept and dropped rows, and
one row sits exactly on the boundary (25, dropped), so the row counts in the task logs differ
from the input counts.

Run it locally with::

    SQL_AS_DAG_ENABLE_DEMOS=true airflow dags test sql_as_dag_passthrough_demo
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

DAG_ID = "sql_as_dag_passthrough_demo"

_DEMO_DIR = Path(tempfile.gettempdir()) / "sql_as_dag_passthrough_demo"

# Six partitions, one mapped task-group instance each. Amounts straddle the filter threshold in
# every file so no instance trivially keeps or drops everything.
_INPUT_PARTITIONS: list[dict[str, list]] = [
    {
        "customer_id": ["alice", "bob", "carol", "erin", "frank", "grace"],
        "amount": [10, 30, 5, 45, 22, 60],
    },
    {
        "customer_id": ["heidi", "ivan", "judy", "ken", "alice", "bob"],
        "amount": [18, 26, 12, 55, 40, 8],
    },
    {
        "customer_id": ["carol", "erin", "frank", "grace", "heidi", "ivan"],
        "amount": [33, 15, 70, 24, 29, 11],
    },
    {
        "customer_id": ["judy", "ken", "alice", "bob", "carol", "erin"],
        "amount": [48, 19, 27, 6, 52, 35],
    },
    # 25 is not > 25: the boundary row is dropped.
    {
        "customer_id": ["frank", "grace", "heidi", "ivan", "judy", "ken"],
        "amount": [21, 38, 9, 65, 25, 43],
    },
    {
        "customer_id": ["alice", "carol", "frank", "heidi", "judy", "ken"],
        "amount": [31, 14, 58, 23, 47, 16],
    },
]
_SQL = "SELECT customer_id, amount FROM orders WHERE amount > 25"


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
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["sql_as_dag", "demo"],
    )
