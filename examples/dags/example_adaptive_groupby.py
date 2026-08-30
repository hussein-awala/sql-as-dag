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
Demo: GROUP BY with a runtime-decided shuffle width.

Same compiled GROUP BY as ``example_compiled_groupby``, but the number of shuffle buckets is
chosen at run time by a ``plan_shuffle_width_*`` task from the source's row count (the ``rows``
bucketing strategy with a small target, so the fan-out is visible on tiny demo data). Swap the
``BucketingPolicy`` strategy for ``bytes``, ``partitions``, or ``static`` to compare.

Run it locally with::

    airflow dags test sql_as_dag_adaptive_groupby
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sql_as_dag.compiler import compile_sql
from sql_as_dag.dag import dag_from_stages
from sql_as_dag.ir import BucketingPolicy, Sink, Source

DAG_ID = "sql_as_dag_adaptive_groupby"

_DEMO_DIR = Path(tempfile.gettempdir()) / "sql_as_dag_adaptive_groupby"
_INPUT_PARTITIONS: list[dict[str, list]] = [
    {"customer_id": ["a", "b", "a", "c", "b"], "amount": [10, 20, 30, 40, 50]},
    {"customer_id": ["a", "d", "c", "b", "a"], "amount": [60, 70, 80, 90, 100]},
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


_SOURCE = Source(source_id="orders_src", table_name="orders", options={"uris": _materialize_inputs()})
_GRAPH = compile_sql(_SQL, [_SOURCE], stage_id_prefix="orders_by_customer")

dag = dag_from_stages(
    _GRAPH,
    dag_id=DAG_ID,
    sink=Sink("parquet", {"base_uri": (_DEMO_DIR / "out").resolve().as_uri()}),
    bucketing=BucketingPolicy(strategy="rows", num_buckets=8, target_rows_per_bucket=4),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sql_as_dag", "example", "adaptive"],
)
