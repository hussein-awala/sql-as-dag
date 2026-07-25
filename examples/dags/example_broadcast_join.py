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
Demo: the same join, but with a broadcast strategy chosen at runtime.

Identical query and data to ``example_join``, but ``broadcast_threshold`` is set so that the
small ``customers`` side is replicated to every populated ``orders`` bucket instead of being
co-partitioned. The ``gather_shuffle_*`` planner task logs which strategy it picked.

Run it locally with::

    airflow dags test sql_as_dag_broadcast_join
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sql_as_dag.compiler import compile_sql
from sql_as_dag.dag import dag_from_stages
from sql_as_dag.ir import Sink, Source

DAG_ID = "sql_as_dag_broadcast_join"

_DEMO_DIR = Path(tempfile.gettempdir()) / "sql_as_dag_broadcast_join"
_ORDERS = [
    {"customer_id": ["a", "b", "a"], "amount": [10, 20, 30]},
    {"customer_id": ["c", "b", "a"], "amount": [40, 50, 60]},
]
_CUSTOMERS = [{"id": ["a", "b", "c", "d"], "name": ["Alice", "Bob", "Carol", "Dave"]}]
_SQL = (
    "SELECT orders.customer_id, orders.amount, customers.name "
    "FROM orders JOIN customers ON orders.customer_id = customers.id"
)


def _materialize(name: str, partitions: list[dict]) -> list[str]:
    d = _DEMO_DIR / name
    d.mkdir(parents=True, exist_ok=True)
    uris: list[str] = []
    for i, data in enumerate(partitions):
        p = d / f"{name}_{i}.parquet"
        pq.write_table(pa.table(data), p)
        uris.append(p.resolve().as_uri())
    return uris


_ORDERS_SRC = Source(
    source_id="orders_src", table_name="orders", options={"uris": _materialize("orders", _ORDERS)}
)
_CUSTOMERS_SRC = Source(
    source_id="customers_src", table_name="customers", options={"uris": _materialize("customers", _CUSTOMERS)}
)
_GRAPH = compile_sql(_SQL, [_ORDERS_SRC, _CUSTOMERS_SRC], num_buckets=3, stage_id_prefix="oc")

dag = dag_from_stages(
    _GRAPH,
    dag_id=DAG_ID,
    sink=Sink("parquet", {"base_uri": (_DEMO_DIR / "out").resolve().as_uri()}),
    broadcast_threshold=100,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sql_as_dag", "demo", "join", "broadcast"],
)
