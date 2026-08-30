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
Demo: a single-stage scan / filter DAG.

Two Parquet partitions are filtered and projected in parallel (one mapped
``read -> compute -> write`` task-group instance per partition) and written back as
Parquet. No shuffle — the whole query is one stage with a ``pipeline`` output exchange.

Run it locally with::

    airflow dags test sql_as_dag_passthrough
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sql_as_dag.dag import dag_from_stages
from sql_as_dag.ir import Sink, Source, Stage, StageGraph, StageInput

DAG_ID = "sql_as_dag_passthrough"

_DEMO_DIR = Path(tempfile.gettempdir()) / "sql_as_dag_passthrough"
_INPUT_PARTITIONS: list[dict[str, list]] = [
    {"customer_id": ["a", "b", "a", "c"], "amount": [10, 30, 50, 20]},
    {"customer_id": ["b", "d", "a"], "amount": [40, 5, 90]},
]


def _materialize_inputs() -> list[str]:
    inputs_dir = _DEMO_DIR / "orders"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    uris: list[str] = []
    for i, data in enumerate(_INPUT_PARTITIONS):
        p = inputs_dir / f"orders_{i}.parquet"
        pq.write_table(pa.table(data), p)
        uris.append(p.resolve().as_uri())
    return uris


_SOURCE = Source(
    source_id="orders_src",
    table_name="orders",
    connector="parquet",
    options={"uris": _materialize_inputs()},
)
_STAGE = Stage(
    stage_id="main",
    sql="SELECT customer_id, amount FROM orders WHERE amount > 25",
    inputs=[StageInput(table_name="orders", source_id="orders_src")],
)
_GRAPH = StageGraph(sources=[_SOURCE], stages=[_STAGE], sink_stage_id="main")

dag = dag_from_stages(
    _GRAPH,
    dag_id=DAG_ID,
    sink=Sink("parquet", {"base_uri": (_DEMO_DIR / "out").resolve().as_uri()}),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sql_as_dag", "example"],
)
