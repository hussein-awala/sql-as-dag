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
Demo: the two-stage GROUP BY, hand-built rather than compiled.

``SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id`` is hand-built
as two stages:

* ``partial`` (per source partition): ``SUM(amount) AS __p_0`` grouped by ``customer_id``,
  then hash-shuffled by ``customer_id`` into N buckets.
* ``final`` (per bucket): ``SUM(__p_0) AS total`` grouped by ``customer_id``.

In the UI you can watch the partial stage fan out across source partitions, the shuffle
write bucketed files, and the final stage fan out across buckets.

Run it locally with::

    airflow dags test sql_as_dag_groupby
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sql_as_dag.dag import dag_from_stages
from sql_as_dag.ir import Exchange, Sink, Source, Stage, StageGraph, StageInput

DAG_ID = "sql_as_dag_groupby"

_DEMO_DIR = Path(tempfile.gettempdir()) / "sql_as_dag_groupby"
_INPUT_PARTITIONS: list[dict[str, list]] = [
    {"customer_id": ["a", "b", "a", "c", "b"], "amount": [10, 20, 30, 40, 50]},
    {"customer_id": ["a", "d", "c", "b", "a"], "amount": [60, 70, 80, 90, 100]},
]
_NUM_BUCKETS = 3


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
_PARTIAL = Stage(
    stage_id="partial",
    sql="SELECT customer_id, SUM(amount) AS __p_0 FROM orders GROUP BY customer_id",
    inputs=[StageInput(table_name="orders", source_id="orders_src")],
    output_exchange=Exchange(kind="hash_shuffle", keys=["customer_id"], num_buckets=_NUM_BUCKETS),
)
_FINAL = Stage(
    stage_id="final",
    sql="SELECT customer_id, SUM(__p_0) AS total FROM partials GROUP BY customer_id",
    inputs=[StageInput(table_name="partials", upstream_stage_id="partial")],
)
_GRAPH = StageGraph(sources=[_SOURCE], stages=[_PARTIAL, _FINAL], sink_stage_id="final")

dag = dag_from_stages(
    _GRAPH,
    dag_id=DAG_ID,
    sink=Sink("parquet", {"base_uri": (_DEMO_DIR / "out").resolve().as_uri()}),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sql_as_dag", "demo", "groupby"],
)
