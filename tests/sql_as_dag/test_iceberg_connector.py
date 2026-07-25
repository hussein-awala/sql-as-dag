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

from pathlib import Path

import pyarrow as pa
import pytest

from sql_as_dag.connectors.iceberg import IcebergSink, IcebergSourceConnector
from sql_as_dag.connectors.registry import get_sink, get_source

pytest.importorskip("pyiceberg")


def _cfg(tmp_path: Path) -> dict:
    return {
        "catalog_name": "demo",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": f"file://{tmp_path}/warehouse",
        "identifier": "db.orders",
    }


def test_registry_has_iceberg() -> None:
    assert get_source("iceberg") is IcebergSourceConnector
    assert get_sink("iceberg") is IcebergSink


def test_iceberg_sink_then_source_roundtrip(tmp_path: Path) -> None:
    (tmp_path / "warehouse").mkdir()
    cfg = _cfg(tmp_path)

    sink = IcebergSink(**cfg)
    m0 = sink.write(pa.table({"customer_id": ["a", "b"], "total": [10, 20]}), partition_id=0)
    m1 = sink.write(pa.table({"customer_id": ["c"], "total": [30]}), partition_id=1)
    summary = sink.finalize([m0, m1])
    assert summary["rows"] == 3

    source = IcebergSourceConnector(**cfg)
    assert set(source.schema().names) == {"customer_id", "total"}
    refs = source.list_partitions()
    assert len(refs) >= 1

    totals: dict[str, int] = {}
    for ref in refs:
        table = source.read_partition(ref)
        for cid, total in zip(table.column("customer_id").to_pylist(), table.column("total").to_pylist()):
            totals[cid] = total
    assert totals == {"a": 10, "b": 20, "c": 30}
