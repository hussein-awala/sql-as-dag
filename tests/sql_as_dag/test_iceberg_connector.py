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


def test_finalize_is_idempotent(tmp_path: Path) -> None:
    """
    Re-running finalize must converge, not fail or duplicate rows.

    If the commit succeeds but Airflow does not record the task as done, the retry calls
    finalize again. pyiceberg's add_files rejects already-referenced files, so the retry would
    fail permanently unless the sink filters out what is already committed.
    """
    (tmp_path / "warehouse").mkdir()
    cfg = _cfg(tmp_path)
    sink = IcebergSink(**cfg)
    metas = [
        sink.write(pa.table({"customer_id": ["a", "b"], "total": [10, 20]}), partition_id=0),
        sink.write(pa.table({"customer_id": ["c"], "total": [30]}), partition_id=1),
    ]

    first = sink.finalize(metas)
    assert first["files_added"] == 2
    assert first["files_already_committed"] == 0

    second = sink.finalize(metas)
    assert second["files_added"] == 0
    assert second["files_already_committed"] == 2

    # The data must not have been double-counted.
    source = IcebergSourceConnector(**cfg)
    rows: list[tuple] = []
    for ref in source.list_partitions():
        table = source.read_partition(ref)
        rows += list(zip(table.column("customer_id").to_pylist(), table.column("total").to_pylist()))
    assert sorted(rows) == [("a", 10), ("b", 20), ("c", 30)]


def test_finalize_with_no_rows_and_missing_table_explains_itself(tmp_path: Path) -> None:
    """
    An empty result cannot create a new Iceberg table, and must say why.

    There is no data file to infer a schema from, so this is a genuine dead end — but it should
    surface as an actionable message rather than a bare catalog lookup error.
    """
    (tmp_path / "warehouse").mkdir()
    sink = IcebergSink(**_cfg(tmp_path))
    with pytest.raises(ValueError, match="no schema to create it from"):
        sink.finalize([])


def test_finalize_with_no_rows_on_existing_table_is_a_noop(tmp_path: Path) -> None:
    """Once the table exists, an empty result commits nothing and reports zero rows."""
    (tmp_path / "warehouse").mkdir()
    cfg = _cfg(tmp_path)
    sink = IcebergSink(**cfg)
    meta = sink.write(pa.table({"customer_id": ["a"], "total": [10]}), partition_id=0)
    sink.finalize([meta])

    summary = IcebergSink(**cfg).finalize([])
    assert summary["rows"] == 0
    assert summary["files_added"] == 0
