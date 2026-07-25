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


def _rows(cfg: dict) -> list[tuple]:
    source = IcebergSourceConnector(**cfg)
    rows: list[tuple] = []
    for ref in source.list_partitions():
        table = source.read_partition(ref)
        rows += list(zip(table.column("customer_id").to_pylist(), table.column("total").to_pylist()))
    return sorted(rows)


def test_second_run_appends_instead_of_corrupting_the_first(tmp_path: Path) -> None:
    """
    A later run must not write over a data file an earlier run already committed.

    Staging paths are derived from the partition id, which restarts at 0 each run, so without a
    run scope the second run would rewrite ``p0.parquet`` in place. Iceberg's metadata still
    describes the file it committed, so the first snapshot would silently start reporting the
    second run's rows.
    """
    (tmp_path / "warehouse").mkdir()
    cfg = _cfg(tmp_path)
    sink = IcebergSink(**cfg)

    m0 = sink.write(pa.table({"customer_id": ["a"], "total": [10]}), partition_id=0, run_key="run_a")
    sink.finalize([m0], run_key="run_a")

    m1 = sink.write(pa.table({"customer_id": ["b"], "total": [20]}), partition_id=0, run_key="run_b")
    second = sink.finalize([m1], run_key="run_b")

    assert m0["path"] != m1["path"]
    assert second["files_added"] == 1
    assert second["files_already_committed"] == 0
    # Both runs' rows are present: the first was appended to, not overwritten.
    assert _rows(cfg) == [("a", 10), ("b", 20)]


def test_idempotency_check_does_not_look_outside_the_run(tmp_path: Path) -> None:
    """
    The already-committed filter must be scoped to the run being finalized.

    A broader check could match a file from an earlier run and treat this run's file as already
    committed, skipping the append and losing rows with no error.
    """
    (tmp_path / "warehouse").mkdir()
    cfg = _cfg(tmp_path)
    sink = IcebergSink(**cfg)

    metas_a = [sink.write(pa.table({"customer_id": ["a"], "total": [10]}), partition_id=0, run_key="run_a")]
    sink.finalize(metas_a, run_key="run_a")

    metas_b = [sink.write(pa.table({"customer_id": ["b"], "total": [20]}), partition_id=0, run_key="run_b")]
    sink.finalize(metas_b, run_key="run_b")
    # Retrying run_b still converges, and still sees only its own committed file.
    retry = sink.finalize(metas_b, run_key="run_b")
    assert retry["files_added"] == 0
    assert retry["files_already_committed"] == 1
    assert _rows(cfg) == [("a", 10), ("b", 20)]


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
