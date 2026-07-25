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
import pyarrow.parquet as pq
import pytest

from sql_as_dag.connectors.parquet import ParquetSink, ParquetSourceConnector


def _write_parquet(path: Path, data: dict[str, list]) -> str:
    pq.write_table(pa.table(data), path)
    return path.resolve().as_uri()


def test_source_roundtrip(tmp_path: Path) -> None:
    uri0 = _write_parquet(tmp_path / "p0.parquet", {"customer_id": ["a", "b"], "amount": [10, 20]})
    uri1 = _write_parquet(tmp_path / "p1.parquet", {"customer_id": ["c"], "amount": [30]})

    conn = ParquetSourceConnector(uris=[uri0, uri1])

    schema = conn.schema()
    assert schema.names == ["customer_id", "amount"]

    refs = conn.list_partitions()
    assert refs == [{"uri": uri0}, {"uri": uri1}]

    t0 = conn.read_partition(refs[0])
    assert t0.num_rows == 2
    assert t0.column("customer_id").to_pylist() == ["a", "b"]


def test_source_requires_uris() -> None:
    with pytest.raises(ValueError, match="at least one uri"):
        ParquetSourceConnector(uris=[])


def test_sink_write_and_finalize(tmp_path: Path) -> None:
    base_uri = (tmp_path / "out").resolve().as_uri()
    sink = ParquetSink(base_uri=base_uri)

    meta0 = sink.write(pa.table({"customer_id": ["a"], "total": [40]}), partition_id=0)
    meta1 = sink.write(pa.table({"customer_id": ["b", "c"], "total": [20, 30]}), partition_id=1)

    assert meta0["rows"] == 1
    assert meta1["rows"] == 2

    # Written files are readable and correct.
    written = pq.read_table(meta1["path"].removeprefix("file://"))
    assert written.column("total").to_pylist() == [20, 30]

    summary = sink.finalize([meta0, meta1])
    assert summary["partitions"] == 2
    assert summary["rows"] == 3
    assert (tmp_path / "out" / "_SUCCESS").exists()


def _run_once(sink: ParquetSink, run_key: str, tables: list[pa.Table]) -> dict:
    metas = [sink.write(t, partition_id=i, run_key=run_key) for i, t in enumerate(tables)]
    return sink.finalize(metas, run_key=run_key)


def test_sink_output_is_scoped_to_the_run(tmp_path: Path) -> None:
    """
    Two runs must not share an output directory.

    Partition ids restart at 0 every run and the bucket count can shrink, so a shared directory
    would let the second run overwrite p0 while p1 survived from the first — one directory holding
    rows from two runs, under a freshly written success marker.
    """
    base = tmp_path / "out"
    sink = ParquetSink(base_uri=base.resolve().as_uri())

    first = _run_once(sink, "run_a", [pa.table({"v": [1]}), pa.table({"v": [2]})])
    second = _run_once(sink, "run_b", [pa.table({"v": [3]})])

    assert first["output_uri"] != second["output_uri"]
    assert sorted(p.name for p in base.iterdir() if p.is_dir()) == ["run_a", "run_b"]
    assert [pq.read_table(f)["v"][0].as_py() for f in sorted((base / "run_a").glob("p*/data.parquet"))] == [1, 2]
    assert [pq.read_table(f)["v"][0].as_py() for f in sorted((base / "run_b").glob("p*/data.parquet"))] == [3]
    # Each run's result is self-contained and separately marked complete.
    assert (base / "run_a" / "_SUCCESS").exists()
    assert (base / "run_b" / "_SUCCESS").exists()


def test_latest_pointer_names_the_newest_finalized_run(tmp_path: Path) -> None:
    """Consumers need a way to find the newest complete result without guessing."""
    base = tmp_path / "out"
    sink = ParquetSink(base_uri=base.resolve().as_uri())

    _run_once(sink, "run_a", [pa.table({"v": [1]})])
    assert (base / "_LATEST").read_text() == "run_a"

    _run_once(sink, "run_b", [pa.table({"v": [2]})])
    assert (base / "_LATEST").read_text() == "run_b"


def test_retry_within_a_run_overwrites_in_place(tmp_path: Path) -> None:
    """Re-running the same partition of the same run must not duplicate its output."""
    base = tmp_path / "out"
    sink = ParquetSink(base_uri=base.resolve().as_uri())

    sink.write(pa.table({"v": [1]}), partition_id=0, run_key="run_a")
    meta = sink.write(pa.table({"v": [1]}), partition_id=0, run_key="run_a")
    sink.finalize([meta], run_key="run_a")

    files = sorted((base / "run_a").glob("p*/data.parquet"))
    assert len(files) == 1
    assert pq.read_table(files[0])["v"].to_pylist() == [1]
