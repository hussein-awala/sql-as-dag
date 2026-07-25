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
