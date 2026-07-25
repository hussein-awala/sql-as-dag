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
"""Tests for the internal Parquet IO helpers."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from sql_as_dag.runtime.io import read_concat, read_table, write_table


def test_write_then_read_roundtrip(tmp_path: Path) -> None:
    table = pa.table({"customer_id": ["a", "b"], "amount": [10, 20]})
    uri = write_table(table, f"{tmp_path.resolve().as_uri()}/nested/dir/data.parquet")
    assert read_table(uri).equals(table)


def test_write_then_read_empty_table(tmp_path: Path) -> None:
    """A zero-row table must survive the round-trip with its schema intact."""
    schema = pa.schema([("customer_id", pa.string()), ("amount", pa.int64())])
    table = pa.table({"customer_id": [], "amount": []}, schema=schema)
    uri = write_table(table, f"{tmp_path.resolve().as_uri()}/empty.parquet")
    back = read_table(uri)
    assert back.num_rows == 0
    assert back.schema.equals(schema)


def test_read_concat_joins_multiple_files(tmp_path: Path) -> None:
    base = tmp_path.resolve().as_uri()
    a = write_table(pa.table({"id": [1]}), f"{base}/a.parquet")
    b = write_table(pa.table({"id": [2]}), f"{base}/b.parquet")
    assert read_concat([a, b]).column("id").to_pylist() == [1, 2]


def test_read_concat_requires_at_least_one_uri() -> None:
    with pytest.raises(ValueError, match="at least one uri"):
        read_concat([])


def test_read_concat_reports_schema_mismatch_clearly(tmp_path: Path) -> None:
    """
    Mismatched schemas must name the offending file.

    A source connector reports only its first file's schema at DAG-parse time, so a divergent
    file is first noticed here — where the raw pyarrow error would not say which file is at
    fault.
    """
    base = tmp_path.resolve().as_uri()
    a = write_table(pa.table({"id": [1]}), f"{base}/a.parquet")
    b = write_table(pa.table({"other": ["x"]}), f"{base}/b.parquet")
    with pytest.raises(ValueError, match="different schemas"):
        read_concat([a, b])
