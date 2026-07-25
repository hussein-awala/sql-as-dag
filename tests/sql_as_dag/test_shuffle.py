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

import pyarrow as pa
import pytest

from sql_as_dag.runtime.shuffle import _stable_row_hash, hash_partition


def test_all_rows_preserved_and_bucketed() -> None:
    table = pa.table({"k": ["a", "b", "a", "c", "b", "d"], "v": [1, 2, 3, 4, 5, 6]})
    parts = dict(hash_partition(table, ["k"], 4))

    # No rows lost or duplicated across buckets.
    assert sum(t.num_rows for t in parts.values()) == table.num_rows
    # Every bucket id is within range and non-empty (empty buckets are skipped).
    assert all(0 <= b < 4 for b in parts)
    assert all(t.num_rows > 0 for t in parts.values())


def test_same_key_lands_in_same_bucket() -> None:
    table = pa.table({"k": ["a", "b", "a", "a", "b"], "v": [1, 2, 3, 4, 5]})
    bucket_of: dict[str, int] = {}
    for bucket, sub in hash_partition(table, ["k"], 8):
        for key in sub.column("k").to_pylist():
            assert bucket_of.setdefault(key, bucket) == bucket


def test_single_bucket_yields_whole_table() -> None:
    table = pa.table({"k": ["a", "b"], "v": [1, 2]})
    parts = list(hash_partition(table, ["k"], 1))
    assert len(parts) == 1
    assert parts[0][0] == 0
    assert parts[0][1].num_rows == 2


def test_empty_table_yields_no_buckets() -> None:
    table = pa.table({"k": pa.array([], type=pa.string()), "v": pa.array([], type=pa.int64())})
    assert list(hash_partition(table, ["k"], 4)) == []  # no spurious bucket file
    assert list(hash_partition(table, ["k"], 1)) == []


def test_stable_hash_is_deterministic_and_separates_fields() -> None:
    assert _stable_row_hash(("a", "b")) == _stable_row_hash(("a", "b"))
    # field separator keeps ("a", "b") distinct from ("ab",)
    assert _stable_row_hash(("a", "b")) != _stable_row_hash(("ab",))
    # None is encoded distinctly from the string "None"
    assert _stable_row_hash((None,)) != _stable_row_hash(("None",))


def test_requires_keys_when_multiple_buckets() -> None:
    table = pa.table({"k": ["a"], "v": [1]})
    with pytest.raises(ValueError, match="at least one key"):
        list(hash_partition(table, [], 4))
