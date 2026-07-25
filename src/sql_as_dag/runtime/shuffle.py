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
Map-side shuffle: hash-partition an Arrow table into buckets by key.

This is the partitioning half of the shuffle (the "map" side). Each upstream partition
hashes every output row by the shuffle keys and routes it to ``hash(keys) % num_buckets``;
the reduce side (see ``coordinator.py``) then gathers all files for a bucket.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

_BUCKET_COL = "__sql_as_dag_bucket__"


def _stable_row_hash(values: tuple) -> int:
    """
    Deterministic, cross-process hash of a tuple of key values.

    Python's built-in ``hash()`` is randomized per process (``PYTHONHASHSEED``), and each
    Airflow task is a separate process — so using it would route the same key to different
    buckets in different tasks and split groups apart. blake2b over encoded bytes is stable
    everywhere; a unit separator between fields keeps ``("a", "b")`` distinct from ``("ab",)``.
    """
    h = hashlib.blake2b(digest_size=8)
    for v in values:
        h.update(b"\x00" if v is None else repr(v).encode("utf-8"))
        h.update(b"\x1f")
    return int.from_bytes(h.digest(), "big")


def hash_partition(
    table: pa.Table,
    keys: Sequence[str],
    num_buckets: int,
) -> Iterator[tuple[int, pa.Table]]:
    """
    Yield ``(bucket_id, sub_table)`` pairs covering all rows of ``table``.

    Empty buckets are skipped — callers receive only the buckets that actually contain rows.
    An empty table yields nothing (no spurious bucket file); ``num_buckets == 1`` yields the
    whole (non-empty) table as bucket 0.
    """
    if num_buckets < 1:
        raise ValueError(f"num_buckets must be >= 1, got {num_buckets}")
    if table.num_rows == 0:
        return
    if num_buckets == 1:
        yield 0, table
        return
    if not keys:
        raise ValueError("hash_partition requires at least one key column when num_buckets > 1")

    columns = [table.column(k).to_pylist() for k in keys]
    buckets = [_stable_row_hash(row) % num_buckets for row in zip(*columns)]
    annotated = table.append_column(_BUCKET_COL, pa.array(buckets, type=pa.int32()))
    for b in range(num_buckets):
        mask = pc.equal(annotated.column(_BUCKET_COL), pa.scalar(b, pa.int32()))
        sub = annotated.filter(mask).drop([_BUCKET_COL])
        if sub.num_rows == 0:
            continue
        yield b, sub
