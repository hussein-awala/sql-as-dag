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
"""
Reduce-side shuffle planning: gather upstream bucket files for the next stage.

These planner ("technical") tasks sit between two stages. The upstream stage's mapped
instances each wrote one file per non-empty bucket; the gather groups those files by bucket
and emits one entry per non-empty bucket — that list drives the downstream stage's
``.expand_kwargs(...)``.

The bucket count is **not** required: by default these helpers group by the bucket ids that
actually appear in the upstream files, which is what makes the runtime-adaptive shuffle width
work (the map side may have chosen any width). Passing an explicit ``num_*_buckets`` switches
on a fixed range plus an out-of-range sanity check (used by tests).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sql_as_dag.runtime.types import StageOutput


def _group_by_bucket(outputs: Sequence[StageOutput], num_buckets: int | None = None) -> dict[int, list[str]]:
    """Group file paths by bucket id. With ``num_buckets`` set, validate the range."""
    by_bucket: dict[int, list[str]] = {b: [] for b in range(num_buckets)} if num_buckets is not None else {}
    for so in outputs:
        for f in so["files"]:
            bucket = f["bucket"]
            if num_buckets is not None and bucket not in by_bucket:
                raise ValueError(
                    f"upstream produced bucket={bucket} but coordinator was "
                    f"configured for {num_buckets} buckets"
                )
            by_bucket.setdefault(bucket, []).append(f["path"])
    return by_bucket


def gather_shuffle(
    stage_outputs: Sequence[StageOutput],
    output_base: str,
    num_shuffle_buckets: int | None = None,
) -> list[dict]:
    """
    Group upstream files by downstream bucket; return one entry per non-empty bucket.

    Each entry is ``{"bucket", "input_paths", "output_dir"}``.
    """
    by_bucket = _group_by_bucket(stage_outputs, num_shuffle_buckets)
    base = output_base.rstrip("/")
    return [
        {"bucket": b, "input_paths": by_bucket[b], "output_dir": f"{base}/bucket_{b}"}
        for b in sorted(by_bucket)
        if by_bucket[b]
    ]


def gather_multi(
    upstreams_by_table: Mapping[str, Sequence[StageOutput]],
    output_base: str,
    num_shuffle_buckets: int | None = None,
) -> list[dict]:
    """
    Align one or more co-partitioned upstreams by bucket (the general reduce-side planner).

    A bucket is emitted only when **every** input table has files for it (inner-join /
    single-table semantics). Each entry is
    ``{"bucket", "input_paths_by_table": {table: [paths]}, "output_dir"}``.
    """
    tables = list(upstreams_by_table)
    per_table = {t: _group_by_bucket(upstreams_by_table[t], num_shuffle_buckets) for t in tables}
    present = sorted({b for by_bucket in per_table.values() for b in by_bucket})

    base = output_base.rstrip("/")
    plan: list[dict] = []
    for b in present:
        paths_by_table = {t: per_table[t].get(b, []) for t in tables}
        if all(paths_by_table[t] for t in tables):
            plan.append(
                {"bucket": b, "input_paths_by_table": paths_by_table, "output_dir": f"{base}/bucket_{b}"}
            )
    return plan


def choose_join_strategy(
    left_outputs: Sequence[StageOutput],
    right_outputs: Sequence[StageOutput],
    *,
    left_table: str,
    right_table: str,
    output_base: str,
    broadcast_threshold: int = 0,
    num_buckets: int | None = None,
) -> list[dict]:
    """
    Decide, at runtime, how to combine the two shuffled join inputs.

    Sums the rows each side produced (carried in XCom) and, when the smaller side is at or
    below ``broadcast_threshold`` rows, uses a **broadcast** join — every populated bucket of
    the larger side is joined against *all* of the smaller side's files. Otherwise it uses a
    **shuffle** join (co-partition both sides per bucket). Both are correct for INNER
    equi-joins. Returns entries ``{"bucket", "strategy", "input_paths_by_table", "output_dir"}``.
    """
    left_total = sum(f["rows"] for so in left_outputs for f in so["files"])
    right_total = sum(f["rows"] for so in right_outputs for f in so["files"])
    left_by_bucket = _group_by_bucket(left_outputs, num_buckets)
    right_by_bucket = _group_by_bucket(right_outputs, num_buckets)
    base = output_base.rstrip("/")

    if broadcast_threshold > 0 and min(left_total, right_total) <= broadcast_threshold:
        if right_total <= left_total:
            big_table, big_by_bucket = left_table, left_by_bucket
            small_table, small_by_bucket = right_table, right_by_bucket
        else:
            big_table, big_by_bucket = right_table, right_by_bucket
            small_table, small_by_bucket = left_table, left_by_bucket
        small_all = [p for paths in small_by_bucket.values() for p in paths]
        return [
            {
                "bucket": b,
                "strategy": "broadcast",
                "input_paths_by_table": {big_table: big_by_bucket[b], small_table: small_all},
                "output_dir": f"{base}/bucket_{b}",
            }
            for b in sorted(big_by_bucket)
            if big_by_bucket[b] and small_all
        ]

    present = sorted(set(left_by_bucket) | set(right_by_bucket))
    return [
        {
            "bucket": b,
            "strategy": "shuffle",
            "input_paths_by_table": {
                left_table: left_by_bucket.get(b, []),
                right_table: right_by_bucket.get(b, []),
            },
            "output_dir": f"{base}/bucket_{b}",
        }
        for b in present
        if left_by_bucket.get(b) and right_by_bucket.get(b)
    ]
