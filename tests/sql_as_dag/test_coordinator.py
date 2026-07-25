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

import pytest

from sql_as_dag.runtime.coordinator import gather_multi, gather_shuffle


def _output(partition_id: int, files: list[tuple[str, int]]) -> dict:
    return {
        "partition_id": partition_id,
        "files": [{"path": p, "bucket": b, "rows": 1} for p, b in files],
        "rows_in": len(files),
        "rows_out": len(files),
    }


def test_groups_files_by_bucket_across_partitions() -> None:
    outputs = [
        _output(0, [("p0_b0", 0), ("p0_b1", 1)]),
        _output(1, [("p1_b0", 0), ("p1_b2", 2)]),
    ]
    plan = gather_shuffle(outputs, num_shuffle_buckets=3, output_base="file:///wd/final")

    by_bucket = {entry["bucket"]: entry for entry in plan}
    # bucket 0 gathers both partitions' files; bucket 1 and 2 one each.
    assert sorted(by_bucket[0]["input_paths"]) == ["p0_b0", "p1_b0"]
    assert by_bucket[1]["input_paths"] == ["p0_b1"]
    assert by_bucket[2]["input_paths"] == ["p1_b2"]
    assert by_bucket[0]["output_dir"] == "file:///wd/final/bucket_0"


def test_skips_empty_buckets() -> None:
    outputs = [_output(0, [("p0_b1", 1)])]
    plan = gather_shuffle(outputs, num_shuffle_buckets=4, output_base="file:///wd/final")
    assert [entry["bucket"] for entry in plan] == [1]


def test_rejects_out_of_range_bucket() -> None:
    outputs = [_output(0, [("p0_b5", 5)])]
    with pytest.raises(ValueError, match="configured for 3 buckets"):
        gather_shuffle(outputs, num_shuffle_buckets=3, output_base="file:///wd/final")


def test_gather_multi_single_table_matches_buckets() -> None:
    outputs = [_output(0, [("p0_b0", 0), ("p0_b2", 2)]), _output(1, [("p1_b0", 0)])]
    plan = gather_multi({"partials": outputs}, num_shuffle_buckets=3, output_base="file:///wd/final")
    by_bucket = {e["bucket"]: e for e in plan}
    assert set(by_bucket) == {0, 2}
    assert sorted(by_bucket[0]["input_paths_by_table"]["partials"]) == ["p0_b0", "p1_b0"]


def test_gather_multi_join_only_emits_buckets_present_on_both_sides() -> None:
    left = [_output(0, [("l_b0", 0), ("l_b1", 1)])]
    right = [_output(0, [("r_b1", 1), ("r_b2", 2)])]
    plan = gather_multi(
        {"orders": left, "customers": right},
        num_shuffle_buckets=3,
        output_base="file:///wd/join",
    )
    # only bucket 1 has files on both sides.
    assert [e["bucket"] for e in plan] == [1]
    entry = plan[0]
    assert entry["input_paths_by_table"]["orders"] == ["l_b1"]
    assert entry["input_paths_by_table"]["customers"] == ["r_b1"]
