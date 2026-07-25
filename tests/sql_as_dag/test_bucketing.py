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

from sql_as_dag.connectors.parquet import ParquetSourceConnector
from sql_as_dag.ir import BucketingPolicy


def test_static_returns_num_buckets() -> None:
    assert BucketingPolicy(strategy="static", num_buckets=5).compute() == 5


def test_rows_strategy_scales_and_caps() -> None:
    policy = BucketingPolicy(strategy="rows", num_buckets=8, target_rows_per_bucket=3)
    assert policy.compute(total_rows=10) == 4  # ceil(10/3)
    assert policy.compute(total_rows=100) == 8  # capped at num_buckets
    assert policy.compute(total_rows=0) == 1  # clamped to >= 1


def test_bytes_strategy() -> None:
    policy = BucketingPolicy(strategy="bytes", num_buckets=8, target_bytes_per_bucket=300)
    assert policy.compute(total_bytes=1000) == 4  # ceil(1000/300)


def test_partitions_strategy() -> None:
    policy = BucketingPolicy(strategy="partitions", num_buckets=4)
    assert policy.compute(num_partitions=3) == 3
    assert policy.compute(num_partitions=10) == 4  # capped


def test_invalid_policy() -> None:
    with pytest.raises(ValueError, match="unknown bucketing strategy"):
        BucketingPolicy(strategy="nope")
    with pytest.raises(ValueError, match="num_buckets must be >= 1"):
        BucketingPolicy(num_buckets=0)


def test_parquet_connector_estimates(tmp_path: Path) -> None:
    p0 = tmp_path / "p0.parquet"
    p1 = tmp_path / "p1.parquet"
    pq.write_table(pa.table({"a": list(range(40))}), p0)
    pq.write_table(pa.table({"a": list(range(60))}), p1)
    conn = ParquetSourceConnector(uris=[p0.resolve().as_uri(), p1.resolve().as_uri()])
    assert conn.estimate_total_rows() == 100
    assert conn.estimate_total_bytes() > 0
