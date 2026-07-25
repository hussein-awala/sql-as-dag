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
Typed payloads shuttled between stages via XCom.

Only metadata travels over XCom — file paths, bucket assignments, and row counts. Bulk
data is written to and read from files (see ``docs/shuffle.md``).
These dicts are JSON-serializable so they survive Airflow's default XCom backend without a
custom serializer.
"""

from __future__ import annotations

from typing import TypedDict


class ShuffleBucketFile(TypedDict):
    """One physical file written by a stage instance for one downstream bucket."""

    path: str
    bucket: int
    rows: int


class StageOutput(TypedDict):
    """
    Return value of one mapped stage-runner instance.

    ``files`` holds one entry per downstream shuffle bucket the instance wrote to. For a
    terminal stage (no shuffle) it has a single entry with ``bucket == 0``. ``rows_in`` /
    ``rows_out`` carry the counts that planner tasks use for adaptive decisions and sanity
    checks.
    """

    partition_id: int
    files: list[ShuffleBucketFile]
    rows_in: int
    rows_out: int


class StagePartitionKwargs(TypedDict):
    """
    One element of the kwargs list handed to the next stage's ``.expand_kwargs(...)``.

    ``input_paths_by_table`` maps each input table name to the file URIs that partition
    must read (two keys for a join). ``output_dir`` is where the partition writes its
    output, and ``partition_id`` identifies the mapped instance.
    """

    input_paths_by_table: dict[str, list[str]]
    output_dir: str
    partition_id: int
