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
Internal Parquet IO over Airflow's ``ObjectStoragePath``.

Parquet is the fixed internal exchange format: the scratch files between the
``read``/``compute``/``write`` tasks and the shuffle bucket files all go through here. Using
``ObjectStoragePath`` means the same code works for ``file://`` (local default) and object
stores such as ``s3://``.

These are deliberately tiny wrappers — they exist so the rest of the runtime never touches
fsspec/pyarrow file handles directly.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import ObjectStoragePath


def read_table(uri: str) -> pa.Table:
    """Read a Parquet file at ``uri`` into an Arrow table."""
    with ObjectStoragePath(uri).open("rb") as f:
        return pq.read_table(f)


def read_concat(uris: Sequence[str]) -> pa.Table:
    """Read several Parquet files and concatenate them into one Arrow table."""
    if not uris:
        raise ValueError("read_concat requires at least one uri")
    tables = [read_table(u) for u in uris]
    return tables[0] if len(tables) == 1 else pa.concat_tables(tables)


def write_table(table: pa.Table, uri: str) -> str:
    """Write ``table`` as Parquet to ``uri`` (creating parent dirs); return ``uri``."""
    path = ObjectStoragePath(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        pq.write_table(table, f)
    return uri
