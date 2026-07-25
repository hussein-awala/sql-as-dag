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
Per-partition SQL execution via Apache DataFusion.

This module is the single place that talks to DataFusion's Python API, which is
version-sensitive — isolating it here keeps the version-coupling contained. It is
Airflow-free and operates on Arrow tables in, Arrow table out, so it is fully
unit-testable without a DAG.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    import pyarrow as pa


def execute_sql(inputs: Mapping[str, pa.Table], sql: str) -> pa.Table:
    """
    Run ``sql`` against the given inputs and return the result as an Arrow table.

    ``inputs`` maps a SQL table name to the Arrow table registered under it. Each input
    table's record batches are registered as one DataFusion partition, preserving
    DataFusion's internal parallelism without concatenating into a single buffer.
    """
    # Imported lazily so importing the provider does not require datafusion until a stage
    # actually runs.
    from datafusion import SessionContext

    ctx = SessionContext()
    for table_name, table in inputs.items():
        ctx.register_record_batches(table_name, [table.to_batches()])
    return ctx.sql(sql).to_arrow_table()
