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
Extensible read/write connectors for sql_as_dag.

A connector is the seam that lets the compiler, DAG factory, and runtime stay
format-agnostic: only connectors know how to list, read, and write a particular table
format. Add a new format by writing one :class:`~sql_as_dag.connectors.base.SourceConnector`
/ :class:`~sql_as_dag.connectors.base.SinkConnector` pair and registering
it — no changes to the rest of the engine.

Importing this package registers the built-in Parquet connectors under the name
``"parquet"``.

See ``docs/connectors.md`` for the design narrative.
"""

from __future__ import annotations

from sql_as_dag.connectors.base import (
    PartitionRef,
    SinkConnector,
    SourceConnector,
)
from sql_as_dag.connectors.iceberg import IcebergSink, IcebergSourceConnector
from sql_as_dag.connectors.parquet import ParquetSink, ParquetSourceConnector
from sql_as_dag.connectors.registry import (
    get_sink,
    get_source,
    register_sink,
    register_source,
)

# Register the built-in connectors on import. The Iceberg classes import pyiceberg lazily, so
# registering them here does not require the (optional) iceberg extra to be installed.
register_source("parquet", ParquetSourceConnector)
register_sink("parquet", ParquetSink)
register_source("iceberg", IcebergSourceConnector)
register_sink("iceberg", IcebergSink)

__all__ = [
    "PartitionRef",
    "SourceConnector",
    "SinkConnector",
    "ParquetSourceConnector",
    "ParquetSink",
    "IcebergSourceConnector",
    "IcebergSink",
    "register_source",
    "register_sink",
    "get_source",
    "get_sink",
]
