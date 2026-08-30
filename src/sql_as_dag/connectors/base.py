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
Connector contracts: the ``SourceConnector`` (read) and ``SinkConnector`` (write) Protocols.

A ``PartitionRef`` is a small JSON-serializable descriptor of one readable partition. It
must survive XCom (it is produced by a planner task and consumed by a per-partition task),
so it is a plain ``dict``.

Connectors deliberately know nothing about Airflow: they take and return Arrow tables plus
plain dicts, which keeps them unit-testable without a DAG. Only the DAG factory wires them
to Airflow.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import pyarrow as pa

#: A JSON-serializable descriptor of one readable source partition. The shape is
#: connector-specific (e.g. ``{"uri": "...parquet"}`` for Parquet); the planner treats it
#: as opaque and hands it back to ``SourceConnector.read_partition``.
PartitionRef = dict[str, Any]


@runtime_checkable
class SourceConnector(Protocol):
    """Reads an external table into Arrow, one partition at a time."""

    def schema(self) -> pa.Schema:
        """
        Return the table schema.

        Called at DAG-parse time so the SQL compiler can register the source with
        DataFusion and validate the query without reading any data.
        """
        ...

    def list_partitions(self) -> list[PartitionRef]:
        """
        Enumerate the readable partitions of this source.

        One ``PartitionRef`` becomes one upstream partition (one mapped task-group
        instance at runtime). For Parquet this is one entry per file.
        """
        ...

    def read_partition(self, ref: PartitionRef) -> pa.Table:
        """Read a single partition (identified by ``ref``) into an Arrow table."""
        ...

    def estimate_total_rows(self) -> int | None:
        """
        Cheap row-count estimate across all partitions, or ``None`` if unavailable.

        Used by the adaptive shuffle-width planner. Implementations should read metadata
        only (e.g. Parquet footers, Iceberg manifests) and never scan data.
        """
        ...

    def estimate_total_bytes(self) -> int | None:
        """Cheap total-size estimate (bytes) across all partitions, or ``None``."""
        ...


@runtime_checkable
class SinkConnector(Protocol):
    """Materializes the final query result, one partition at a time, then commits."""

    def write(self, table: pa.Table, *, partition_id: int, run_key: str | None = None) -> dict[str, Any]:
        """
        Write one result partition.

        ``run_key`` identifies the DAG run and is stable across every task in it. A sink should
        include it in the paths it writes so that two runs never collide: ``partition_id`` is the
        bucket id, and the number of buckets varies between runs under an adaptive policy, so
        run-agnostic paths let a later run overwrite part of an earlier result and leave the rest.
        Retries within one run reuse the same paths, which is what makes a retry idempotent.

        Return JSON-serializable metadata (path, row count) suitable for XCom and for
        :meth:`finalize`.
        """
        ...

    def finalize(self, partition_metas: list[dict[str, Any]], *, run_key: str | None = None) -> dict[str, Any]:
        """
        Run the optional commit step after all partitions are written.

        For Parquet this publishes a success marker; for transactional formats such as Iceberg it
        appends the written data files and commits a snapshot. Receives the same ``run_key`` as
        :meth:`write`.

        Must tolerate an empty ``partition_metas``: a query that legitimately matches no rows
        still needs to commit, so that "returned nothing" is distinguishable from "never ran".
        """
        ...
