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
Apache Iceberg source/sink connectors (optional — requires the ``iceberg`` extra).

These prove the connector abstraction extends to a second table format without touching the
compiler, factory, or runtime. ``pyiceberg`` is imported lazily so this module (and the
connector registry) load fine even when the extra is not installed; only *using* an Iceberg
connector requires ``pip install apache-airflow-providers-sql-as-dag[iceberg]``.

Catalog configuration is plain data (``options``) so it survives XCom and DAG re-parsing:

    {"catalog_name": "demo", "uri": "sqlite:///.../catalog.db",
     "warehouse": "file:///.../warehouse", "identifier": "db.table"}

Source reads one partition per Iceberg data file. Sink writes a Parquet data file per result
partition under the warehouse and, in ``finalize``, commits them to the table in a single
snapshot via ``add_files`` (creating the table from the data's schema if it does not exist).
"""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import ObjectStoragePath

if TYPE_CHECKING:
    from sql_as_dag.connectors.base import PartitionRef


def _load_catalog(catalog_name: str, uri: str, warehouse: str, **props: Any):
    from pyiceberg.catalog.sql import SqlCatalog

    return SqlCatalog(catalog_name, uri=uri, warehouse=warehouse, **props)


class IcebergSourceConnector:
    """Reads an Iceberg table, one data file per partition."""

    def __init__(self, *, catalog_name: str, uri: str, warehouse: str, identifier: str, **props: Any) -> None:
        self._catalog_name = catalog_name
        self._uri = uri
        self._warehouse = warehouse
        self._identifier = identifier
        self._props = props

    def _table(self):
        catalog = _load_catalog(self._catalog_name, self._uri, self._warehouse, **self._props)
        return catalog.load_table(self._identifier)

    def schema(self) -> pa.Schema:
        return self._table().schema().as_arrow()

    def list_partitions(self) -> list[PartitionRef]:
        scan = self._table().scan()
        return [{"file": task.file.file_path} for task in scan.plan_files()]

    def read_partition(self, ref: PartitionRef) -> pa.Table:
        with ObjectStoragePath(ref["file"]).open("rb") as f:
            return pq.read_table(f)

    def estimate_total_rows(self) -> int | None:
        return sum(task.file.record_count for task in self._table().scan().plan_files())

    def estimate_total_bytes(self) -> int | None:
        return sum(task.file.file_size_in_bytes for task in self._table().scan().plan_files())


class IcebergSink:
    """Writes one Parquet data file per partition, then commits them as one snapshot."""

    def __init__(self, *, catalog_name: str, uri: str, warehouse: str, identifier: str, **props: Any) -> None:
        self._catalog_name = catalog_name
        self._uri = uri
        self._warehouse = warehouse
        self._identifier = identifier
        self._props = props

    def _staging_dir(self) -> str:
        rel = self._identifier.replace(".", "/")
        return f"{self._warehouse.rstrip('/')}/_staging/{rel}"

    def write(self, table: pa.Table, *, partition_id: int) -> dict[str, Any]:
        out_dir = ObjectStoragePath(self._staging_dir())
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"p{partition_id}.parquet"
        with out_path.open("wb") as f:
            pq.write_table(table, f)
        return {"path": str(out_path), "rows": table.num_rows, "partition_id": partition_id}

    def finalize(self, partition_metas: list[dict[str, Any]]) -> dict[str, Any]:
        paths = [m["path"] for m in partition_metas if m.get("rows", 0) > 0]
        catalog = _load_catalog(self._catalog_name, self._uri, self._warehouse, **self._props)
        table = self._ensure_table(catalog, paths)
        if paths:
            table.add_files(file_paths=paths)
        return {
            "identifier": self._identifier,
            "partitions": len(partition_metas),
            "rows": sum(m.get("rows", 0) for m in partition_metas),
        }

    def _ensure_table(self, catalog, paths: list[str]):
        from pyiceberg.exceptions import NoSuchTableError

        try:
            return catalog.load_table(self._identifier)
        except NoSuchTableError:
            if not paths:
                raise
            namespace = self._identifier.rsplit(".", 1)[0]
            # Namespace may already exist — that is fine.
            with suppress(Exception):
                catalog.create_namespace(namespace)
            with ObjectStoragePath(paths[0]).open("rb") as f:
                schema = pq.read_schema(f)
            return catalog.create_table(self._identifier, schema=schema)
