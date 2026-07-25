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
Built-in Parquet connectors.

``ParquetSourceConnector`` reads a set of Parquet files (one partition per file) and
``ParquetSink`` writes one ``data.parquet`` per result partition. All file access goes
through Airflow's ``ObjectStoragePath`` so the same code works for ``file://`` (local
default) and object stores such as ``s3://``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import ObjectStoragePath

if TYPE_CHECKING:
    from sql_as_dag.connectors.base import PartitionRef


class ParquetSourceConnector:
    """
    Reads Parquet partition files into Arrow.

    Options:
        - ``uris``: explicit list of Parquet file URIs (one partition per file).

    Exactly one source partition is produced per URI.
    """

    def __init__(self, *, uris: list[str]) -> None:
        if not uris:
            raise ValueError("ParquetSourceConnector requires at least one uri")
        self._uris = list(uris)

    def schema(self) -> pa.Schema:
        """
        The schema of the first file, taken as representative of the whole source.

        Reading every file's footer would make DAG parsing scale with the number of files, so
        the files are assumed to share a schema. A divergent file is caught when its partition
        is read, not at parse time.
        """
        with ObjectStoragePath(self._uris[0]).open("rb") as f:
            return pq.read_schema(f)

    def list_partitions(self) -> list[PartitionRef]:
        return [{"uri": uri} for uri in self._uris]

    def read_partition(self, ref: PartitionRef) -> pa.Table:
        with ObjectStoragePath(ref["uri"]).open("rb") as f:
            return pq.read_table(f)

    def estimate_total_rows(self) -> int | None:
        total = 0
        for uri in self._uris:
            with ObjectStoragePath(uri).open("rb") as f:
                total += pq.read_metadata(f).num_rows
        return total

    def estimate_total_bytes(self) -> int | None:
        return sum(ObjectStoragePath(uri).stat().st_size for uri in self._uris)


class ParquetSink:
    """
    Writes each result partition as ``<base_uri>/<run_key>/p<partition_id>/data.parquet``.

    Output is scoped to the run because ``partition_id`` is the bucket id and the bucket count
    varies between runs under an adaptive policy. Sharing one directory would let a run that
    produced fewer buckets overwrite part of the previous result and leave the rest in place, so a
    reader would see rows from two different runs with a freshly written success marker over the
    top. A run-scoped directory makes each result self-contained and immutable.

    ``finalize`` writes an optional ``_SUCCESS`` marker inside the run directory (MapReduce-style)
    plus a ``_LATEST`` pointer at the base naming the most recent run, so a consumer can find the
    newest complete result. It does not move or commit anything: the data files are already
    written in place by :meth:`write`.

    With no ``run_key`` (calling the connector directly rather than through a DAG) the layout is
    the flat ``<base_uri>/p<partition_id>/data.parquet``, and the caller owns the collision risk.
    """

    def __init__(self, *, base_uri: str, write_success_marker: bool = True) -> None:
        self._base = base_uri.rstrip("/")
        self._write_success_marker = write_success_marker

    def _run_dir(self, run_key: str | None) -> str:
        return self._base if run_key is None else f"{self._base}/{run_key}"

    def write(self, table: pa.Table, *, partition_id: int, run_key: str | None = None) -> dict[str, Any]:
        out_dir = ObjectStoragePath(f"{self._run_dir(run_key)}/p{partition_id}")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"
        with out_path.open("wb") as f:
            pq.write_table(table, f)
        return {"path": str(out_path), "rows": table.num_rows, "partition_id": partition_id}

    def finalize(
        self, partition_metas: list[dict[str, Any]], *, run_key: str | None = None
    ) -> dict[str, Any]:
        run_dir = self._run_dir(run_key)
        if self._write_success_marker:
            out = ObjectStoragePath(run_dir)
            out.mkdir(parents=True, exist_ok=True)
            with (out / "_SUCCESS").open("wb") as f:
                f.write(b"")
            if run_key is not None:
                # Pointer to the newest complete result, so consumers do not have to guess which
                # run directory to read.
                with (ObjectStoragePath(self._base) / "_LATEST").open("wb") as f:
                    f.write(run_key.encode())
        return {
            "base_uri": self._base,
            "output_uri": run_dir,
            "partitions": len(partition_metas),
            "rows": sum(m.get("rows", 0) for m in partition_metas),
        }
