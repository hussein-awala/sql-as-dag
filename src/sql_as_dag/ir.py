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
Stage IR — the engine-agnostic intermediate representation of a compiled SQL query.

A :class:`StageGraph` is a topologically-ordered list of :class:`Stage` objects plus a set
of external :class:`Source` tables. Each stage runs one piece of per-partition SQL against
its inputs (sources or upstream stages) and declares, via :class:`Exchange`, how its output
is partitioned for the next stage.

The SQL compiler produces a ``StageGraph``; the DAG factory consumes it. Treat instances as
immutable after construction. ``StageGraph.validate()`` runs structural checks and is the
single source of truth for what a well-formed graph looks like.

See ``docs/compiler-and-ir.md`` for the design narrative.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

#: Bucketing strategies for deciding shuffle width.
#: - ``static``: always ``num_buckets``.
#: - ``rows``: ``ceil(total_rows / target_rows_per_bucket)``, capped at ``num_buckets``.
#: - ``bytes``: ``ceil(total_bytes / target_bytes_per_bucket)``, capped at ``num_buckets``.
#: - ``partitions``: number of source partitions, capped at ``num_buckets``.
BUCKETING_STRATEGIES = frozenset({"static", "rows", "bytes", "partitions"})


@dataclass
class BucketingPolicy:
    """
    DAG-level policy for choosing the shuffle width (number of buckets) at runtime.

    ``num_buckets`` is the bucket count for ``static`` and the **maximum** (cap) for the
    adaptive strategies. The adaptive strategies are evaluated by a planner task from cheap
    source metadata (row counts, byte sizes, or partition counts) before the map side runs,
    so the same query fans out wider for big inputs and narrower for small ones.
    """

    strategy: str = "static"
    num_buckets: int = 4
    target_rows_per_bucket: int = 1_000_000
    target_bytes_per_bucket: int = 128 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.strategy not in BUCKETING_STRATEGIES:
            raise ValueError(
                f"unknown bucketing strategy {self.strategy!r} (expected one of {sorted(BUCKETING_STRATEGIES)})"
            )
        if self.num_buckets < 1:
            raise ValueError(f"num_buckets must be >= 1, got {self.num_buckets}")

    def compute(
        self,
        *,
        total_rows: int | None = None,
        total_bytes: int | None = None,
        num_partitions: int | None = None,
    ) -> int:
        """Resolve the bucket count from observed metadata, clamped to ``[1, num_buckets]``."""
        if self.strategy == "static":
            return self.num_buckets
        if self.strategy == "rows":
            base = math.ceil((total_rows or 0) / self.target_rows_per_bucket)
        elif self.strategy == "bytes":
            base = math.ceil((total_bytes or 0) / self.target_bytes_per_bucket)
        else:  # partitions
            base = num_partitions or 0
        return max(1, min(base, self.num_buckets))


#: Exchange kinds — how a stage's output is moved to the next stage.
#: - ``pipeline``: 1:1 narrow dependency, no re-partitioning.
#: - ``hash_shuffle``: N:M, rows routed to buckets by a hash of ``keys``.
#: - ``broadcast``: 1:N, output replicated to every downstream partition (joins; later).
EXCHANGE_KINDS = frozenset({"pipeline", "hash_shuffle", "broadcast"})


@dataclass
class Source:
    """
    An external table the query reads from.

    ``connector`` names the registered :class:`SourceConnector` (e.g. ``"parquet"``) and
    ``options`` carries connector-specific configuration (paths, table refs, filters). Both
    are kept as plain data so a ``Source`` stays JSON-serializable and free of live objects.
    """

    source_id: str
    table_name: str
    connector: str = "parquet"
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class Sink:
    """
    Where the final query result is written.

    Like :class:`Source`, this is plain data: ``connector`` names a registered
    :class:`SinkConnector` (e.g. ``"parquet"``) and ``options`` carries its configuration
    (e.g. ``{"base_uri": "..."}``). It is a build-time argument to the DAG factory rather
    than part of the compiled graph.
    """

    connector: str = "parquet"
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class StageInput:
    """
    One input to a :class:`Stage`, referencing either a source or an upstream stage.

    Exactly one of ``source_id`` or ``upstream_stage_id`` must be set. ``table_name`` is
    the SQL identifier the input is registered under before the stage's SQL runs.
    """

    table_name: str
    source_id: str | None = None
    upstream_stage_id: str | None = None

    def __post_init__(self) -> None:
        if (self.source_id is None) == (self.upstream_stage_id is None):
            raise ValueError(
                "StageInput must reference exactly one of source_id or upstream_stage_id "
                f"(got source_id={self.source_id!r}, upstream_stage_id={self.upstream_stage_id!r})"
            )


@dataclass
class Exchange:
    """
    How a stage's output is partitioned for the next stage.

    ``pipeline`` keeps the partitioning as-is; ``hash_shuffle`` re-partitions into
    ``num_buckets`` buckets by a hash of ``keys``; ``broadcast`` replicates the output.
    """

    kind: str = "pipeline"
    keys: list[str] = field(default_factory=list)
    num_buckets: int = 1

    def __post_init__(self) -> None:
        if self.kind not in EXCHANGE_KINDS:
            raise ValueError(f"unknown exchange kind {self.kind!r} (expected one of {sorted(EXCHANGE_KINDS)})")
        if self.num_buckets < 1:
            raise ValueError(f"num_buckets must be >= 1, got {self.num_buckets}")
        if self.kind == "hash_shuffle":
            if not self.keys:
                raise ValueError("hash_shuffle exchange requires at least one key")
            if self.num_buckets < 2:
                raise ValueError("hash_shuffle exchange requires num_buckets >= 2")
        if self.kind == "pipeline" and self.num_buckets != 1:
            raise ValueError("pipeline exchange must have num_buckets == 1")


@dataclass
class Stage:
    """
    One pipeline stage.

    ``sql`` runs once per partition against the inputs (each registered under its
    ``table_name``). ``output_exchange`` declares how the result is partitioned for the
    downstream stage.
    """

    stage_id: str
    sql: str
    inputs: list[StageInput]
    output_exchange: Exchange = field(default_factory=Exchange)

    def __post_init__(self) -> None:
        if not self.inputs:
            raise ValueError(f"Stage {self.stage_id!r}: must declare at least one input")
        seen: set[str] = set()
        for inp in self.inputs:
            if inp.table_name in seen:
                raise ValueError(f"Stage {self.stage_id!r}: duplicate input table_name {inp.table_name!r}")
            seen.add(inp.table_name)


@dataclass
class StageGraph:
    """The full plan: sources + topologically-ordered stages + the sink stage reference."""

    sources: list[Source]
    stages: list[Stage]
    sink_stage_id: str

    def get_stage(self, stage_id: str) -> Stage:
        for s in self.stages:
            if s.stage_id == stage_id:
                return s
        raise KeyError(f"unknown stage_id {stage_id!r}")

    def get_source(self, source_id: str) -> Source:
        for s in self.sources:
            if s.source_id == source_id:
                return s
        raise KeyError(f"unknown source_id {source_id!r}")

    def validate(self) -> None:
        """
        Structural checks. Raises :class:`ValueError` on any inconsistency.

        Verifies: unique source and stage ids; every input reference resolves; the sink
        exists; and stages are in topological order (every ``upstream_stage_id`` appears
        earlier in the list than the stage that references it).
        """
        source_ids = {s.source_id for s in self.sources}
        if len(source_ids) != len(self.sources):
            raise ValueError("duplicate source_id in sources")
        stage_ids = {s.stage_id for s in self.stages}
        if len(stage_ids) != len(self.stages):
            raise ValueError("duplicate stage_id in stages")
        if not self.stages:
            raise ValueError("StageGraph must contain at least one stage")
        if self.sink_stage_id not in stage_ids:
            raise ValueError(f"sink_stage_id {self.sink_stage_id!r} not found in stages")

        seen: set[str] = set()
        for stage in self.stages:
            for inp in stage.inputs:
                if inp.source_id is not None and inp.source_id not in source_ids:
                    raise ValueError(f"Stage {stage.stage_id!r} references unknown source {inp.source_id!r}")
                if inp.upstream_stage_id is not None:
                    if inp.upstream_stage_id not in stage_ids:
                        raise ValueError(
                            f"Stage {stage.stage_id!r} references unknown upstream stage {inp.upstream_stage_id!r}"
                        )
                    if inp.upstream_stage_id not in seen:
                        raise ValueError(
                            f"Stage {stage.stage_id!r} references upstream stage "
                            f"{inp.upstream_stage_id!r} that has not been defined yet "
                            "(stages must appear in topological order)"
                        )
            seen.add(stage.stage_id)
