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
Build an Airflow DAG from a ``StageGraph``.

Each stage is a dynamically-mapped task group of ``read -> compute -> write``, one instance
per partition. Planner ("technical") tasks between stages decide fan-out and feed the next
stage's ``.expand_kwargs(...)``:

    make_work_dir
      [plan_shuffle_width_<consumer>]   (adaptive bucketing only: one per co-partition group)
      -> plan_partitions_<stage>        (source-fed: one entry per source partition)
         OR gather_shuffle_<stage>      (shuffle-fed: one entry per non-empty bucket)
      -> stage_<stage>.expand_kwargs(read -> compute -> write)
      -> ... -> finalize                (sink.finalize over the terminal stage's writes)

**Adaptive shuffle width.** The number of buckets a map stage writes is a per-partition value
carried in each instance's kwargs (key ``num_buckets``), not a compile-time constant. With a
``BucketingPolicy`` other than ``static``, a ``plan_shuffle_width_<consumer>`` task reads cheap
source metadata (row/byte/partition counts) and decides the width at runtime; all map stages in
the same co-partition group (a GROUP BY's single scan, or a join's two scans) share one width
so they stay co-partitioned. The reduce side groups by whatever bucket ids actually appear, so
it needs no fixed count.

Connectors are reconstructed inside tasks from their ``(name, options)`` via the registry, so
every task is self-contained and serializable. Bulk data moves through Parquet on
``ObjectStoragePath``; only file paths and small metadata cross XCom.
"""

from __future__ import annotations

import tempfile
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.sdk import ObjectStoragePath, dag, task, task_group

from sql_as_dag.connectors.registry import get_sink, get_source
from sql_as_dag.runtime.coordinator import choose_join_strategy, gather_multi
from sql_as_dag.runtime.executor import execute_sql
from sql_as_dag.runtime.io import read_concat, read_table, write_table
from sql_as_dag.runtime.shuffle import hash_partition

if TYPE_CHECKING:
    from sql_as_dag.ir import BucketingPolicy, Sink, Stage, StageGraph


def dag_from_stages(
    graph: StageGraph,
    *,
    dag_id: str,
    sink: Sink,
    bucketing: BucketingPolicy | None = None,
    broadcast_threshold: int = 0,
    work_dir_base: str | None = None,
    **dag_kwargs: Any,
):
    """
    Build a DAG that executes ``graph`` and writes the result through ``sink``.

    ``bucketing`` chooses the shuffle width: ``None`` or a ``static`` policy uses each
    exchange's compile-time ``num_buckets`` (or the policy's fixed count); a ``rows`` / ``bytes``
    / ``partitions`` policy decides the width at runtime from source metadata.

    ``broadcast_threshold`` controls the join strategy: when a join's smaller side produces at
    most this many rows, the join broadcasts the small side instead of co-partitioning.

    ``work_dir_base`` is the base URI for the run's shuffle/scratch directory. ``None`` (default)
    uses a local temp dir; set it to e.g. ``s3://bucket/prefix`` to keep intermediates on an
    object store (required once tasks run on distributed workers).

    ``dag_kwargs`` are forwarded to the ``@dag`` decorator. Returns the instantiated DAG.
    """
    graph.validate()
    _check_supported(graph)

    sink_connector = sink.connector
    sink_options = dict(sink.options)
    sink_stage_id = graph.sink_stage_id
    adaptive = bucketing is not None and bucketing.strategy != "static"
    groups = _copartition_groups(graph)

    @dag(dag_id=dag_id, **dag_kwargs)
    def _generated() -> None:
        @task(task_id="make_work_dir")
        def make_work_dir() -> str:
            run_id = ""
            if work_dir_base is not None:
                from airflow.sdk import get_current_context

                run_id = str(get_current_context().get("run_id", "run"))
            return _mint_work_dir(work_dir_base, dag_id, run_id)

        # trigger_rule: a query that legitimately returns no rows expands the terminal stage to
        # zero mapped instances, which Airflow marks SKIPPED. Under the default all_success rule
        # finalize would be skipped too, leaving no _SUCCESS marker and no sink commit — making
        # "returned nothing" indistinguishable from "never ran". 'none_failed' still refuses to
        # commit when an upstream actually failed.
        @task(task_id="finalize", trigger_rule="none_failed")
        def finalize(partition_metas: list[dict] | None, work_dir: str) -> dict:
            connector = get_sink(sink_connector)(**sink_options)
            return connector.finalize(list(partition_metas or []), run_key=run_key(work_dir))

        work_dir = make_work_dir()

        # Adaptive: one width planner per co-partition group, shared by its members.
        width_by_stage: dict[str, Any] = {}
        if adaptive:
            for consumer_id, members in groups.items():
                width = _make_width_planner(consumer_id, members, graph, bucketing)()
                for member in members:
                    width_by_stage[member.stage_id] = width

        stage_outputs: dict[str, Any] = {}
        for stage in graph.stages:
            is_terminal = stage.stage_id == sink_stage_id
            if stage.inputs[0].source_id is not None:
                source = graph.get_source(stage.inputs[0].source_id)
                num_buckets = _static_width(stage, sink_stage_id, bucketing)
                if stage.stage_id in width_by_stage:
                    num_buckets = width_by_stage[stage.stage_id]
                kwargs = _make_source_prepare(stage, source)(work_dir, num_buckets)
                runner = _make_stage_runner(
                    stage,
                    is_terminal=is_terminal,
                    source_connector=source.connector,
                    source_options=dict(source.options),
                    sink_connector=sink_connector,
                    sink_options=sink_options,
                )
            else:
                upstreams = [graph.get_stage(inp.upstream_stage_id) for inp in stage.inputs]
                upstream_outputs = [stage_outputs[u.stage_id] for u in upstreams]
                kwargs = _make_gather(stage, broadcast_threshold)(upstream_outputs, work_dir)
                runner = _make_stage_runner(
                    stage,
                    is_terminal=is_terminal,
                    source_connector=None,
                    source_options=None,
                    sink_connector=sink_connector,
                    sink_options=sink_options,
                )
            stage_outputs[stage.stage_id] = runner.expand_kwargs(kwargs)

        finalize(stage_outputs[sink_stage_id], work_dir)

    return _generated()


def run_key(work_dir: str) -> str:
    """
    A per-run identifier derived from the run's work directory.

    Sinks use it to keep each run's output separate. The work dir is already unique per run — a
    sanitized run id under a configured base, or a fresh temp directory otherwise — so its last
    path segment is a run identifier that every task can compute from a value it already has,
    with no dependency on the task context.
    """
    return work_dir.rstrip("/").rsplit("/", 1)[-1]


def _mint_work_dir(base: str | None, dag_id: str, run_id: str) -> str:
    """
    Return the run's work-dir URI.

    With no ``base``, a local temp dir (``file://``). With a ``base`` (e.g. ``s3://bucket/x``),
    a run-scoped prefix ``<base>/<dag_id>/<run_id>`` created via ``ObjectStoragePath``.
    """
    if base is None:
        return Path(tempfile.mkdtemp(prefix=f"{dag_id}_")).resolve().as_uri()
    safe_run = "".join(c if (c.isalnum() or c in "-_") else "_" for c in run_id) or "run"
    path = ObjectStoragePath(f"{base.rstrip('/')}/{dag_id}/{safe_run}")
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def _static_width(stage: Stage, sink_stage_id: str, bucketing: BucketingPolicy | None) -> int:
    """Compile-time bucket count for a source-fed stage when not using an adaptive policy."""
    if stage.stage_id == sink_stage_id or stage.output_exchange.kind != "hash_shuffle":
        return 1  # terminal stage writes to the sink; num_buckets is unused
    if bucketing is not None:
        return bucketing.num_buckets
    return stage.output_exchange.num_buckets


def _make_width_planner(consumer_id: str, members: list[Stage], graph: StageGraph, policy: BucketingPolicy):
    """Planner task: decide the shuffle width for a co-partition group from source metadata."""
    source_cfgs = []
    for member in members:
        src = graph.get_source(member.inputs[0].source_id)
        source_cfgs.append((src.connector, dict(src.options)))

    @task(task_id=f"plan_shuffle_width_{consumer_id}")
    def plan_width() -> int:
        total_rows = 0
        total_bytes = 0
        num_partitions = 0
        for connector_name, options in source_cfgs:
            connector = get_source(connector_name)(**options)
            num_partitions += len(connector.list_partitions())
            rows = getattr(connector, "estimate_total_rows", lambda: None)()
            byts = getattr(connector, "estimate_total_bytes", lambda: None)()
            total_rows += rows or 0
            total_bytes += byts or 0
        width = policy.compute(total_rows=total_rows, total_bytes=total_bytes, num_partitions=num_partitions)
        print(
            f"shuffle width for {consumer_id}: strategy={policy.strategy} -> {width} "
            f"(rows={total_rows}, bytes={total_bytes}, partitions={num_partitions}, cap={policy.num_buckets})"
        )
        return width

    return plan_width


def _make_source_prepare(stage: Stage, source):
    """Planner task for a source-fed stage: one kwargs entry per source partition."""
    stage_id = stage.stage_id
    table_name = stage.inputs[0].table_name
    connector_name = source.connector
    options = dict(source.options)

    @task(task_id=f"plan_partitions_{stage_id}")
    def prepare(work_dir: str, num_buckets: int) -> list[dict]:
        connector = get_source(connector_name)(**options)
        base = work_dir.rstrip("/")
        return [
            {
                "input_spec": {table_name: {"partition_ref": ref}},
                "output_dir": f"{base}/{stage_id}/p{i}",
                "partition_id": i,
                "num_buckets": num_buckets,
                "run_key": run_key(work_dir),
            }
            for i, ref in enumerate(connector.list_partitions())
        ]

    return prepare


def _make_gather(stage: Stage, broadcast_threshold: int):
    """
    Planner task for a shuffle-fed stage (always terminal in current shapes).

    One input → align per bucket (GROUP BY final). Two inputs → choose broadcast vs shuffle at
    runtime (an equi-join). Bucket ids are taken from the upstream files (no fixed count).
    """
    stage_id = stage.stage_id
    table_names = [inp.table_name for inp in stage.inputs]

    def _to_kwargs(plan: list[dict], work_dir: str) -> list[dict]:
        return [
            {
                "input_spec": {t: {"input_paths": paths} for t, paths in si["input_paths_by_table"].items()},
                "output_dir": si["output_dir"],
                "partition_id": si["bucket"],
                "num_buckets": 1,  # terminal stage writes to the sink; unused
                "run_key": run_key(work_dir),
            }
            for si in plan
        ]

    if len(table_names) == 2:
        left_table, right_table = table_names

        @task(task_id=f"gather_shuffle_{stage_id}")
        def gather(upstream_outputs: list, work_dir: str) -> list[dict]:
            plan = choose_join_strategy(
                upstream_outputs[0],
                upstream_outputs[1],
                left_table=left_table,
                right_table=right_table,
                output_base=f"{work_dir.rstrip('/')}/{stage_id}",
                broadcast_threshold=broadcast_threshold,
            )
            print(f"join {stage_id}: strategy={ {si['strategy'] for si in plan} }, partitions={len(plan)}")
            return _to_kwargs(plan, work_dir)

        return gather

    @task(task_id=f"gather_shuffle_{stage_id}")
    def gather(upstream_outputs: list, work_dir: str) -> list[dict]:
        upstreams_by_table = {table_names[i]: upstream_outputs[i] for i in range(len(table_names))}
        plan = gather_multi(upstreams_by_table, output_base=f"{work_dir.rstrip('/')}/{stage_id}")
        return _to_kwargs(plan, work_dir)

    return gather


def _make_stage_runner(
    stage: Stage,
    *,
    is_terminal: bool,
    source_connector: str | None,
    source_options: dict | None,
    sink_connector: str,
    sink_options: dict,
):
    """Build the mapped ``read -> compute -> write`` task group for one stage."""
    stage_id = stage.stage_id
    stage_sql = stage.sql
    shuffle_keys = list(stage.output_exchange.keys)

    @task_group(group_id=f"stage_{stage_id}")
    def runner(input_spec: dict, output_dir: str, partition_id: int, num_buckets: int, run_key: str):
        @task(task_id="read")
        def read(input_spec: dict, output_dir: str) -> dict:
            base = output_dir.rstrip("/")
            scratch: dict[str, str] = {}
            for table_name, spec in input_spec.items():
                if "partition_ref" in spec:
                    connector = get_source(source_connector)(**source_options)
                    table = connector.read_partition(spec["partition_ref"])
                else:
                    table = read_concat(spec["input_paths"])
                scratch[table_name] = write_table(table, f"{base}/_scratch_read_{table_name}.parquet")
            return scratch

        @task(task_id="compute")
        def compute(scratch_by_table: dict, output_dir: str) -> str:
            tables = {name: read_table(uri) for name, uri in scratch_by_table.items()}
            result = execute_sql(tables, stage_sql)
            return write_table(result, f"{output_dir.rstrip('/')}/_scratch_compute.parquet")

        @task(task_id="write")
        def write(scratch_uri: str, output_dir: str, partition_id: int, num_buckets: int, run_key: str) -> dict:
            table = read_table(scratch_uri)
            if is_terminal:
                connector = get_sink(sink_connector)(**sink_options)
                return connector.write(table, partition_id=partition_id, run_key=run_key)
            base = output_dir.rstrip("/")
            files: list[dict] = []
            for bucket, sub in hash_partition(table, shuffle_keys, num_buckets):
                path = write_table(sub, f"{base}/bucket_{bucket}.parquet")
                files.append({"path": path, "bucket": bucket, "rows": sub.num_rows})
            return {
                "partition_id": partition_id,
                "files": files,
                "rows_in": table.num_rows,
                "rows_out": sum(f["rows"] for f in files),
            }

        scratch_read = read(input_spec, output_dir)
        scratch_compute = compute(scratch_read, output_dir)
        return write(scratch_compute, output_dir, partition_id, num_buckets, run_key)

    return runner


def _copartition_groups(graph: StageGraph) -> dict[str, list[Stage]]:
    """Group the shuffling (non-sink) stages by their downstream consumer stage."""
    consumer_of: dict[str, str] = {}
    for stage in graph.stages:
        for inp in stage.inputs:
            if inp.upstream_stage_id is not None:
                consumer_of[inp.upstream_stage_id] = stage.stage_id

    groups: dict[str, list[Stage]] = defaultdict(list)
    for stage in graph.stages:
        if stage.stage_id == graph.sink_stage_id or stage.output_exchange.kind != "hash_shuffle":
            continue
        if stage.inputs[0].source_id is None:
            raise NotImplementedError(
                f"shuffling stage {stage.stage_id!r} is not source-fed; multi-level shuffles are not supported yet"
            )
        consumer = consumer_of.get(stage.stage_id)
        if consumer is None:
            raise NotImplementedError(
                f"shuffling stage {stage.stage_id!r} has no downstream consumer; every "
                "non-sink stage must feed another stage"
            )
        groups[consumer].append(stage)
    return dict(groups)


def _check_supported(graph: StageGraph) -> None:
    """Raise ``NotImplementedError`` for graph shapes beyond the current scope."""
    sink_id = graph.sink_stage_id
    for stage in graph.stages:
        source_inputs = [i for i in stage.inputs if i.source_id is not None]
        upstream_inputs = [i for i in stage.inputs if i.upstream_stage_id is not None]
        if source_inputs and upstream_inputs:
            raise NotImplementedError(f"stage {stage.stage_id!r} mixes source and upstream inputs; not supported")
        if source_inputs and len(source_inputs) != 1:
            raise NotImplementedError(
                f"stage {stage.stage_id!r} reads from {len(source_inputs)} sources directly; "
                "shuffle each source through its own scan stage instead"
            )

    sink_stage = graph.get_stage(sink_id)
    if sink_stage.output_exchange.kind != "pipeline":
        raise NotImplementedError(
            "the sink stage must have a 'pipeline' output exchange (it writes to the sink), "
            f"got {sink_stage.output_exchange.kind!r}"
        )

    for stage in graph.stages:
        if stage.stage_id == sink_id:
            continue
        if stage.output_exchange.kind != "hash_shuffle":
            raise NotImplementedError(
                f"non-sink stage {stage.stage_id!r} must shuffle its output (hash_shuffle); "
                f"pipeline chaining between stages is not supported yet, got "
                f"{stage.output_exchange.kind!r}"
            )

    # Stages feeding the same consumer are co-partitioned: the reduce side pairs their buckets
    # by bucket id, so a disagreement on width or key arity would pair unrelated keys and
    # silently drop rows. The compiler always emits matching widths; a hand-built StageGraph
    # can get this wrong, so reject it at DAG-parse time rather than returning a wrong answer.
    for consumer, members in _copartition_groups(graph).items():
        if len(members) < 2:
            continue
        widths = {s.output_exchange.num_buckets for s in members}
        if len(widths) > 1:
            detail = ", ".join(f"{s.stage_id}={s.output_exchange.num_buckets}" for s in members)
            raise ValueError(
                f"co-partitioned stages feeding {consumer!r} disagree on shuffle width "
                f"({detail}); they must all use the same num_buckets or the join loses rows"
            )
        arities = {len(s.output_exchange.keys) for s in members}
        if len(arities) > 1:
            detail = ", ".join(f"{s.stage_id}={len(s.output_exchange.keys)}" for s in members)
            raise ValueError(
                f"co-partitioned stages feeding {consumer!r} disagree on the number of shuffle "
                f"keys ({detail}); the keys are hashed positionally, so the counts must match"
            )
