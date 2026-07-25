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
SQL → ``StageGraph`` compiler.

DataFusion parses the user's SQL into a ``LogicalPlan``; this module pattern-matches three
shapes and emits a ``StageGraph`` (see ``docs/compiler-and-ir.md``):

* **No aggregate** — ``Projection → [Filter] → TableScan``. One stage runs the user's SQL
  verbatim against the source (``pipeline`` output → sink).

* **Single aggregate** — ``Projection → Aggregate → [Filter] → TableScan``. Two stages:
  ``partial`` runs ``SUM/COUNT/MIN/MAX`` per source partition with ``__p_<i>`` aliases and
  hash-shuffles by the group keys; ``final`` combines the partials (``COUNT`` combines with
  ``SUM``). A ``WHERE`` is folded into the partial stage's ``FROM``. ``AVG`` and ``DISTINCT``
  aggregates are rejected (not combinable from partials).

* **Single INNER equi-join** — ``Projection → Join``. Both sides are hash-shuffled by their
  join key into the same width, then the join stage runs the original SQL per bucket.

Anything else (``HAVING``, sub-queries, window functions, global aggregates, multi-key or
non-INNER joins, multi-source non-join FROM) raises :class:`UnsupportedSQLError`.

All DataFusion logical-plan introspection is confined to this module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa

from sql_as_dag.connectors.registry import get_source
from sql_as_dag.ir import Exchange, Source, Stage, StageGraph, StageInput

if TYPE_CHECKING:
    from collections.abc import Sequence


class UnsupportedSQLError(NotImplementedError):
    """Raised when the input SQL falls outside the compiler's currently-supported shapes."""


# partial-stage aggregate func → combine func used in the final stage.
# count is combined by sum (sum-of-counts); sum/min/max are self-combining.
_COMBINE: dict[str, str] = {"sum": "sum", "count": "sum", "min": "min", "max": "max"}


def compile_sql(
    sql: str,
    sources: Sequence[Source],
    *,
    num_buckets: int = 4,
    stage_id_prefix: str = "q",
) -> StageGraph:
    """Compile ``sql`` into a ``StageGraph`` against the given ``sources``."""
    df = _parse(sql, sources)
    info = _classify(df.logical_plan())
    if info["kind"] == "join":
        return _emit_join(sql, df, sources, num_buckets, stage_id_prefix)

    if len(sources) != 1:
        raise UnsupportedSQLError(f"non-join queries support exactly one source, got {len(sources)}")
    source = sources[0]
    if info["kind"] == "passthrough":
        stages = _emit_passthrough(sql, source, stage_id_prefix)
    else:
        stages = _emit_aggregate(info, source, num_buckets, stage_id_prefix)
    graph = StageGraph(sources=list(sources), stages=stages, sink_stage_id=stages[-1].stage_id)
    graph.validate()
    return graph


def _parse(sql: str, sources: Sequence[Source]) -> Any:
    """Register each source's schema in an empty DataFusion context; return the DataFrame."""
    import datafusion

    ctx = datafusion.SessionContext()
    for src in sources:
        schema = get_source(src.connector)(**src.options).schema()
        # register_record_batches panics on an empty batch list; materialise one explicit
        # empty batch so the table exists with the right schema for planning/validation.
        empty_batch = pa.RecordBatch.from_pylist([], schema=schema)
        ctx.register_record_batches(src.table_name, [[empty_batch]])
    return ctx.sql(sql)


def _classify(plan: Any) -> dict[str, Any]:
    """Identify which supported shape ``plan`` is. Raises on unsupported shapes."""
    proj = plan.to_variant()
    if _name(proj) != "Projection":
        raise UnsupportedSQLError(f"top-level node must be a Projection, got {_name(proj)}")

    inner_plan = plan.inputs()[0]
    inner = inner_plan.to_variant()
    inner_name = _name(inner)

    if inner_name == "Join":
        return {"kind": "join"}

    if inner_name == "Aggregate":
        agg_input = inner_plan.inputs()[0]  # the node feeding the Aggregate (TableScan or Filter)
        has_filter = _name(agg_input.to_variant()) == "Filter"
        # Walk through any Filter(s) down to the TableScan (so WHERE + GROUP BY is allowed).
        cur = agg_input
        while _name(cur.to_variant()) == "Filter":
            cur = cur.inputs()[0]
        if _name(cur.to_variant()) != "TableScan":
            raise UnsupportedSQLError(
                f"unsupported node {_name(agg_input.to_variant())} between Aggregate and TableScan"
            )
        return {
            "kind": "aggregate",
            "table_name": _call(cur.to_variant().table_name),
            "group_keys": _extract_group_keys(inner),
            "aggregates": _extract_aggregates(inner),
            "output_aliases": _extract_output_aliases(proj),
            # When a WHERE is present, the filtered-scan subtree is unparsed into the partial SQL.
            "filtered_scan": agg_input if has_filter else None,
        }

    if inner_name in {"Filter", "TableScan"}:
        cur = inner_plan
        while _name(cur.to_variant()) == "Filter":
            cur = cur.inputs()[0]
        if _name(cur.to_variant()) != "TableScan":
            raise UnsupportedSQLError(f"expected TableScan under Projection, got {_name(cur.to_variant())}")
        return {"kind": "passthrough"}

    raise UnsupportedSQLError(f"unsupported plan shape: Projection → {inner_name}")


def _extract_group_keys(agg: Any) -> list[str]:
    keys: list[str] = []
    for g in _call(agg.group_by_exprs):
        gv = g.to_variant()
        if _name(gv) != "Column":
            raise UnsupportedSQLError(
                f"GROUP BY expression must be a plain column reference, got {g.canonical_name()!r}"
            )
        keys.append(_call(gv.name))
    if not keys:
        raise UnsupportedSQLError("global aggregate (no GROUP BY keys) is not supported yet")
    return keys


def _extract_aggregates(agg: Any) -> list[tuple[str, str, str]]:
    """Return ``[(func, arg_col, schema_name), ...]`` — one tuple per aggregate."""
    out: list[tuple[str, str, str]] = []
    for ax in _call(agg.agg_expressions):
        func = agg.agg_func_name(ax)
        if func not in _COMBINE:
            raise UnsupportedSQLError(
                f"aggregate function {func!r} not supported (supported: {sorted(_COMBINE)})"
            )
        # DISTINCT aggregates (COUNT(DISTINCT x), SUM(DISTINCT x), ...) are not sum-combinable
        # across partial/final, so reject them outright rather than return wrong numbers.
        if "DISTINCT" in ax.schema_name():
            raise UnsupportedSQLError(f"DISTINCT aggregates are not supported: {ax.schema_name()!r}")
        args = agg.aggregation_arguments(ax)
        # COUNT(*) lowers to count(Int64(1)) (a literal, not a column) — treat it as a row count.
        if func == "count" and (len(args) != 1 or _name(args[0].to_variant()) != "Column"):
            out.append(("count", "*", ax.schema_name()))
            continue
        if len(args) != 1:
            raise UnsupportedSQLError(f"aggregate {func} with {len(args)} args not supported")
        argv = args[0].to_variant()
        if _name(argv) != "Column":
            raise UnsupportedSQLError(
                f"aggregate argument must be a plain column, got {args[0].canonical_name()!r}"
            )
        out.append((func, _call(argv.name), ax.schema_name()))
    return out


def _extract_output_aliases(proj: Any) -> dict[str, str]:
    """Map ``Aggregate.schema_name`` (e.g. 'sum(orders.amount)') → user-visible name."""
    aliases: dict[str, str] = {}
    for p in _call(proj.projections):
        pv = p.to_variant()
        pn = _name(pv)
        if pn == "Column":
            n = _call(pv.name)
            aliases[n] = n
        elif pn == "Alias":
            alias_name = _call(pv.alias)
            inner = _call(pv.expr)
            inner_var = inner.to_variant()
            if _name(inner_var) == "Column":
                aliases[_call(inner_var.name)] = alias_name
            else:
                # Aggregate (e.g. COUNT(*)) projected with an alias arrives wrapped in nested
                # aliases; unwrap to the underlying expr whose schema_name matches the
                # aggregate's (so the final-stage alias lookup hits).
                target = inner
                tv = inner_var
                while _name(tv) == "Alias":
                    target = _call(tv.expr)
                    tv = target.to_variant()
                aliases[target.schema_name()] = alias_name
        else:
            raise UnsupportedSQLError(
                f"projection items must be columns or aliases, got {pn}: {p.canonical_name()!r}"
            )
    return aliases


def _emit_passthrough(sql: str, source: Source, prefix: str) -> list[Stage]:
    return [
        Stage(
            stage_id=f"{prefix}_main",
            sql=sql,
            inputs=[StageInput(table_name=source.table_name, source_id=source.source_id)],
            output_exchange=Exchange(),
        )
    ]


def _emit_aggregate(info: dict[str, Any], source: Source, num_buckets: int, prefix: str) -> list[Stage]:
    group_keys: list[str] = info["group_keys"]
    aggregates: list[tuple[str, str, str]] = info["aggregates"]
    output_aliases: dict[str, str] = info["output_aliases"]
    table_name: str = info["table_name"]

    if table_name != source.table_name:
        raise UnsupportedSQLError(
            f"query references table {table_name!r} but source declares {source.table_name!r}"
        )

    # FROM clause: the bare table, or the unparsed filtered-scan subquery when a WHERE is present.
    if info.get("filtered_scan") is not None:
        from_clause = f"({_unparse(info['filtered_scan'])})"
    else:
        from_clause = table_name

    # Partial SQL: groups + AGG(col) AS __p_<i>
    partial_select: list[str] = list(group_keys)
    partial_aggs: list[tuple[str, str, str]] = []  # (partial_col, func, schema_name)
    for i, (func, arg_col, schema_name) in enumerate(aggregates):
        partial_col = f"__p_{i}"
        partial_select.append(f"{func}({arg_col}) AS {partial_col}")
        partial_aggs.append((partial_col, func, schema_name))
    partial_sql = f"SELECT {', '.join(partial_select)} FROM {from_clause} GROUP BY {', '.join(group_keys)}"

    # Final SQL: groups + combine(__p_<i>) AS <user alias>
    partials_name = f"{prefix}_partials"
    final_select: list[str] = list(group_keys)
    for partial_col, func, schema_name in partial_aggs:
        combine = _COMBINE[func]
        out_alias = output_aliases.get(schema_name, schema_name)
        final_select.append(f"{combine}({partial_col}) AS {out_alias}")
    final_sql = f"SELECT {', '.join(final_select)} FROM {partials_name} GROUP BY {', '.join(group_keys)}"

    partial_id = f"{prefix}_partial"
    return [
        Stage(
            stage_id=partial_id,
            sql=partial_sql,
            inputs=[StageInput(table_name=table_name, source_id=source.source_id)],
            output_exchange=Exchange(kind="hash_shuffle", keys=group_keys, num_buckets=num_buckets),
        ),
        Stage(
            stage_id=f"{prefix}_final",
            sql=final_sql,
            inputs=[StageInput(table_name=partials_name, upstream_stage_id=partial_id)],
            output_exchange=Exchange(),
        ),
    ]


def _emit_join(sql: str, df: Any, sources: Sequence[Source], num_buckets: int, prefix: str) -> StageGraph:
    """
    Compile a two-table INNER equi-join.

    Each side is scanned and hash-shuffled by its join key into the same number of buckets,
    so matching rows co-locate; the join stage reads one bucket from each side and runs the
    original user SQL (correct because hash partitioning co-locates equal keys, and the
    per-bucket inner joins union to the full result). Join keys are read from the *optimized*
    plan, where DataFusion surfaces them as ``Join.on`` (the unoptimized plan keeps them in a
    filter).
    """
    join_node = _find_join(df.optimized_logical_plan())
    if join_node is None:
        raise UnsupportedSQLError("expected a Join node but none was found")
    jv = join_node.to_variant()

    join_type = str(_call(jv.join_type)).rsplit(".", 1)[-1]
    if join_type != "Inner":
        raise UnsupportedSQLError(f"only INNER equi-joins are supported, got {join_type}")
    on = _call(jv.on)
    if len(on) != 1:
        raise UnsupportedSQLError(f"only single equi-key joins are supported, got {len(on)} key pairs")
    left_key = _column_name(on[0][0])
    right_key = _column_name(on[0][1])

    left_table = _tablescan_name(join_node.inputs()[0])
    right_table = _tablescan_name(join_node.inputs()[1])
    if left_table == right_table:
        raise UnsupportedSQLError("self-joins are not supported")

    src_by_table = {s.table_name: s for s in sources}
    for table in (left_table, right_table):
        if table not in src_by_table:
            raise UnsupportedSQLError(f"join references table {table!r} with no matching source")

    scan_left = Stage(
        stage_id=f"{prefix}_scan_{left_table}",
        sql=f"SELECT * FROM {left_table}",
        inputs=[StageInput(table_name=left_table, source_id=src_by_table[left_table].source_id)],
        output_exchange=Exchange(kind="hash_shuffle", keys=[left_key], num_buckets=num_buckets),
    )
    scan_right = Stage(
        stage_id=f"{prefix}_scan_{right_table}",
        sql=f"SELECT * FROM {right_table}",
        inputs=[StageInput(table_name=right_table, source_id=src_by_table[right_table].source_id)],
        output_exchange=Exchange(kind="hash_shuffle", keys=[right_key], num_buckets=num_buckets),
    )
    join_stage = Stage(
        stage_id=f"{prefix}_join",
        sql=sql,
        inputs=[
            StageInput(table_name=left_table, upstream_stage_id=scan_left.stage_id),
            StageInput(table_name=right_table, upstream_stage_id=scan_right.stage_id),
        ],
        output_exchange=Exchange(),
    )
    graph = StageGraph(
        sources=list(sources),
        stages=[scan_left, scan_right, join_stage],
        sink_stage_id=join_stage.stage_id,
    )
    graph.validate()
    return graph


def _find_join(node: Any) -> Any:
    """Depth-first search for the first ``Join`` node, returning the plan node (not variant)."""
    if _name(node.to_variant()) == "Join":
        return node
    for child in node.inputs():
        found = _find_join(child)
        if found is not None:
            return found
    return None


def _tablescan_name(node: Any) -> str:
    """Descend a single-input chain to the underlying ``TableScan`` and return its table name."""
    variant = node.to_variant()
    if _name(variant) == "TableScan":
        return _call(variant.table_name)
    children = node.inputs()
    if len(children) == 1:
        return _tablescan_name(children[0])
    raise UnsupportedSQLError(f"could not resolve a single source table under a {_name(variant)} node")


def _column_name(expr: Any) -> str:
    variant = expr.to_variant()
    # DataFusion inserts a CAST into the join key whenever the two sides' types differ (even
    # int32 vs int64). Independent per-side hashing on differently-typed values would route
    # equal-but-differently-typed keys to different buckets and silently drop matches, so we
    # reject rather than risk a wrong answer. The two key columns must already share a type.
    if _name(variant) == "Cast":
        raise UnsupportedSQLError(
            "join key types are not co-partition compatible (DataFusion inserted a CAST); "
            "cast both join keys to the same type explicitly in the query"
        )
    if _name(variant) != "Column":
        raise UnsupportedSQLError(f"join key must be a plain column, got {_name(variant)}")
    return _call(variant.name)


def _unparse(plan: Any) -> str:
    """Render a DataFusion ``LogicalPlan`` subtree back to SQL (used to fold WHERE into partials)."""
    from datafusion.unparser import Dialect, Unparser

    return Unparser(Dialect.default()).plan_to_sql(plan)


# A handful of DataFusion attributes are exposed as methods, others as values.
def _call(attr: Any) -> Any:
    return attr() if callable(attr) else attr


def _name(variant: Any) -> str:
    return type(variant).__name__
