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
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sql_as_dag.compiler import UnsupportedSQLError, compile_sql
from sql_as_dag.ir import Source
from sql_as_dag.runtime.coordinator import gather_shuffle
from sql_as_dag.runtime.executor import execute_sql
from sql_as_dag.runtime.shuffle import hash_partition


@pytest.fixture
def orders_source(tmp_path: Path) -> Source:
    p = tmp_path / "orders.parquet"
    pq.write_table(pa.table({"customer_id": ["a", "b"], "amount": [10, 20]}), p)
    return Source(
        source_id="orders_src",
        table_name="orders",
        connector="parquet",
        options={"uris": [p.resolve().as_uri()]},
    )


def test_passthrough_compiles_to_single_pipeline_stage(orders_source: Source) -> None:
    graph = compile_sql("SELECT customer_id, amount FROM orders WHERE amount > 5", [orders_source])
    assert len(graph.stages) == 1
    stage = graph.stages[0]
    assert stage.output_exchange.kind == "pipeline"
    assert graph.sink_stage_id == stage.stage_id


def test_groupby_compiles_to_partial_shuffle_final(orders_source: Source) -> None:
    graph = compile_sql(
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
        [orders_source],
        num_buckets=3,
        stage_id_prefix="g",
    )
    assert [s.stage_id for s in graph.stages] == ["g_partial", "g_final"]
    partial, final = graph.stages
    assert partial.output_exchange.kind == "hash_shuffle"
    assert partial.output_exchange.keys == ["customer_id"]
    assert partial.output_exchange.num_buckets == 3
    assert "__p_0" in partial.sql
    assert final.output_exchange.kind == "pipeline"
    # Identifiers are always quoted, so a column named after a SQL function cannot be reparsed
    # as a call to it.
    assert 'AS "total"' in final.sql
    assert graph.sink_stage_id == "g_final"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT customer_id, AVG(amount) AS a FROM orders GROUP BY customer_id",
        "SELECT SUM(amount) AS total FROM orders",  # global aggregate, no group keys
    ],
)
def test_unsupported_sql_raises(orders_source: Source, sql: str) -> None:
    with pytest.raises(UnsupportedSQLError):
        compile_sql(sql, [orders_source])


def test_count_star_compiles(orders_source: Source) -> None:
    graph = compile_sql(
        "SELECT customer_id, COUNT(*) AS c FROM orders GROUP BY customer_id",
        [orders_source],
        stage_id_prefix="g",
    )
    partial, final = graph.stages
    assert "count(*) as __p_0" in partial.sql.lower()
    assert 'AS "c"' in final.sql


def test_where_groupby_compiles_with_filter_subquery(orders_source: Source) -> None:
    graph = compile_sql(
        "SELECT customer_id, SUM(amount) AS total FROM orders WHERE amount > 5 GROUP BY customer_id",
        [orders_source],
        stage_id_prefix="g",
    )
    partial = graph.stages[0]
    # the WHERE is folded into the partial via an unparsed subquery
    assert "FROM (" in partial.sql
    assert "amount" in partial.sql
    assert ">" in partial.sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT DISTINCT customer_id FROM orders",
        "SELECT customer_id, COUNT(DISTINCT amount) AS c FROM orders GROUP BY customer_id",
    ],
)
def test_distinct_is_rejected(orders_source: Source, sql: str) -> None:
    with pytest.raises(UnsupportedSQLError):
        compile_sql(sql, [orders_source])


def test_rejects_multiple_sources_for_non_join(orders_source: Source, tmp_path: Path) -> None:
    other_path = tmp_path / "other.parquet"
    pq.write_table(pa.table({"x": [1]}), other_path)
    other = Source(
        source_id="other_src", table_name="other", options={"uris": [other_path.resolve().as_uri()]}
    )
    with pytest.raises(UnsupportedSQLError, match="one source"):
        compile_sql("SELECT customer_id, amount FROM orders WHERE amount > 5", [orders_source, other])


def test_compiled_groupby_runtime_matches_single_node(tmp_path: Path) -> None:
    """Compile a GROUP BY from SQL, then run partial→shuffle→final and check totals."""
    partitions = [
        {"customer_id": ["a", "b", "a", "c", "b"], "amount": [10, 20, 30, 40, 50]},
        {"customer_id": ["a", "d", "c", "b", "a"], "amount": [60, 70, 80, 90, 100]},
    ]
    uris = []
    for i, data in enumerate(partitions):
        p = tmp_path / f"orders_{i}.parquet"
        pq.write_table(pa.table(data), p)
        uris.append(p.resolve().as_uri())
    source = Source(source_id="orders_src", table_name="orders", options={"uris": uris})

    num_buckets = 3
    graph = compile_sql(
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
        [source],
        num_buckets=num_buckets,
    )
    partial, final = graph.stages

    # Map side: run the partial SQL per source partition, shuffle by customer_id.
    stage_outputs: list[dict] = []
    for pid, data in enumerate(partitions):
        result = execute_sql({"orders": pa.table(data)}, partial.sql)
        files: list[dict] = []
        for bucket, sub in hash_partition(result, partial.output_exchange.keys, num_buckets):
            path = tmp_path / f"p{pid}_b{bucket}.parquet"
            pq.write_table(sub, path)
            files.append({"path": path.resolve().as_uri(), "bucket": bucket, "rows": sub.num_rows})
        stage_outputs.append({"partition_id": pid, "files": files, "rows_in": 0, "rows_out": 0})

    # Reduce side: gather per bucket, run the final SQL.
    plan = gather_shuffle(stage_outputs, num_shuffle_buckets=num_buckets, output_base=tmp_path.as_uri())
    final_table_name = final.inputs[0].table_name
    totals: dict[str, int] = {}
    for entry in plan:
        tables = [pq.read_table(p.removeprefix("file://")) for p in entry["input_paths"]]
        combined = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
        out = execute_sql({final_table_name: combined}, final.sql)
        for cid, total in zip(out.column("customer_id").to_pylist(), out.column("total").to_pylist()):
            totals[cid] = total

    assert totals == {"a": 200, "b": 160, "c": 120, "d": 70}


_PARTITIONS = [
    {"customer_id": ["a", "b", "a", "c", "b"], "amount": [10, 20, 30, 40, 50]},
    {"customer_id": ["a", "d", "c", "b", "a"], "amount": [60, 70, 80, 90, 100]},
]


def _run_compiled_groupby(graph, partitions: list[dict], tmp_path: Path) -> dict:
    """Run a compiled two-stage group-by (partial → shuffle → final) and return {key: value}."""
    partial, final = graph.stages
    num_buckets = partial.output_exchange.num_buckets
    stage_outputs: list[dict] = []
    for pid, data in enumerate(partitions):
        result = execute_sql({"orders": pa.table(data)}, partial.sql)
        files = []
        for bucket, sub in hash_partition(result, partial.output_exchange.keys, num_buckets):
            p = tmp_path / f"{partial.stage_id}_p{pid}_b{bucket}.parquet"
            pq.write_table(sub, p)
            files.append({"path": p.resolve().as_uri(), "bucket": bucket, "rows": sub.num_rows})
        stage_outputs.append({"partition_id": pid, "files": files, "rows_in": 0, "rows_out": 0})

    plan = gather_shuffle(stage_outputs, output_base=tmp_path.as_uri())
    table_name = final.inputs[0].table_name
    out_rows: dict = {}
    for entry in plan:
        tables = [pq.read_table(p.removeprefix("file://")) for p in entry["input_paths"]]
        combined = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
        out = execute_sql({table_name: combined}, final.sql)
        for key, val in zip(out.column(0).to_pylist(), out.column(1).to_pylist()):
            out_rows[key] = val
    return out_rows


def _orders_source_from(partitions: list[dict], tmp_path: Path) -> Source:
    uris = []
    for i, data in enumerate(partitions):
        p = tmp_path / f"orders_{i}.parquet"
        pq.write_table(pa.table(data), p)
        uris.append(p.resolve().as_uri())
    return Source(source_id="orders_src", table_name="orders", options={"uris": uris})


def test_count_star_runtime_counts_rows(tmp_path: Path) -> None:
    source = _orders_source_from(_PARTITIONS, tmp_path)
    graph = compile_sql(
        "SELECT customer_id, COUNT(*) AS c FROM orders GROUP BY customer_id", [source], num_buckets=3
    )
    assert _run_compiled_groupby(graph, _PARTITIONS, tmp_path) == {"a": 4, "b": 3, "c": 2, "d": 1}


def test_where_groupby_runtime_matches_single_node(tmp_path: Path) -> None:
    source = _orders_source_from(_PARTITIONS, tmp_path)
    graph = compile_sql(
        "SELECT customer_id, SUM(amount) AS total FROM orders WHERE amount > 25 GROUP BY customer_id",
        [source],
        num_buckets=3,
    )
    assert _run_compiled_groupby(graph, _PARTITIONS, tmp_path) == {"a": 190, "b": 140, "c": 120, "d": 70}


def _run_compiled_to_table(graph, partitions: list[dict], tmp_path: Path) -> pa.Table:
    """Run a compiled two-stage group-by and return the concatenated result table."""
    partial, final = graph.stages
    num_buckets = partial.output_exchange.num_buckets
    stage_outputs: list[dict] = []
    for pid, data in enumerate(partitions):
        result = execute_sql({"orders": pa.table(data)}, partial.sql)
        files = []
        for bucket, sub in hash_partition(result, partial.output_exchange.keys, num_buckets):
            p = tmp_path / f"tbl_{partial.stage_id}_p{pid}_b{bucket}.parquet"
            pq.write_table(sub, p)
            files.append({"path": p.resolve().as_uri(), "bucket": bucket, "rows": sub.num_rows})
        stage_outputs.append({"partition_id": pid, "files": files, "rows_in": 0, "rows_out": 0})

    plan = gather_shuffle(stage_outputs, output_base=tmp_path.as_uri())
    table_name = final.inputs[0].table_name
    outs = []
    for entry in plan:
        tables = [pq.read_table(p.removeprefix("file://")) for p in entry["input_paths"]]
        combined = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
        outs.append(execute_sql({table_name: combined}, final.sql))
    return pa.concat_tables(outs)


def _sorted_rows(table: pa.Table) -> list[tuple]:
    """Rows as tuples in column order, sorted, so two results compare independently of bucketing."""
    return sorted(tuple(row[name] for name in table.column_names) for row in table.to_pylist())


@pytest.mark.parametrize(
    "sql",
    [
        # Unaliased aggregates: the output name is the expression itself, which is not a bare
        # identifier and must be quoted in the generated final-stage SQL.
        "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
        "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id",
        "SELECT customer_id, MIN(amount) FROM orders GROUP BY customer_id",
        # An aliased group key must keep its alias.
        "SELECT customer_id AS cid, SUM(amount) AS total FROM orders GROUP BY customer_id",
        # The same aggregate twice: both requested columns must appear.
        "SELECT customer_id, SUM(amount) AS a, SUM(amount) AS b FROM orders GROUP BY customer_id",
        # Aggregate before the group key: projection order must be preserved.
        "SELECT SUM(amount) AS total, customer_id FROM orders GROUP BY customer_id",
        # MIN/MAX combine, end to end rather than only asserted on the emitted SQL.
        "SELECT customer_id, MAX(amount) AS hi, MIN(amount) AS lo FROM orders GROUP BY customer_id",
        "SELECT customer_id, SUM(amount) AS s, COUNT(*) AS n, MIN(amount) AS lo FROM orders GROUP BY customer_id",
        # An alias that only differs by case, plus the plain baseline.
        "SELECT customer_id, SUM(amount) AS Total FROM orders GROUP BY customer_id",
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
        "SELECT customer_id, SUM(amount) AS total FROM orders WHERE amount > 25 GROUP BY customer_id",
    ],
)
def test_compiled_output_matches_single_node(tmp_path: Path, sql: str) -> None:
    """
    The two-stage plan must return exactly what a single-node engine returns.

    Column names, column order, and values are all compared, because the alias-recovery step
    rebuilds the final SELECT and is the easiest place to silently drop or rename a column —
    which would be a wrong answer, not an error.
    """
    source = _orders_source_from(_PARTITIONS, tmp_path)
    graph = compile_sql(sql, [source], num_buckets=3)
    distributed = _run_compiled_to_table(graph, _PARTITIONS, tmp_path)

    whole = pa.concat_tables([pa.table(p) for p in _PARTITIONS])
    reference = execute_sql({"orders": whole}, sql)

    assert distributed.column_names == reference.column_names
    assert _sorted_rows(distributed) == _sorted_rows(reference)


def test_unaliased_aggregate_emits_valid_sql(tmp_path: Path) -> None:
    """
    An unaliased aggregate must not emit an unquoted expression as an identifier.

    `AS sum(orders.amount)` is a parse error, and because it only surfaces when the final stage
    runs, the DAG would parse and fail mid-run instead of at compile time.
    """
    source = _orders_source_from(_PARTITIONS, tmp_path)
    graph = compile_sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id", [source])
    final = graph.stages[-1]
    assert 'AS "sum(orders.amount)"' in final.sql
    assert "AS sum(orders.amount)" not in final.sql

    # And it really executes: quoting is what makes the emitted SQL parseable.
    partials = pa.table({"customer_id": ["a"], "__p_0": [3]})
    out = execute_sql({final.inputs[0].table_name: partials}, final.sql)
    assert out.column_names == ["customer_id", "sum(orders.amount)"]


def test_empty_result_compiles_and_runs(tmp_path: Path) -> None:
    """A filter that matches nothing yields an empty result, not a failure."""
    source = _orders_source_from(_PARTITIONS, tmp_path)
    graph = compile_sql(
        "SELECT customer_id, SUM(amount) AS total FROM orders WHERE amount > 100000 GROUP BY customer_id",
        [source],
        num_buckets=3,
    )
    partial, final = graph.stages
    # Every map-side partition is empty, so the shuffle emits no buckets at all.
    for data in _PARTITIONS:
        result = execute_sql({"orders": pa.table(data)}, partial.sql)
        assert result.num_rows == 0
        assert list(hash_partition(result, partial.output_exchange.keys, 3)) == []


_TWO_SOURCE_REJECTIONS = [
    # A join is only lowered on its own; a filter or aggregation above it is not handled.
    "SELECT o.customer_id, c.name FROM orders o JOIN customers c ON o.customer_id = c.customer_id "
    "WHERE o.amount > 5",
    "SELECT o.customer_id, SUM(o.amount) AS total FROM orders o "
    "JOIN customers c ON o.customer_id = c.customer_id GROUP BY o.customer_id",
    # Non-INNER joins need null padding on the reduce side.
    "SELECT o.customer_id, c.name FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id",
    # Cross join and non-equi join have no hash key.
    "SELECT o.customer_id, c.name FROM orders o CROSS JOIN customers c",
    "SELECT o.customer_id, c.name FROM orders o JOIN customers c ON o.amount > c.customer_id",
    # Set operations are not implemented.
    "SELECT customer_id FROM orders UNION ALL SELECT customer_id FROM customers",
]


@pytest.mark.parametrize(
    "sql",
    [
        # Aggregates that cannot be combined from partial results.
        "SELECT customer_id, AVG(amount) AS a FROM orders GROUP BY customer_id",
        "SELECT customer_id, COUNT(DISTINCT amount) AS c FROM orders GROUP BY customer_id",
        # No group keys means no shuffle key.
        "SELECT SUM(amount) AS total FROM orders",
        # Only plain column arguments and plain group keys are extracted.
        "SELECT customer_id, SUM(amount * 2) AS total FROM orders GROUP BY customer_id",
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id, amount % 2",
        # Plan shapes above the projection that the compiler refuses to lower.
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id HAVING SUM(amount) > 5",
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id ORDER BY total",
        "SELECT customer_id FROM orders LIMIT 1",
        "SELECT DISTINCT customer_id FROM orders",
        "SELECT customer_id, SUM(amount) OVER (PARTITION BY customer_id) AS w FROM orders",
        # Subqueries in WHERE are not row-local: evaluated per partition they compare each row
        # against its own partition's aggregate, so they must never reach a mapped stage.
        "SELECT customer_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
        "SELECT customer_id, SUM(amount) AS t FROM orders "
        "WHERE amount > (SELECT AVG(amount) FROM orders) GROUP BY customer_id",
        "SELECT customer_id FROM orders WHERE amount IN (SELECT amount FROM orders)",
        "SELECT customer_id FROM orders WHERE EXISTS (SELECT 1 FROM orders o2 WHERE o2.amount = orders.amount)",
    ],
)
def test_rejected_sql_raises_at_compile_time(orders_source: Source, sql: str) -> None:
    """
    Every unsupported construct must be refused when the DAG is parsed.

    These all happen to be rejected today; pinning them means a DataFusion upgrade that changes
    a plan shape turns into a failing test rather than an accepted-but-wrong plan.
    """
    with pytest.raises(UnsupportedSQLError):
        compile_sql(sql, [orders_source])


@pytest.mark.parametrize("sql", _TWO_SOURCE_REJECTIONS)
def test_rejected_two_source_sql_raises_at_compile_time(tmp_path: Path, sql: str) -> None:
    orders_p = tmp_path / "orders.parquet"
    customers_p = tmp_path / "customers.parquet"
    pq.write_table(pa.table({"customer_id": ["a", "b"], "amount": [10, 20]}), orders_p)
    pq.write_table(pa.table({"customer_id": ["a", "b"], "name": ["A", "B"]}), customers_p)
    sources = [
        Source(source_id="orders_src", table_name="orders", options={"uris": [orders_p.resolve().as_uri()]}),
        Source(
            source_id="customers_src",
            table_name="customers",
            options={"uris": [customers_p.resolve().as_uri()]},
        ),
    ]
    with pytest.raises(UnsupportedSQLError):
        compile_sql(sql, sources)


def test_three_table_join_is_rejected(tmp_path: Path) -> None:
    paths = {}
    for name, table in {
        "orders": pa.table({"customer_id": ["a"], "amount": [1]}),
        "customers": pa.table({"customer_id": ["a"], "name": ["A"]}),
        "regions": pa.table({"customer_id": ["a"], "region": ["eu"]}),
    }.items():
        p = tmp_path / f"{name}.parquet"
        pq.write_table(table, p)
        paths[name] = p
    sources = [
        Source(source_id=f"{n}_src", table_name=n, options={"uris": [p.resolve().as_uri()]})
        for n, p in paths.items()
    ]
    sql = (
        "SELECT o.customer_id, c.name, r.region FROM orders o "
        "JOIN customers c ON o.customer_id = c.customer_id "
        "JOIN regions r ON o.customer_id = r.customer_id"
    )
    with pytest.raises(UnsupportedSQLError):
        compile_sql(sql, sources)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT customer_id, (SELECT MAX(amount) FROM orders) AS global_max FROM orders",
        "SELECT customer_id, amount IN (SELECT amount FROM orders) AS f FROM orders",
        "SELECT customer_id, EXISTS (SELECT 1 FROM orders o2 WHERE o2.amount = orders.amount) AS e FROM orders",
        # Nested inside a larger expression rather than projected on its own.
        "SELECT customer_id, amount + (SELECT MAX(amount) FROM orders) AS bumped FROM orders",
        # Same hazard alongside an aggregate.
        "SELECT customer_id, SUM(amount) AS total, (SELECT MAX(amount) FROM orders) AS mx "
        "FROM orders GROUP BY customer_id",
    ],
)
def test_subquery_in_select_list_is_rejected(orders_source: Source, sql: str) -> None:
    """
    A subquery in the SELECT list must be refused, like one in WHERE.

    The stage SQL runs once per partition, so `(SELECT MAX(amount) FROM orders)` would report the
    maximum of whichever rows happen to share a partition. Every partition then returns a
    different "global" value and none is correct — with no error anywhere.
    """
    with pytest.raises(UnsupportedSQLError, match="subquery"):
        compile_sql(sql, [orders_source])


def test_string_literal_resembling_a_subquery_is_not_rejected(tmp_path: Path) -> None:
    """The subquery guard must key on the rendered expression, not a substring of a literal."""
    p = tmp_path / "orders.parquet"
    pq.write_table(pa.table({"customer_id": ["a", "<subquery>"], "amount": [10, 20]}), p)
    source = Source(source_id="orders_src", table_name="orders", options={"uris": [p.resolve().as_uri()]})
    graph = compile_sql("SELECT customer_id FROM orders WHERE customer_id = '<subquery>'", [source])
    assert len(graph.stages) == 1


def test_column_named_after_a_sql_function_is_not_reparsed(tmp_path: Path) -> None:
    """
    A column whose name is a niladic SQL function must be read as a column.

    Emitted unquoted, `max(current_timestamp)` becomes a call to the function and returns a clock
    reading instead of the column's values — wrong values and a wrong type, silently.
    """
    partitions = [
        {"customer_id": ["a"], "current_timestamp": ["z1"]},
        {"customer_id": ["a", "b"], "current_timestamp": ["z3", "z2"]},
    ]
    source = _orders_source_from(partitions, tmp_path)
    sql = 'SELECT customer_id, MAX("current_timestamp") AS mx FROM orders GROUP BY customer_id'
    graph = compile_sql(sql, [source], num_buckets=2)

    distributed = _run_compiled_to_table(graph, partitions, tmp_path)
    whole = pa.concat_tables([pa.table(p) for p in partitions])
    reference = execute_sql({"orders": whole}, sql)

    assert distributed.column_names == reference.column_names
    assert _sorted_rows(distributed) == _sorted_rows(reference)
    assert _sorted_rows(distributed) == [("a", "z3"), ("b", "z2")]


@pytest.mark.parametrize(
    "column",
    ["Mixed_Case", "with space", "clé", 'has"quote', "select", "group", "current_date"],
)
def test_awkward_column_names_round_trip(tmp_path: Path, column: str) -> None:
    """Group keys and aggregate arguments must survive names that are not bare identifiers."""
    partitions = [
        {column: ["a", "b"], "amount": [10, 20]},
        {column: ["a", "c"], "amount": [30, 40]},
    ]
    uris = []
    for i, data in enumerate(partitions):
        p = tmp_path / f"t_{i}.parquet"
        pq.write_table(pa.table(data), p)
        uris.append(p.resolve().as_uri())
    source = Source(source_id="orders_src", table_name="orders", options={"uris": uris})

    quoted = '"' + column.replace('"', '""') + '"'
    sql = f"SELECT {quoted}, SUM(amount) AS total FROM orders GROUP BY {quoted}"
    graph = compile_sql(sql, [source], num_buckets=2)

    distributed = _run_compiled_to_table(graph, partitions, tmp_path)
    whole = pa.concat_tables([pa.table(p) for p in partitions])
    reference = execute_sql({"orders": whole}, sql)

    assert distributed.column_names == reference.column_names
    assert _sorted_rows(distributed) == _sorted_rows(reference)


def test_self_join_is_rejected(tmp_path: Path) -> None:
    p = tmp_path / "orders.parquet"
    pq.write_table(pa.table({"customer_id": ["a", "b"], "amount": [10, 20]}), p)
    source = Source(source_id="orders_src", table_name="orders", options={"uris": [p.resolve().as_uri()]})
    with pytest.raises(UnsupportedSQLError):
        compile_sql(
            "SELECT a.customer_id FROM orders a JOIN orders b ON a.customer_id = b.customer_id",
            [source],
        )
