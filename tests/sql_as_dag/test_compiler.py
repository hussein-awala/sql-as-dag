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
    assert "AS total" in final.sql
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
    assert "AS c" in final.sql


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
