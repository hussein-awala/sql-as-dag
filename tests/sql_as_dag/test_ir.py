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

import pytest

from sql_as_dag.ir import (
    Exchange,
    Source,
    Stage,
    StageGraph,
    StageInput,
)


def _two_stage_graph() -> StageGraph:
    """A well-formed partial-agg -> shuffle -> final-agg graph."""
    src = Source(source_id="orders_src", table_name="orders", connector="parquet", options={"uris": ["x"]})
    partial = Stage(
        stage_id="partial",
        sql="SELECT customer_id, SUM(amount) AS __p_0 FROM orders GROUP BY customer_id",
        inputs=[StageInput(table_name="orders", source_id="orders_src")],
        output_exchange=Exchange(kind="hash_shuffle", keys=["customer_id"], num_buckets=4),
    )
    final = Stage(
        stage_id="final",
        sql="SELECT customer_id, SUM(__p_0) AS total FROM partials GROUP BY customer_id",
        inputs=[StageInput(table_name="partials", upstream_stage_id="partial")],
    )
    return StageGraph(sources=[src], stages=[partial, final], sink_stage_id="final")


def test_valid_graph_passes() -> None:
    _two_stage_graph().validate()


def test_get_stage_and_source() -> None:
    g = _two_stage_graph()
    assert g.get_stage("partial").stage_id == "partial"
    assert g.get_source("orders_src").table_name == "orders"
    with pytest.raises(KeyError):
        g.get_stage("nope")
    with pytest.raises(KeyError):
        g.get_source("nope")


def test_stage_input_requires_exactly_one_ref() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        StageInput(table_name="t")
    with pytest.raises(ValueError, match="exactly one"):
        StageInput(table_name="t", source_id="s", upstream_stage_id="u")


def test_stage_rejects_no_inputs() -> None:
    with pytest.raises(ValueError, match="at least one input"):
        Stage(stage_id="s", sql="SELECT 1", inputs=[])


def test_stage_rejects_duplicate_input_table_names() -> None:
    with pytest.raises(ValueError, match="duplicate input table_name"):
        Stage(
            stage_id="s",
            sql="SELECT 1",
            inputs=[
                StageInput(table_name="t", source_id="a"),
                StageInput(table_name="t", source_id="b"),
            ],
        )


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"kind": "bogus"}, "unknown exchange kind"),
        ({"kind": "hash_shuffle", "keys": [], "num_buckets": 4}, "requires at least one key"),
        ({"kind": "hash_shuffle", "keys": ["k"], "num_buckets": 1}, "num_buckets >= 2"),
        ({"kind": "pipeline", "num_buckets": 3}, "num_buckets == 1"),
        ({"num_buckets": 0}, "num_buckets must be >= 1"),
    ],
)
def test_exchange_validation(kwargs: dict, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        Exchange(**kwargs)


def test_validate_rejects_duplicate_stage_ids() -> None:
    src = Source(source_id="s", table_name="t")
    a = Stage("dup", "SELECT 1", [StageInput(table_name="t", source_id="s")])
    b = Stage("dup", "SELECT 2", [StageInput(table_name="t", source_id="s")])
    with pytest.raises(ValueError, match="duplicate stage_id"):
        StageGraph(sources=[src], stages=[a, b], sink_stage_id="dup").validate()


def test_validate_rejects_unknown_source() -> None:
    a = Stage("a", "SELECT 1", [StageInput(table_name="t", source_id="missing")])
    with pytest.raises(ValueError, match="unknown source"):
        StageGraph(sources=[], stages=[a], sink_stage_id="a").validate()


def test_validate_rejects_unknown_upstream_stage() -> None:
    a = Stage("a", "SELECT 1", [StageInput(table_name="t", upstream_stage_id="ghost")])
    with pytest.raises(ValueError, match="unknown upstream"):
        StageGraph(sources=[], stages=[a], sink_stage_id="a").validate()


def test_validate_rejects_out_of_order_upstream() -> None:
    src = Source(source_id="s", table_name="t")
    # 'first' references 'second' which is defined later -> not topological.
    first = Stage("first", "SELECT 1", [StageInput(table_name="x", upstream_stage_id="second")])
    second = Stage("second", "SELECT 2", [StageInput(table_name="t", source_id="s")])
    with pytest.raises(ValueError, match="topological order"):
        StageGraph(sources=[src], stages=[first, second], sink_stage_id="first").validate()


def test_validate_rejects_missing_sink() -> None:
    src = Source(source_id="s", table_name="t")
    a = Stage("a", "SELECT 1", [StageInput(table_name="t", source_id="s")])
    with pytest.raises(ValueError, match="sink_stage_id"):
        StageGraph(sources=[src], stages=[a], sink_stage_id="not_here").validate()


def test_validate_rejects_empty_stage_list() -> None:
    with pytest.raises(ValueError, match="at least one stage"):
        StageGraph(sources=[], stages=[], sink_stage_id="x").validate()
