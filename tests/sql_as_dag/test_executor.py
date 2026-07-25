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

import pyarrow as pa

from sql_as_dag.runtime.executor import execute_sql


def test_filter_and_project() -> None:
    orders = pa.table({"customer_id": ["a", "b", "a", "c"], "amount": [10, 30, 50, 20]})
    result = execute_sql(
        {"orders": orders},
        "SELECT customer_id, amount FROM orders WHERE amount > 25 ORDER BY amount",
    )
    assert result.column_names == ["customer_id", "amount"]
    assert result.column("amount").to_pylist() == [30, 50]
    assert result.column("customer_id").to_pylist() == ["b", "a"]


def test_aggregate_single_partition() -> None:
    orders = pa.table({"customer_id": ["a", "b", "a"], "amount": [10, 20, 30]})
    result = execute_sql(
        {"orders": orders},
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
    )
    totals = dict(zip(result.column("customer_id").to_pylist(), result.column("total").to_pylist()))
    assert totals == {"a": 40, "b": 20}


def test_multiple_inputs_can_be_registered() -> None:
    left = pa.table({"id": [1, 2], "v": ["x", "y"]})
    right = pa.table({"id": [2, 3], "w": ["p", "q"]})
    result = execute_sql(
        {"left": left, "right": right},
        "SELECT left.id, v, w FROM left JOIN right ON left.id = right.id",
    )
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [2]
