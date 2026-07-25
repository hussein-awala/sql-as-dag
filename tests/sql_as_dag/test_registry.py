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

import pytest

from sql_as_dag.connectors import ParquetSink, ParquetSourceConnector
from sql_as_dag.connectors.registry import (
    get_sink,
    get_source,
    register_sink,
    register_source,
)


def test_builtin_parquet_registered_on_import() -> None:
    assert get_source("parquet") is ParquetSourceConnector
    assert get_sink("parquet") is ParquetSink


def test_get_unknown_raises() -> None:
    with pytest.raises(KeyError, match="unknown source connector"):
        get_source("does_not_exist")
    with pytest.raises(KeyError, match="unknown sink connector"):
        get_sink("does_not_exist")


def test_register_and_retrieve_custom() -> None:
    register_source("dummy_src", ParquetSourceConnector)
    register_sink("dummy_sink", ParquetSink)
    assert get_source("dummy_src") is ParquetSourceConnector
    assert get_sink("dummy_sink") is ParquetSink


def test_duplicate_registration_raises_unless_replace() -> None:
    register_source("dup_src", ParquetSourceConnector)
    with pytest.raises(ValueError, match="already registered"):
        register_source("dup_src", ParquetSourceConnector)
    # replace=True overrides without error.
    register_source("dup_src", ParquetSourceConnector, replace=True)
