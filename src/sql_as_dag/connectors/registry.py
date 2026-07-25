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
Connector registry mapping short names to connector classes.

Short names (``"parquet"``, ``"iceberg"``, ...) let DAG authors and the IR select
connectors declaratively. The registry stores classes, not instances; the DAG factory
instantiates them with the ``Source.options`` / sink options at build time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sql_as_dag.connectors.base import SinkConnector, SourceConnector

_SOURCE_CONNECTORS: dict[str, type[SourceConnector]] = {}
_SINK_CONNECTORS: dict[str, type[SinkConnector]] = {}


def register_source(name: str, connector: type[SourceConnector], *, replace: bool = False) -> None:
    """Register a source connector class under ``name``. Raises if already registered unless ``replace``."""
    if not replace and name in _SOURCE_CONNECTORS:
        raise ValueError(f"source connector {name!r} is already registered")
    _SOURCE_CONNECTORS[name] = connector


def register_sink(name: str, connector: type[SinkConnector], *, replace: bool = False) -> None:
    """Register a sink connector class under ``name``. Raises if already registered unless ``replace``."""
    if not replace and name in _SINK_CONNECTORS:
        raise ValueError(f"sink connector {name!r} is already registered")
    _SINK_CONNECTORS[name] = connector


def get_source(name: str) -> type[SourceConnector]:
    """Look up a registered source connector class. Raises ``KeyError`` if unknown."""
    try:
        return _SOURCE_CONNECTORS[name]
    except KeyError:
        raise KeyError(
            f"unknown source connector {name!r} (registered: {sorted(_SOURCE_CONNECTORS)})"
        ) from None


def get_sink(name: str) -> type[SinkConnector]:
    """Look up a registered sink connector class. Raises ``KeyError`` if unknown."""
    try:
        return _SINK_CONNECTORS[name]
    except KeyError:
        raise KeyError(f"unknown sink connector {name!r} (registered: {sorted(_SINK_CONNECTORS)})") from None
