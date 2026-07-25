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


def get_provider_info():
    """Provider metadata, discovered by Airflow via the ``apache_airflow_provider`` entry point."""
    return {
        "package-name": "sql-as-dag",
        "name": "SQL-as-DAG",
        "description": (
            "Compiles a SQL query into an Airflow DAG: each relational operator becomes a task, "
            "fan-out / fan-in is expressed via dynamic task mapping, and shuffle between stages "
            "happens through ``ObjectStoragePath``. Per-partition execution is delegated to "
            "`Apache DataFusion <https://datafusion.apache.org/>`__.\n"
        ),
        "versions": ["0.0.1"],
        "integrations": [
            {
                "integration-name": "SQL-as-DAG",
                "external-doc-url": "https://airflowsummit.org/sessions/2026/a-sql-query-is-just-a-dag/",
                "tags": ["software"],
            }
        ],
    }
