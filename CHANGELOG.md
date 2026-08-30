# Changelog

## 0.0.1

First release. Demo companion for the Airflow Summit 2026 talk
["A SQL Query is Just a DAG"](https://airflowsummit.org/sessions/2026/a-sql-query-is-just-a-dag/).

Compiles a SQL query into an Apache Airflow DAG and runs it across mapped tasks, with each
partition executed by Apache DataFusion.

- `compile_sql()` lowers a query to a `StageGraph`; `dag_from_stages()` turns that into a DAG of
  mapped `read -> compute -> write` task groups.
- `SELECT`, `WHERE`, projection and aliases; `GROUP BY` with `SUM`, `COUNT`, `MIN`, `MAX` as
  two-phase aggregation; a single INNER equi-join, with broadcast-versus-shuffle chosen at run
  time from actual row counts.
- Shuffle width is either static or adaptive from row count, byte size, or source partition count.
- Pluggable connectors: Parquet built in, Iceberg via the `iceberg` extra.

Anything that cannot be compiled into a *correct* DAG is rejected at DAG-parse time with a clear
error rather than silently returning a wrong answer. See
[docs/limitations.md](docs/limitations.md) for the boundaries.

Sink writes are scoped to the DAG run rather than sharing one output location — see
[docs/connectors.md](docs/connectors.md#writes-are-scoped-to-the-run).

No public API stability: the runtime, the Stage IR, and the SQL compiler are all expected to
change in subsequent releases.
