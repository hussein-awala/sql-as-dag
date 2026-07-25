# Changelog

## Unreleased

Correctness fixes found by a review of the initial code drop. Each of these could return a wrong
answer rather than an error, which is the one thing the project sets out not to do.

- **Output columns now match a single-node run exactly.** The final stage is rebuilt in projection
  order, so column order is preserved, an aliased `GROUP BY` key keeps its alias, and the same
  aggregate may appear twice under different aliases. An unaliased aggregate is emitted as a
  quoted identifier; previously it produced invalid SQL that only failed once the final stage ran.
- **A subquery in `WHERE` is now rejected.** It was accepted and evaluated per partition, so
  `WHERE amount > (SELECT AVG(amount) FROM orders)` compared each row against its own partition's
  average and silently returned the wrong rows.
- **An empty source partition no longer aborts the task.** A zero-row input registered an empty
  batch list, which DataFusion answered with a Rust panic instead of an empty result.
- **Co-partitioned stages must agree on shuffle width and key count.** Hand-built graphs could
  declare mismatched widths, which mispairs buckets and drops rows; this is now refused at
  DAG-parse time. An unconsumed shuffling stage reports a clear error instead of a `KeyError`.
- **An empty result set still commits.** `finalize` uses the `none_failed` trigger rule, so a query
  that legitimately matches no rows still writes its `_SUCCESS` marker instead of being skipped
  along with the empty expansion. An upstream failure still prevents the commit.
- **The Iceberg sink's `finalize` is idempotent**, so a retry after an unrecorded commit converges
  instead of failing on the duplicate-file check.
- `read_concat` names the offending file on a schema mismatch.
- Licensing corrected: the project's own copyright and the standard Apache-2.0 header replace the
  ASF contributor headers and Airflow's `NOTICE`.

A second review round found three more silent-wrong-answer paths and one data-loss path:

- **A subquery in the `SELECT` list is now rejected too**, for the same per-partition reason as one
  in `WHERE`: `SELECT (SELECT MAX(amount) FROM orders) AS mx` reported a different "global" maximum
  in every partition. Scalar, `IN`, and `EXISTS` forms are all caught, including a subquery nested
  inside a larger expression. The check is now structural (on the plan's expression variants)
  rather than a substring match, so a string literal containing `<subquery>` is no longer mistaken
  for one.
- **Identifiers are always quoted.** A lowercase name was emitted bare, so a column named after a
  niladic SQL function — `current_timestamp`, `current_date` — was reparsed as a call to that
  function and returned a clock reading instead of the column's values.
- **Sink writes are scoped to the DAG run.** Paths were derived from `partition_id` alone, which
  restarts at 0 each run and whose range shrinks when adaptive bucketing picks a smaller width. A
  second run therefore overwrote the first few Parquet partitions and left the rest, publishing a
  directory holding two runs' rows under a fresh `_SUCCESS`; for Iceberg it rewrote data files an
  earlier snapshot had already committed, silently changing what that snapshot returned. `write`
  and `finalize` now take a `run_key` derived from the run's work directory, so Parquet writes
  `<base>/<run_key>/p<id>/data.parquet` (plus a `_LATEST` pointer to the newest finalized run) and
  Iceberg stages under a per-run prefix. Retries within a run reuse the same paths, so they stay
  idempotent, and the Iceberg duplicate-file check is scoped to the run being finalized so it
  cannot mask a genuinely new file.

## 0.0.1

Initial incubation release. Demo companion for the Airflow Summit 2026 talk
["A SQL Query is Just a DAG"](https://airflowsummit.org/sessions/2026/a-sql-query-is-just-a-dag/).

No public API stability — the interface is expected to change as the runtime, the Stage IR, and
the SQL compiler evolve in subsequent releases.
