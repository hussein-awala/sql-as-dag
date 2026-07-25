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

## 0.0.1

Initial incubation release. Demo companion for the Airflow Summit 2026 talk
["A SQL Query is Just a DAG"](https://airflowsummit.org/sessions/2026/a-sql-query-is-just-a-dag/).

No public API stability — the interface is expected to change as the runtime, the Stage IR, and
the SQL compiler evolve in subsequent releases.
