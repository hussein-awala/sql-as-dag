# Limitations

This is a teaching engine. The guiding rule is that anything that cannot be compiled into a
*correct* DAG is rejected at DAG-parse time with a clear message — the engine never guesses and never
silently returns a wrong number.

## Supported SQL

- `SELECT` with projection and `WHERE`.
- `GROUP BY` with `SUM`, `COUNT` (including `COUNT(*)`), `MIN`, `MAX`.
- `WHERE` together with `GROUP BY` (the filter is folded into the map-side stage).
- Column aliases, which are preserved through to the output.
- A single INNER equi-join between two tables on one key pair of matching types.

## Rejected, and why

Each of these raises `UnsupportedSQLError`:

| Construct | Reason |
| --- | --- |
| `AVG` | An average of per-partition averages is wrong. Needs splitting into `SUM` and `COUNT` with a recombination step. |
| `COUNT(DISTINCT x)` and other `DISTINCT` aggregates | Not combinable from partial results without seeing every value. |
| Aggregate with no `GROUP BY` (global aggregate) | No key to shuffle on; needs a single-reducer plan shape. |
| Aggregate over an expression, e.g. `SUM(a * b)` | Only plain column arguments are extracted. |
| `GROUP BY` on an expression rather than a plain column | Same reason. |
| Non-INNER joins (`LEFT`, `RIGHT`, `FULL`) | Need null-padding logic on the reduce side. |
| Multi-key joins | Only a single equi-key pair is handled. |
| Self-joins | Both sides would resolve to the same table name. |
| Join keys of different types | DataFusion inserts a `CAST`; hashing each side independently on differently-typed values would route equal keys to different buckets and silently drop matches. Cast both keys explicitly in the query instead. |
| `HAVING`, sub-queries, window functions, `ORDER BY` as a global sort, set operations | Not implemented. |
| More than one source in a non-join query | Not a supported plan shape. |

## Structural limits of the runtime

Enforced with `NotImplementedError` by the DAG factory:

- **No multi-level shuffles.** A shuffling stage must read from a source, so shuffle-to-shuffle
  chains are refused. This caps plans at map, shuffle, reduce.
- **No pipeline chaining between stages.** Every non-sink stage must shuffle its output.
- **One source per stage.** Each source is shuffled through its own scan stage.
- **No mixing** of source inputs and upstream inputs in one stage.

The IR can express more than the factory currently builds; these are runtime limits, not IR limits.

## Airflow and scale considerations

- **Dynamic task mapping has a ceiling.** Airflow caps the number of mapped instances per task via
  `core.max_map_length` (1024 by default). Both source partitions and shuffle buckets are mapped
  instances, so a very wide source or a large `num_buckets` will hit it. An adaptive
  `BucketingPolicy` cap is the natural place to stay under it.
- **Per-task overhead is real.** Each partition is a task group of three tasks, each with scheduling
  latency and a database round trip. Below a certain data size the overhead dominates entirely — this
  engine is not competing with a real distributed engine on throughput, and is not trying to.
- **Local temp work directories do not work across workers.** The default work directory is a local
  temp dir, which is fine on one machine. Set `work_dir_base` to an object-store URI as soon as tasks
  can land on different workers.
- **Every partition's data is materialized.** `read`, `compute`, and `write` each go through Parquet
  files, and a partition's inputs are concatenated in memory. A partition must fit in its worker's
  memory.

## Dependency coupling

The compiler introspects DataFusion's logical-plan shapes and relies on DataFusion inserting a `CAST`
for mismatched join keys. Both behaviours vary across releases, so `datafusion` is pinned to a tested
floor. Bump it only after running the test suite.
