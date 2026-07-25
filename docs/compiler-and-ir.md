# The compiler and the Stage IR

## Why an intermediate representation

The compiler could emit Airflow tasks directly. It does not, and the reason is worth stating: a
`StageGraph` is plain data, so it can be constructed by hand, asserted on in a test, printed, and
built without importing Airflow or DataFusion. The compiler's entire output is inspectable before
anything runs, and the DAG factory can be tested against hand-written graphs that no SQL query
produces.

It also means you are not obliged to write SQL at all. Building a `StageGraph` directly is a
supported entry point — useful when you want a shape the compiler cannot yet produce.

## The IR

Five dataclasses in `sql_as_dag.ir`:

- **`Source`** — an external table: an id, the SQL table name, a connector name, and an options
  dict. Plain data, so it stays JSON-serializable.
- **`StageInput`** — one input to a stage, referencing *either* a source *or* an upstream stage
  (exactly one; the constructor enforces it), registered under a SQL `table_name`.
- **`Exchange`** — how a stage's output is partitioned for whatever consumes it: `pipeline` (1:1, no
  re-partitioning), `hash_shuffle` (N:M by a hash of `keys`), or `broadcast` (1:N).
- **`Stage`** — a `stage_id`, the SQL to run *once per partition*, its inputs, and its
  `output_exchange`.
- **`StageGraph`** — the sources, the topologically-ordered stages, and which stage feeds the sink.

`StageGraph.validate()` is the single source of truth for well-formedness: unique ids, every
reference resolves, the sink exists, and stages appear in topological order. It runs at
construction and again in the DAG factory.

The invariant that makes everything else work: **a stage's SQL is written to run against one
partition of its inputs.** Fan-out is not in the SQL; it is in the exchange.

## SQL to StageGraph

DataFusion does the parsing and planning. The compiler registers each source's schema in an empty
`SessionContext` — one explicitly-empty Arrow batch per table, enough for planning and validation
without reading data — then pattern-matches the resulting logical plan.

Three shapes are recognized.

### Passthrough: `Projection -> [Filter] -> TableScan`

No aggregate. One stage runs the user's SQL verbatim against the source and pipelines straight to
the sink. Filters are pushed nowhere in particular because there is nowhere to push them: one stage,
one pass.

```sql
SELECT customer_id, amount FROM orders WHERE amount > 100
```

### Aggregate: `Projection -> Aggregate -> [Filter] -> TableScan`

This is the interesting one, and it is the classic two-phase aggregation every distributed engine
performs. `SUM`, `COUNT` (including `COUNT(*)`), `MIN`, and `MAX` are *combinable*: you can compute
them per partition and then combine the partial results, and the answer is identical to computing
them over the whole table at once.

So the compiler emits two stages:

```sql
-- partial: runs per source partition, hash-shuffled by the group keys
SELECT customer_id, sum(amount) AS __p_0 FROM orders GROUP BY customer_id

-- final: runs per bucket, combining partials
SELECT customer_id, sum(__p_0) AS total FROM q_partials GROUP BY customer_id
```

Note the combine function is not always the same as the partial function: a `COUNT` is combined with
a `SUM`, because summing per-partition counts gives the total count. That mapping is the whole trick,
and it is the reason `AVG` is rejected — an average of averages is wrong, so it would need to be
split into a `SUM` and a `COUNT` and recombined. Likewise `DISTINCT` aggregates are rejected:
`COUNT(DISTINCT x)` cannot be combined from per-partition counts without seeing all the values.

The `__p_<i>` aliases are internal plumbing between the two stages. The user's requested output
names are recovered from the projection and applied in the final stage, so `SUM(amount) AS total`
comes out as `total`.

When a `WHERE` accompanies a `GROUP BY`, the filtered-scan subtree is unparsed back into SQL and
folded into the partial stage's `FROM` clause, so filtering happens as early as possible — before
the shuffle, on the map side.

### Join: `Projection -> Join`

A single INNER equi-join. Each side becomes its own scan stage, hash-shuffled by its join key into
the same number of buckets. Because equal keys hash to the same bucket, matching rows from both
sides land in the same bucket — so the join stage can read one bucket from each side, run the
original user SQL over just those two fragments, and the union of the per-bucket results is the
correct full join.

Join keys are read from the **optimized** plan, not the raw one. In the unoptimized plan the
equality still lives in a filter above the join; the optimizer is what turns it into `Join.on`.

One subtlety worth knowing about, because it protects correctness: DataFusion inserts a `CAST` into
a join key when the two sides' types differ, even for something as innocuous as `int32` vs `int64`.
Hashing each side independently on differently-typed values would send equal-but-differently-typed
keys to different buckets and silently drop matches. Rather than return a wrong answer, the compiler
detects the `CAST` and refuses, asking you to make the types match explicitly in the query.

## What gets rejected

The compiler raises `UnsupportedSQLError` rather than guessing. See
[limitations.md](limitations.md) for the full list. The design principle: a query that cannot be
compiled into a correct DAG must fail at parse time with a clear message, never produce a DAG that
computes the wrong number.

## Version coupling

All DataFusion logical-plan introspection lives in one module,
`sql_as_dag/compiler/sql_compiler.py`, and all DataFusion *execution* lives in one other,
`sql_as_dag/runtime/executor.py`. This is deliberate containment: plan shapes and attribute access
patterns vary between DataFusion releases, so the dependency is pinned to a tested floor and the
coupling is confined to two files.
