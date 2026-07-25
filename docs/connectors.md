# Connectors

Connectors are the seam that keeps the compiler, the DAG factory, and the runtime format-agnostic.
Only a connector knows how to enumerate, read, or write a particular table format; adding a format
means writing one pair of classes and registering them, with no changes anywhere else in the engine.

## The contracts

Both are `Protocol`s in `sql_as_dag.connectors.base`, so there is no base class to inherit from —
structural typing is enough.

### `SourceConnector`

| Method | Called | Purpose |
| --- | --- | --- |
| `schema()` | DAG-parse time | Return the Arrow schema. Metadata only; must not read data. This is what lets the compiler plan and validate the SQL without touching the table. |
| `list_partitions()` | Run time, in a planner task | Enumerate readable partitions. Each `PartitionRef` becomes one mapped task-group instance. For Parquet, one entry per file. |
| `read_partition(ref)` | Run time, per partition | Read one partition into an Arrow table. |
| `estimate_total_rows()` | Run time, width planner | Cheap row estimate, or `None`. Metadata only. |
| `estimate_total_bytes()` | Run time, width planner | Cheap size estimate in bytes, or `None`. |

### `SinkConnector`

| Method | Purpose |
| --- | --- |
| `write(table, *, partition_id)` | Write one result partition; return JSON-serializable metadata. |
| `finalize(partition_metas)` | Commit after all partitions are written. |

The `finalize` split exists for transactional formats. Parquet has nothing to do — the files are
already there — but Iceberg needs to append the written data files and commit a single snapshot, so
that either the whole query result becomes visible or none of it does. Putting the commit in its own
task also makes it visible and separately retryable.

## Two hard constraints

**A `PartitionRef` must survive XCom.** It is produced by a planner task and consumed by a
per-partition task in a different process, so it is a plain `dict`. Its internal shape is entirely up
to the connector — `{"uri": "..."}` for Parquet, something else elsewhere — and the planner treats it
as opaque.

**Connectors must be reconstructible from `(name, options)`.** Tasks never receive a live connector
object; they receive the registered name and an options dict and rebuild it via the registry. This
is what keeps each task independently serializable and runnable, and it is why `Source` and `Sink`
hold plain data rather than instances.

**Connectors know nothing about Airflow.** They take and return Arrow tables and plain dicts, which
means they are unit-testable with no DAG, no scheduler, and no database.

## The registry

Importing `sql_as_dag.connectors` registers the built-ins:

| Name | Source | Sink | Requires |
| --- | --- | --- | --- |
| `parquet` | `ParquetSourceConnector` | `ParquetSink` | built in |
| `iceberg` | `IcebergSourceConnector` | `IcebergSink` | `pip install "sql-as-dag[iceberg]"` |

The Iceberg classes import `pyiceberg` lazily, so registration does not require the optional extra to
be installed — you only need it if you actually use an Iceberg source or sink.

## Adding a format

```python
import pyarrow as pa
from sql_as_dag.connectors import register_source


class MyFormatSource:
    def __init__(self, **options):
        self._options = options

    def schema(self) -> pa.Schema:
        ...  # metadata only

    def list_partitions(self) -> list[dict]:
        return [{"chunk": i} for i in range(self._num_chunks())]

    def read_partition(self, ref: dict) -> pa.Table:
        return self._read_chunk(ref["chunk"])

    def estimate_total_rows(self) -> int | None:
        return None

    def estimate_total_bytes(self) -> int | None:
        return None


register_source("myformat", MyFormatSource)
```

Then reference it by name:

```python
Source(source_id="s", table_name="events", connector="myformat", options={...})
```

How you choose to partition is the interesting design decision, because `list_partitions()` directly
determines the map-side parallelism: one ref, one task. Files, row groups, or key ranges are all
reasonable; just keep the refs small, since they travel through XCom.
