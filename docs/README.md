# sql-as-dag design docs

How a SQL query becomes an Airflow DAG, and why the pieces are shaped the way they are.

Read in this order:

| Document | What it covers |
| --- | --- |
| [architecture.md](architecture.md) | The components, the layers, and the end-to-end path from SQL text to written output |
| [compiler-and-ir.md](compiler-and-ir.md) | SQL to a DataFusion logical plan to the Stage IR |
| [dag-mapping.md](dag-mapping.md) | How stages become task groups, and how dynamic task mapping expresses partitioning |
| [shuffle.md](shuffle.md) | The exchange between stages: bucketing, Parquet files, adaptive width, broadcast joins |
| [connectors.md](connectors.md) | The source/sink contract and how to add a table format |
| [limitations.md](limitations.md) | The supported SQL subset and the boundaries you will hit |
| [development.md](development.md) | Running the tests, and testing the provider inside Airflow Breeze |

## The one-paragraph version

A SQL query is a directed acyclic graph of relational operators, and so is an Airflow DAG. This
project takes that literally: it parses SQL with [Apache DataFusion](https://datafusion.apache.org/),
lowers the resulting logical plan into a small intermediate representation of *stages*, and turns
each stage into a dynamically-mapped Airflow task group that runs one instance per data partition.
Data moves between stages as Parquet files through Airflow's `ObjectStoragePath`, partitioned by a
hash of the shuffle keys — the same map/reduce exchange a distributed query engine performs
internally, except every step is a task you can see, retry, and inspect in the Airflow UI.

The goal is legibility, not throughput. If you want a fast distributed SQL engine, use one. If you
want to *see* what a distributed SQL engine does, run a query here and watch the grid.
