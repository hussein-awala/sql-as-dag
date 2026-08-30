# Development

## Environment and checks

```bash
uv sync --group dev          # create the environment
uv run pytest                # unit tests
uv run ruff check .          # lint
```

The tests are deliberately fast and Airflow-free wherever possible: the compiler is tested by
asserting on the emitted `StageGraph`, the executor and shuffle by passing Arrow tables in and
checking the tables that come out. Only the DAG factory tests need Airflow imports.

Note that `pytest` runs with `--import-mode=importlib`, so the test directory does not shadow the
installed `sql_as_dag` package.

## Running the example DAGs

The DAGs in [`../examples/dags`](../examples/dags) are self-contained: each one materializes its own
small demo Parquet input under `/tmp` before running, so you can drop them into any Airflow 3 DAGs
folder and trigger them.

```bash
airflow dags test sql_as_dag_passthrough
airflow dags test sql_as_dag_compiled_groupby      # the two-stage shuffle; the core demo
airflow dags test sql_as_dag_join
airflow dags test sql_as_dag_broadcast_join
airflow dags test sql_as_dag_adaptive_groupby
airflow dags test sql_as_dag_iceberg_groupby       # also needs: pip install pyiceberg
```

### Enabling the wide live-demo DAGs

The wider, stage-oriented examples in [`../examples/dags/demo`](../examples/dags/demo) are disabled
by default. Unlike the regular examples, they are intended for a live presentation and create more
source partitions so that mapped fan-out is visible in the Grid view. Keeping them disabled prevents
the Dag processor from repeatedly materializing their input and output files under `/tmp` during
normal development.

Set `SQL_AS_DAG_ENABLE_DEMOS` before starting Airflow or running one directly:

```bash
export SQL_AS_DAG_ENABLE_DEMOS=true

airflow dags test sql_as_dag_passthrough_demo
airflow dags test sql_as_dag_groupby_demo
airflow dags test sql_as_dag_adaptive_groupby_demo
```

Unset the variable and restart the Dag processor to disable them again:

```bash
unset SQL_AS_DAG_ENABLE_DEMOS
```

The switch is read while each file is parsed, so changing it in a shell does not affect an already
running Dag processor. With the switch disabled, the files create no Dag objects and do not call
their input-materialization helpers.

### Testing in Airflow standalone

Airflow's standalone mode starts the API server, scheduler, Dag processor, and triggerer together,
which is useful for checking the examples in the Grid view without setting up Breeze. From the
repository root, use a temporary Airflow home and point Dag discovery at the examples:

```bash
export AIRFLOW_HOME=/tmp/af_demo
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/examples/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False

uv run airflow standalone
```

Open <http://localhost:8080>. Standalone prints the generated admin credentials during startup and
also writes them to `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`. Stop all
standalone components with `Ctrl+C`.

To include the wide live-demo Dags, export the demo switch in the same shell before starting
Airflow:

```bash
export SQL_AS_DAG_ENABLE_DEMOS=true
uv run airflow standalone
```

Environment variables are inherited when the standalone components start. Restart standalone after
changing the Dag folder or the demo switch. A second terminal running Airflow CLI commands must
export the same `AIRFLOW_HOME`, Dag folder, and demo-switch values so that it uses the same
configuration and metadata database.

> **macOS users:** if Dag-processing subprocesses are killed while importing the examples, disable
> Objective-C fork-safety checks and bypass proxies for local component communication before
> starting standalone:
>
> ```bash
> export no_proxy='*' OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
> ```

## Testing the provider in Airflow Breeze

[Breeze](https://github.com/apache/airflow/blob/main/dev/breeze/doc/README.rst) is the Airflow
project's development environment. It is the most realistic way to check that this package is
correctly registered and indexed as a third-party provider.

Breeze installs provider wheels from its `dist/` folder with `--use-distributions-from-dist`. There
is one wrinkle: its in-container installer only picks up wheels whose filename starts with
`apache_airflow_providers_`, so a `sql_as_dag-*.whl` is silently ignored.

A built wheel cannot simply be renamed to work around this. The distribution name is baked into the
wheel's internal metadata — the `.dist-info/` directory name, `METADATA`'s `Name:` field, and the
`RECORD` checksums — and pip and uv both reject a mismatch:

```
The .dist-info directory sql_as_dag-0.0.1 does not start with the normalized package name:
apache-airflow-providers-sql-as-dag
```

So the wheel has to be *built* under a Breeze-compatible distribution name. `scripts/build-breeze-wheel.sh`
does that: it temporarily overrides the distribution name, builds, and always restores the source
files afterwards, even if the build fails. The importable module stays `sql_as_dag`; only the
distribution name changes.

```bash
# builds apache_airflow_providers_sql_as_dag-*.whl and stages it in Breeze's dist/
scripts/build-breeze-wheel.sh /path/to/airflow/dist
```

Then start Breeze and let it install from `dist/`. `--providers-skip-constraints` is required,
because a third-party provider is not covered by Airflow's constraints files:

```bash
breeze start-airflow \
  --use-distributions-from-dist --distribution-format wheel --providers-skip-constraints
```

Confirm the provider was discovered, from inside the container:

```bash
airflow providers list | grep -i sql-as-dag
python -c "from airflow.providers_manager import ProvidersManager as P; \
  print([k for k in P().providers if 'sql-as-dag' in k])"
```

Provider discovery is entry-point based, so the provider manager finds the package through its
`apache_airflow_provider` entry point regardless of the distribution name it was installed under.

> **Colima users:** if `DOCKER_HOST` points at a Colima socket, Breeze's docker-in-docker socket
> mount fails with `operation not supported`, because Colima cannot bind-mount a socket file. Run
> Breeze as `env -u DOCKER_HOST DOCKER_CONTEXT=colima breeze ...` so it falls back to mounting the
> in-VM `/var/run/docker.sock`.
