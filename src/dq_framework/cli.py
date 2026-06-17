"""
dq_framework.cli
~~~~~~~~~~~~~~~~~
Command-line interface for the DQ Framework.

The CLI is a *control-plane* tool — it connects to a Databricks workspace via the SDK
and submits/queries work there. It does NOT execute Spark locally.

Install and use::

    pip install 'dq_framework[cli]'
    dq-framework --help

Authentication: set ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` env vars, or configure
``~/.databrickscfg`` (any standard SDK auth method works).

Commands:
    setup       Bootstrap all DQ tables and views in a catalog.schema
    validate    Pre-flight config check for a table (no run triggered)
    run         Trigger an immediate Workflows job run
    score       Display latest DQ scores from the audit table
    schedule    Manage Workflows job schedules
    version     Print the dq_framework version
"""

from __future__ import annotations

import json
import sys
import time

try:
    import typer
except ImportError:
    print(
        "The CLI requires 'typer'. Install with: pip install 'dq_framework[cli]'",
        file=sys.stderr,
    )
    sys.exit(1)

app = typer.Typer(
    name="dq-framework",
    help="Databricks Data Quality Framework — control-plane CLI",
    no_args_is_help=True,
    add_completion=False,
)

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _get_ws():
    try:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415
        return WorkspaceClient()
    except ImportError:
        typer.echo("databricks-sdk not installed. Run: pip install databricks-sdk", err=True)
        raise typer.Exit(1)


def _get_spark():
    try:
        from pyspark.sql import SparkSession  # noqa: PLC0415
        return SparkSession.builder.getOrCreate()
    except Exception as e:
        typer.echo(f"SparkSession unavailable: {e}", err=True)
        raise typer.Exit(1)


# ------------------------------------------------------------------
# setup
# ------------------------------------------------------------------

@app.command()
def setup(
    catalog: str  = typer.Argument(..., help="Unity Catalog name (e.g. 'prod_catalog')."),
    schema:  str  = typer.Argument(..., help="Schema name (e.g. 'dq')."),
    example: bool = typer.Option(False, "--example", help="Insert a placeholder config row."),
):
    """Bootstrap all DQ tables and views in a Databricks catalog.schema."""
    from .infra.setup import DQSetup  # noqa: PLC0415
    spark  = _get_spark()
    report = DQSetup(spark, catalog=catalog, schema=schema).bootstrap(
        create_example_config=example
    )
    typer.echo(json.dumps(report, indent=2))
    raise typer.Exit(0 if report["success"] else 1)


# ------------------------------------------------------------------
# validate
# ------------------------------------------------------------------

@app.command()
def validate(
    config_table: str = typer.Argument(..., help="Fully qualified dq_config table."),
    table_name:   str = typer.Argument(..., help="Fully qualified table to validate."),
):
    """Pre-flight config check for a table — no DQ run is triggered."""
    from .core.config import DQConfig  # noqa: PLC0415
    spark  = _get_spark()
    result = DQConfig(spark, config_table).validate_config(table_name)
    typer.echo(json.dumps(result, indent=2))
    raise typer.Exit(0 if result["valid"] else 1)


# ------------------------------------------------------------------
# run
# ------------------------------------------------------------------

@app.command()
def run(
    job_id: str  = typer.Argument(..., help="Databricks Workflows job ID to trigger."),
    wait:   bool = typer.Option(False, "--wait", help="Poll until the run completes."),
    params: str  = typer.Option("{}", "--params", help="JSON string of notebook params."),
):
    """Trigger an immediate run of an existing DQ Workflows job."""
    from .infra.scheduling import DQScheduler  # noqa: PLC0415
    ws        = _get_ws()
    scheduler = DQScheduler(ws)
    run_id    = scheduler.trigger_run(job_id, notebook_params=json.loads(params))
    typer.echo(f"Run started: run_id={run_id}")

    if wait:
        typer.echo("Polling run status ...")
        while True:
            state = ws.jobs.get_run(run_id=int(run_id)).state
            life  = state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN"
            typer.echo(f"  [{life}]")
            if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                result_state = state.result_state.value if state.result_state else "UNKNOWN"
                typer.echo(f"Run finished: {result_state}")
                raise typer.Exit(0 if result_state == "SUCCESS" else 1)
            time.sleep(15)


# ------------------------------------------------------------------
# score
# ------------------------------------------------------------------

@app.command()
def score(
    audit_table: str           = typer.Argument(..., help="Fully qualified dq_audit table."),
    table_name:  str | None    = typer.Option(None, "--table", help="Filter by table name."),
    date:        str | None    = typer.Option(None, "--date",  help="Filter by partition_date (YYYY-MM-DD)."),
):
    """Display the latest DQ scores from the audit table."""
    import pyspark.sql.functions as F  # noqa: PLC0415
    from pyspark.sql.window import Window  # noqa: PLC0415

    spark = _get_spark()
    df    = spark.table(audit_table).filter(F.col("active_flag") == 1)
    if table_name:
        df = df.filter(F.lower(F.col("table_name")) == table_name.lower())
    if date:
        df = df.filter(F.col("partition_date") == F.lit(date))

    rows = (
        df.withColumn("_rn", F.row_number().over(
            Window.partitionBy("config_id", "table_name", "partition_date")
                  .orderBy(F.col("audit_ts").desc())
        ))
        .filter(F.col("_rn") == 1)
        .select("table_name", "partition_date", "dq_score", "table_count", "audit_ts")
        .orderBy("table_name", F.col("partition_date").desc())
        .collect()
    )

    if not rows:
        typer.echo("No audit records found.")
        return

    typer.echo(f"{'table_name':<50} {'date':<12} {'score':>7} {'rows':>10}")
    typer.echo("-" * 84)
    for r in rows:
        typer.echo(
            f"{r['table_name']:<50} {str(r['partition_date']):<12} "
            f"{(r['dq_score'] or 0.0):>7.2f} {(r['table_count'] or 0):>10,}"
        )


# ------------------------------------------------------------------
# schedule sub-commands
# ------------------------------------------------------------------

schedule_app = typer.Typer(help="Manage Databricks Workflows schedules.", no_args_is_help=True)
app.add_typer(schedule_app, name="schedule")


@schedule_app.command("create")
def schedule_create(
    job_name:   str          = typer.Argument(..., help="Display name for the job."),
    notebook:   str          = typer.Argument(..., help="Absolute workspace notebook path."),
    cron:       str          = typer.Argument(..., help="Quartz cron (e.g. '0 0 6 * * ?')."),
    cluster_id: str | None   = typer.Option(None, "--cluster-id", help="Existing cluster ID."),
    timezone:   str          = typer.Option("UTC", "--timezone"),
):
    """Create a Workflows job that runs a notebook on a cron schedule."""
    from .infra.scheduling import DQScheduler  # noqa: PLC0415
    job_id = DQScheduler(_get_ws()).create_job(
        job_name=job_name, notebook_path=notebook,
        cron_expression=cron, existing_cluster_id=cluster_id, timezone_id=timezone,
    )
    typer.echo(f"Job created: job_id={job_id}")


@schedule_app.command("list")
def schedule_list(
    prefix: str = typer.Option("dq_", "--prefix", help="Filter jobs by name prefix."),
):
    """List DQ Workflows jobs."""
    jobs = DQScheduler(_get_ws()).list_jobs(prefix=prefix)
    if not jobs:
        typer.echo("No jobs found.")
        return
    for j in jobs:
        typer.echo(f"  {j['job_id']:>12}  {j['name']}")


@schedule_app.command("pause")
def schedule_pause(
    job_id: str = typer.Argument(..., help="Job ID to pause."),
):
    """Pause the schedule of an existing job."""
    ok = DQScheduler(_get_ws()).pause_job(job_id)
    typer.echo("Paused." if ok else "Failed to pause.")
    raise typer.Exit(0 if ok else 1)


@schedule_app.command("delete")
def schedule_delete(
    job_id:  str  = typer.Argument(..., help="Job ID to delete."),
    confirm: bool = typer.Option(False, "--confirm", help="Required to actually delete."),
):
    """Permanently delete a Workflows job."""
    if not confirm:
        typer.echo("Pass --confirm to delete the job.")
        raise typer.Exit(1)
    ok = DQScheduler(_get_ws()).delete_job(job_id)
    typer.echo("Deleted." if ok else "Failed to delete.")
    raise typer.Exit(0 if ok else 1)


# ------------------------------------------------------------------
# version
# ------------------------------------------------------------------

@app.command()
def version():
    """Print the dq_framework version."""
    from . import __version__  # noqa: PLC0415
    typer.echo(f"dq-framework {__version__}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
