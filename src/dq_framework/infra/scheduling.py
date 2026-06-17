"""
dq_framework.scheduling
~~~~~~~~~~~~~~~~~~~~~~~~
:class:`DQScheduler` — creates and manages Databricks Workflows jobs for recurring DQ runs.

Requires ``databricks-sdk`` (already a transitive dependency of ``databricks-labs-dqx``).

Timezone note: Databricks cron uses Quartz format with a leading seconds field::

    "0 0 6 * * ?"   →  6:00 AM UTC daily
    "0 30 8 ? * MON-FRI"  →  8:30 AM UTC, Monday–Friday

Example::

    from databricks.sdk import WorkspaceClient
    scheduler = DQScheduler(WorkspaceClient())
    job_id = scheduler.create_job(
        job_name            = "dq_daily_orders",
        notebook_path       = "/Shared/dq/run_dq",
        cron_expression     = "0 0 6 * * ?",
        existing_cluster_id = "0123-456789-abcde",
    )
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class DQScheduler:
    """
    Manages Databricks Workflows jobs for automated DQ runs.

    Args:
        workspace_client : Authenticated ``databricks.sdk.WorkspaceClient``.
                           On Databricks clusters, ``WorkspaceClient()`` authenticates
                           automatically with no parameters.
    """

    def __init__(self, workspace_client):
        self.ws = workspace_client

    def create_job(
        self,
        job_name:            str,
        notebook_path:       str,
        cron_expression:     str,
        existing_cluster_id: str | None = None,
        spark_version:       str = "15.4.x-scala2.12",
        node_type_id:        str = "Standard_DS3_v2",
        notebook_params:     dict | None = None,
        timezone_id:         str = "UTC",
        max_retries:         int = 1,
    ) -> str:
        """
        Creates a Workflows job that runs a notebook on a cron schedule.

        Args:
            job_name            : Display name for the job.
            notebook_path       : Absolute workspace path to the driver notebook.
            cron_expression     : Quartz cron expression (e.g. ``"0 0 6 * * ?"``).
            existing_cluster_id : Run on this interactive cluster. If omitted, a new
                                  job cluster is created with ``spark_version`` and
                                  ``node_type_id``.
            spark_version       : DBR version for the new cluster (ignored when
                                  ``existing_cluster_id`` is set).
            node_type_id        : VM type for the new cluster driver node.
            notebook_params     : Key-value pairs passed to the notebook as widgets.
            timezone_id         : Timezone for the schedule (default ``"UTC"``).
            max_retries         : Retry attempts on task failure.

        Returns:
            Job ID string.
        """
        try:
            from databricks.sdk.service.jobs import (  # noqa: PLC0415
                ClusterSpec, CronSchedule, JobCluster, NotebookTask, Task,
            )
        except ImportError as e:
            raise ImportError(
                "databricks-sdk is required. Install with: pip install databricks-sdk"
            ) from e

        task_kwargs: dict = {
            "task_key":    "dq_run",
            "notebook_task": NotebookTask(
                notebook_path=notebook_path,
                base_parameters=notebook_params or {},
            ),
            "max_retries": max_retries,
        }

        if existing_cluster_id:
            task_kwargs["existing_cluster_id"] = existing_cluster_id
            clusters: list = []
        else:
            task_kwargs["job_cluster_key"] = "dq_cluster"
            clusters = [JobCluster(
                job_cluster_key="dq_cluster",
                new_cluster=ClusterSpec(
                    spark_version=spark_version,
                    node_type_id=node_type_id,
                    num_workers=1,
                ),
            )]

        job = self.ws.jobs.create(
            name=job_name,
            tasks=[Task(**task_kwargs)],
            job_clusters=clusters or None,
            schedule=CronSchedule(
                quartz_cron_expression=cron_expression,
                timezone_id=timezone_id,
                pause_status="UNPAUSED",
            ),
        )
        job_id = str(job.job_id)
        logger.info(f"Created Workflows job '{job_name}' (id={job_id}).")
        print(f"[DQScheduler] Job '{job_name}' created — id={job_id}.")
        return job_id

    def update_schedule(
        self,
        job_id:          str,
        cron_expression: str,
        timezone_id:     str = "UTC",
    ) -> bool:
        """Updates the cron schedule for an existing job."""
        try:
            from databricks.sdk.service.jobs import CronSchedule  # noqa: PLC0415
            self.ws.jobs.update(
                job_id=int(job_id),
                new_settings={"schedule": CronSchedule(
                    quartz_cron_expression=cron_expression,
                    timezone_id=timezone_id,
                    pause_status="UNPAUSED",
                )},
            )
            logger.info(f"Job {job_id} schedule updated to '{cron_expression}'.")
            return True
        except Exception as e:
            logger.exception(f"Failed to update schedule for job {job_id}: {e}")
            return False

    def trigger_run(self, job_id: str, notebook_params: dict | None = None) -> str:
        """
        Triggers an immediate (unscheduled) run of the job.

        Returns:
            Run ID string.
        """
        run = self.ws.jobs.run_now(
            job_id=int(job_id),
            notebook_params=notebook_params or {},
        )
        run_id = str(run.run_id)
        logger.info(f"Triggered job {job_id} → run_id={run_id}.")
        return run_id

    def pause_job(self, job_id: str) -> bool:
        """Pauses the schedule of a job without deleting it."""
        try:
            from databricks.sdk.service.jobs import CronSchedule  # noqa: PLC0415
            existing = self.ws.jobs.get(job_id=int(job_id)).settings.schedule
            self.ws.jobs.update(
                job_id=int(job_id),
                new_settings={"schedule": CronSchedule(
                    quartz_cron_expression=existing.quartz_cron_expression,
                    timezone_id=existing.timezone_id,
                    pause_status="PAUSED",
                )},
            )
            logger.info(f"Job {job_id} paused.")
            return True
        except Exception as e:
            logger.exception(f"Failed to pause job {job_id}: {e}")
            return False

    def list_jobs(self, prefix: str = "dq_") -> list[dict]:
        """Lists all jobs whose name starts with ``prefix``."""
        try:
            return [
                {"job_id": str(j.job_id), "name": j.settings.name}
                for j in self.ws.jobs.list()
                if j.settings and j.settings.name and j.settings.name.startswith(prefix)
            ]
        except Exception as e:
            logger.exception(f"Failed to list jobs: {e}")
            return []

    def delete_job(self, job_id: str) -> bool:
        """Permanently deletes a job."""
        try:
            self.ws.jobs.delete(job_id=int(job_id))
            logger.info(f"Job {job_id} deleted.")
            return True
        except Exception as e:
            logger.exception(f"Failed to delete job {job_id}: {e}")
            return False


__all__ = ["DQScheduler"]
