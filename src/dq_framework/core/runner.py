"""
dq_framework.runner
~~~~~~~~~~~~~~~~~~~~
:class:`DQRunner` — orchestrates the end-to-end DQ pipeline for one or more tables.

All concerns from the original notebook version are resolved:
  - ``spark`` is injected via ``__init__``, never assumed as a global.
  - ``dbutils`` is removed entirely (notebook concern only).
  - ``contextlib.redirect_stdout`` is replaced with the thread-safe
    :func:`dq_framework._explain.suppress_explain` monkeypatch.
  - ``databricks-labs-dqx`` is version-pinned in ``pyproject.toml``.

Fatal pipeline failures raise typed :mod:`dq_framework.exceptions`. In single-table
mode these propagate to the caller; in multi-table mode they are captured per table
in :class:`dq_framework.results.DQBatchResult`.
"""

from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from ._explain import suppress_explain
from .config import DQConfig
from ..exceptions import (
    ConfigNotFoundError,
    DQDependencyError,
    MetricComputationError,
    RuleApplicationError,
    RuleGenerationError,
)
from .metrics import compute_metric_stats
from .results import DQBatchResult, summarize_batch
from .scoring import compute_dq_score

logger = logging.getLogger(__name__)


class DQRunner(DQConfig):
    """
    Orchestrates the end-to-end DQ pipeline for one or more tables.

    Single table   → :meth:`run_dq` runs synchronously, returns DataFrame | JSON.
    Multiple tables → :meth:`run_dq` fans out via ``ThreadPoolExecutor``, returns
    ``List[DQBatchResult]``.

    Args:
        spark               : Active SparkSession (required — never uses a global).
        config_table        : Fully qualified Delta config table name.
        table_name          : Single table name (str) or list of table names.
        catalog_schema      : Default catalog.schema for unqualified audit table names.
        collect_table_stats : Whether to collect row counts (can be slow on large tables).
        max_workers         : Max parallel threads for multi-table mode. Capped at 8.
        max_retries         : Retry attempts for transient step failures. Default 1.
        quarantine          : Optional :class:`dq_framework.quarantine.DQQuarantine` instance.
                              When set, error rows are routed to a quarantine table after
                              each run.

    Raises:
        DQDependencyError: if ``databricks-labs-dqx`` / ``databricks-sdk`` are not installed.
        ValueError: if ``table_name`` is not a non-empty string or list.
    """

    _MAX_WORKERS_CAP = 8

    def __init__(
        self,
        spark: SparkSession,
        config_table: str,
        table_name: str | list[str],
        catalog_schema: str | None = None,
        collect_table_stats: bool = True,
        max_workers: int | None = None,
        max_retries: int = 1,
        quarantine=None,
    ):
        # Lazy import so the package can be imported without dqx installed
        # (useful for unit-testing config/alert logic in isolation).
        try:
            from databricks.labs.dqx.config import InputConfig
            from databricks.labs.dqx.engine import DQEngine
            from databricks.labs.dqx.profiler.generator import DQGenerator
            from databricks.sdk import WorkspaceClient
        except ImportError as e:
            raise DQDependencyError(
                "databricks-labs-dqx and databricks-sdk are required for DQRunner. "
                "Install with: pip install 'databricks-labs-dqx[llm]'"
            ) from e

        if isinstance(table_name, str):
            self.table_names = [table_name]
        elif isinstance(table_name, list) and table_name:
            self.table_names = table_name
        else:
            raise ValueError("table_name must be a non-empty string or list of strings.")

        # Convenience default for single-table helpers (get_valid_invalid_df). In
        # multi-table mode callers should pass an explicit table_name to those helpers.
        self.table_name          = self.table_names[0]
        self.catalog_schema      = catalog_schema
        self.collect_table_stats = collect_table_stats
        self.max_retries         = max(1, max_retries)
        self.max_workers         = min(
            max_workers if max_workers is not None else len(self.table_names),
            self._MAX_WORKERS_CAP,
        )
        self.quarantine         = quarantine
        self._audit_write_lock  = threading.Lock()
        self._config_write_lock = threading.Lock()

        super().__init__(spark, config_table)

        # Single WorkspaceClient shared across generator and engine.
        ws = WorkspaceClient()
        self.generator    = DQGenerator(ws, spark)
        self.dq_engine    = DQEngine(ws)
        self._InputConfig = InputConfig
        logger.info(
            f"DQRunner initialised | tables={self.table_names} "
            f"| max_workers={self.max_workers} | max_retries={self.max_retries}."
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _with_retry(self, fn, label: str, *args, **kwargs):
        """Calls fn(*args) up to max_retries times. Returns result or None."""
        for attempt in range(1, self.max_retries + 1):
            try:
                result = fn(*args, **kwargs)
                if result is not None:
                    return result
                logger.warning(f"[Retry {attempt}/{self.max_retries}] '{label}' returned None.")
            except Exception as e:
                logger.warning(f"[Retry {attempt}/{self.max_retries}] '{label}' raised {type(e).__name__}: {e}")
            if attempt == self.max_retries:
                logger.error(f"All {self.max_retries} attempt(s) failed for '{label}'.")
        return None

    # ------------------------------------------------------------------
    # Public pipeline steps
    # ------------------------------------------------------------------

    def get_table_stats(self, table_name: str) -> tuple:
        """Returns (row_count | None, column_list). Soft — never raises."""
        logger.info(f"Collecting table stats for table='{table_name}'.")
        try:
            df_table      = self.spark.table(table_name)
            total_columns = df_table.columns
            total_count   = df_table.count() if self.collect_table_stats else None
            return total_count, total_columns
        except Exception as e:
            logger.exception(f"Failed to collect table stats for table='{table_name}': {e}")
            return None, []

    def generate_dq_rule(self, table_name: str, business_rules: str) -> dict | None:
        """Generates DQ rules via AI-assisted generator. Returns rule dict or None."""
        logger.info(f"Generating DQ rules for table='{table_name}'.")
        try:
            rules = self.generator.generate_dq_rules_ai_assisted(
                user_input=business_rules,
                input_config=self._InputConfig(location=table_name),
            )
            return rules
        except Exception as e:
            logger.exception(f"Failed to generate DQ rules for table='{table_name}': {e}")
            return None

    def apply_dq_rules(self, table_name: str, dq_rule: dict, dq_flag: bool = True):
        """
        Applies DQ rules to a table. Returns the filtered status DataFrame
        (``dq_flag=True``) or the full valid+invalid DataFrame (``dq_flag=False``);
        ``None`` on failure.

        DQX calls ``DataFrame.explain()`` internally, printing a verbose plan to
        stdout. We suppress it via the thread-safe :func:`suppress_explain` context
        manager — no global ``sys.stdout`` mutation.
        """
        logger.info(f"Applying DQ rules | table='{table_name}' | status_only={dq_flag}.")
        try:
            input_df = self.spark.read.table(table_name)
            with suppress_explain():
                valid_and_invalid_df = self.dq_engine.apply_checks_by_metadata(input_df, dq_rule)
        except Exception as e:
            logger.exception(f"Failed to apply DQ rules for table='{table_name}': {e}")
            return None

        if not dq_flag:
            return valid_and_invalid_df

        status_cols = [c for c in valid_and_invalid_df.columns if c.lower() in ("_errors", "_warnings")]
        if not status_cols:
            # DQX should always emit _errors/_warnings; an empty list here would make
            # select(*[]) raise an opaque AnalysisException. Surface it as a clear failure.
            logger.error(f"DQX produced no _errors/_warnings columns for table='{table_name}'.")
            return None
        return valid_and_invalid_df.select(*status_cols)

    def get_metric_stats(self, df_dq_output, json_output: bool = False, start_ts: str | None = None):
        """
        Aggregates DQ error metrics. Returns a JSON string or a Spark DataFrame.

        Thin wrapper around :func:`dq_framework.metrics.compute_metric_stats`; raises
        :class:`MetricComputationError` on failure.
        """
        return compute_metric_stats(df_dq_output, json_output=json_output, start_ts=start_ts)

    def get_valid_invalid_df(self, table_name: str | None = None):
        """
        Returns the full DataFrame with valid and invalid rows after DQ rule application.

        Raises:
            ConfigNotFoundError: if no config / dq_rule_payload exists for the table.
        """
        table_name = table_name or self.table_name
        raw_config = self.get_config_from_delta(table_name)
        if not raw_config or not raw_config.get("dq_rule_payload"):
            raise ConfigNotFoundError(f"Missing or invalid dq_rule_payload for table='{table_name}'.")
        dq_dict = (
            json.loads(raw_config["dq_rule_payload"])
            if isinstance(raw_config["dq_rule_payload"], str)
            else raw_config["dq_rule_payload"]
        )
        return self.apply_dq_rules(table_name, dq_dict, dq_flag=False)

    def audit_dq_as_delta(self, df_dq, audit_table_name: str = "dq_audit", write_mode: str = "append") -> bool:
        """Persists DQ metric results to a Delta audit table partitioned by date. Returns success."""
        logger.info(f"Writing audit results | table='{audit_table_name}' | mode='{write_mode}'.")
        try:
            df_dq_final = (
                df_dq
                .withColumn("audit_ts",        F.current_timestamp())
                .withColumn("partition_date",  F.current_date())
                .withColumn("active_flag",     F.lit(1))
                .withColumn("audit_update_ts", F.lit(None).cast("timestamp"))
            )
            name_parts = audit_table_name.split(".")
            if len(name_parts) == 3:
                pass
            elif len(name_parts) == 2:
                logger.error(f"Ambiguous audit table name '{audit_table_name}': use catalog.schema.table or table only.")
                return False
            else:
                if not self.catalog_schema:
                    logger.error(f"catalog_schema not set and audit_table_name='{audit_table_name}' is unqualified.")
                    return False
                audit_table_name = f"{self.catalog_schema}.{audit_table_name}"

            df_dq_final.write.partitionBy("partition_date").mode(write_mode).saveAsTable(audit_table_name)
            logger.info(f"Audit write successful | table='{audit_table_name}'.")
            return True
        except Exception as e:
            logger.exception(f"Failed to write audit results to '{audit_table_name}': {e}")
            return False

    def get_batch_summary(self, results: list[DQBatchResult]) -> dict:
        """Structured summary from a completed batch run."""
        return summarize_batch(results)

    # ------------------------------------------------------------------
    # Private pipeline
    # ------------------------------------------------------------------

    def _run_dq_single(
        self,
        table_name:       str,
        json_output:      bool = False,
        debug_flag:       bool = False,
        force_regenerate: bool = False,
    ):
        """
        Core DQ pipeline for one table. Returns a metric DataFrame / JSON string.

        Raises one of :class:`ConfigNotFoundError`, :class:`RuleGenerationError`,
        :class:`RuleApplicationError`, :class:`MetricComputationError` on fatal failure.
        """
        logger.info(f"DQ run started | table='{table_name}' | debug_flag={debug_flag}.")
        print(f"\n{'='*60}\n[DQRunner] DQ run | table='{table_name}' | debug={debug_flag}\n{'='*60}")

        v_config = self.get_config_from_delta(table_name)
        if not v_config:
            raise ConfigNotFoundError(f"No valid config for table='{table_name}'.")
        v_config_id = v_config["config_id"]

        payload_missing  = not v_config.get("dq_rule_payload")
        should_regen     = force_regenerate or v_config["change_flag"] or payload_missing
        if should_regen:
            reason = (
                "force_regenerate=True" if force_regenerate
                else ("change_flag=True" if v_config["change_flag"] else "payload missing")
            )
            logger.info(f"Regenerating DQ rules ({reason}) for table='{table_name}'.")
            new_payload = self._with_retry(
                self.generate_dq_rule, f"generate_dq_rule:{table_name}",
                table_name, v_config["business_rules"],
            )
            if not new_payload:
                raise RuleGenerationError(f"Rule generation failed for table='{table_name}'.")
            with self._config_write_lock:
                update_ok = self.update_dq_rule_payload(v_config_id, json.dumps(new_payload))
                if not update_ok:
                    logger.warning(
                        f"Config update failed for table='{table_name}'. Proceeding with in-memory payload."
                    )
            v_config["dq_rule_payload"] = new_payload
        else:
            logger.info(f"Using existing payload for table='{table_name}'.")
            print("[DQRunner] Using existing DQ rule payload.")

        v_start_ts = str(datetime.now())

        # When quarantine is enabled we need the full DF; otherwise status columns only.
        dq_flag       = self.quarantine is None
        df_dq_applied = self._with_retry(
            self.apply_dq_rules, f"apply_dq_rules:{table_name}",
            table_name, v_config["dq_rule_payload"], dq_flag,
        )
        if df_dq_applied is None:
            raise RuleApplicationError(f"Rule application failed for table='{table_name}'.")

        # Quarantine error rows; df_dq_applied becomes the clean subset.
        if self.quarantine is not None:
            _, _ = self.quarantine.route(df_dq_applied, table_name)
            # Extract status columns for metrics
            status_cols   = [c for c in df_dq_applied.columns if c.lower() in ("_errors", "_warnings")]
            df_dq_applied = df_dq_applied.select(*status_cols) if status_cols else df_dq_applied

        metric_stats = self.get_metric_stats(df_dq_applied, json_output, v_start_ts)
        if metric_stats is None:
            raise MetricComputationError(f"Metric computation produced no result for table='{table_name}'.")

        if json_output:
            return metric_stats

        table_count, table_columns = self.get_table_stats(table_name)

        # Composite DQ score (0-100)
        dq_score = compute_dq_score(df_dq_applied, table_count)

        df_metric_stats = (
            metric_stats
            .withColumn(
                "total_time",
                (F.unix_timestamp("final_end_timestamp") - F.unix_timestamp(F.lit(v_start_ts).cast("timestamp"))) / 60,
            )
            .select(
                F.lit(v_config_id).cast("bigint").alias("config_id"),
                F.lit(table_name).alias("table_name"),
                F.lit(table_count).cast("bigint").alias("table_count"),
                F.lit(",".join(table_columns)).alias("table_cols"),
                "final_payload",
                F.lit(v_start_ts).cast("timestamp").alias("start_ts"),
                F.col("final_end_timestamp").alias("end_ts"),
                "total_time",
                F.lit(dq_score).alias("dq_score"),
            )
        )
        print(f"[DQRunner] DQ score for '{table_name}': {dq_score:.2f}/100")

        if not debug_flag:
            with self._audit_write_lock:
                if not self.audit_dq_as_delta(df_metric_stats):
                    logger.warning(f"Audit write failed for table='{table_name}' (run continues).")
        else:
            logger.info(f"Audit write skipped (debug_flag=True) for table='{table_name}'.")
            print("[DQRunner] Audit write skipped (debug_flag=True).")

        logger.info(f"DQ run complete for table='{table_name}'.")
        return df_metric_stats

    def _run_dq_single_threaded(
        self,
        table_name:       str,
        json_output:      bool,
        debug_flag:       bool,
        force_regenerate: bool,
    ) -> DQBatchResult:
        """Thread-worker wrapper around :meth:`_run_dq_single` — never raises."""
        thread_name = threading.current_thread().name
        start_time  = datetime.now()
        print(f"[DQRunner | {thread_name}] → '{table_name}' ...")
        try:
            result     = self._run_dq_single(table_name, json_output, debug_flag, force_regenerate)
            duration_s = (datetime.now() - start_time).total_seconds()
            print(f"[DQRunner | {thread_name}] ✓ '{table_name}' in {duration_s:.2f}s.")
            return DQBatchResult(
                table_name=table_name, success=True,
                result=result, duration_s=duration_s, thread_name=thread_name,
            )
        except Exception as e:
            duration_s = (datetime.now() - start_time).total_seconds()
            logger.exception(f"[{thread_name}] FAILED | table='{table_name}'.")
            return DQBatchResult(
                table_name=table_name, success=False,
                error=e, duration_s=duration_s, thread_name=thread_name,
            )

    def run_dq(
        self,
        json_output:      bool = False,
        debug_flag:       bool = False,
        force_regenerate: bool = False,
    ):
        """
        Runs the DQ pipeline for all configured tables. Always returns ``List[DQBatchResult]``
        regardless of table count — failures are captured per table, never raised.

        Args:
            json_output      : Unused (kept for compatibility).
            debug_flag       : Skip the audit write — useful for testing.
            force_regenerate : Re-generate DQ rules even if a valid payload already exists.
        """
        self.reload_config()

        total = len(self.table_names)
        print(f"\n{'='*60}\n[DQRunner] Batch — {total} table(s), {self.max_workers} thread(s)\n{'='*60}")

        results:     list[DQBatchResult] = []
        batch_start = datetime.now()

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="dq_worker") as executor:
            future_to_table = {
                executor.submit(
                    self._run_dq_single_threaded, t, json_output, debug_flag, force_regenerate
                ): t
                for t in self.table_names
            }
            for future in as_completed(future_to_table):
                results.append(future.result())

        batch_duration_s = (datetime.now() - batch_start).total_seconds()
        summary          = summarize_batch(results)

        print(f"\n{'='*60}")
        print(f"[DQRunner] Batch complete in {batch_duration_s:.2f}s")
        print(f"  ✓ {summary['succeeded']}/{summary['total']} succeeded")
        print(f"  ✗ {summary['failed']}/{summary['total']} failed")
        for r in results:
            if not r.success:
                print(f"    - '{r.table_name}': {type(r.error).__name__}: {r.error}")
        print(f"{'='*60}\n")

        return results


__all__ = ["DQRunner"]
