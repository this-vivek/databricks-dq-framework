"""
dq_framework.notifications
~~~~~~~~~~~~~~~~~~~~~~~~~~~
:class:`DQNotifier` — sends DQ alert summaries to Slack, Microsoft Teams, or email.

Install the optional dependency group to use this module::

    pip install 'dq_framework[notifications]'

which pulls in ``requests``.  SMTP email uses stdlib ``smtplib`` with no extra deps.

Example::

    notifier = DQNotifier(
        slack_webhook="https://hooks.slack.com/services/...",
        email_config={
            "smtp_host": "smtp.gmail.com",
            "sender":    "dq@org.com",
            "password":  "...",
            "recipients": ["team@org.com"],
        },
    )
    notifier.send("DQ Alert", "Quality dropped below SLA", severity="CRITICAL")
"""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

_SEVERITY_COLORS = {
    "INFO":     "#36a64f",
    "WARNING":  "#ffaa00",
    "CRITICAL": "#cc0000",
}


class DQNotifier:
    """
    Dispatches DQ alert notifications to one or more channels.

    Configure any subset of channels — unconfigured channels are silently skipped.

    Args:
        slack_webhook : Slack incoming webhook URL.
        teams_webhook : Teams Power Automate / Logic Apps HTTP trigger URL.
        email_config  : dict with keys:
                        ``smtp_host``, ``smtp_port`` (default 587),
                        ``sender``, ``password``, ``recipients`` (list[str]),
                        ``use_tls`` (default True).
    """

    def __init__(
        self,
        slack_webhook: str | None = None,
        teams_webhook: str | None = None,
        email_config:  dict | None = None,
    ):
        self.slack_webhook = slack_webhook
        self.teams_webhook = teams_webhook
        self.email_config  = email_config or {}
        n_channels = sum([bool(slack_webhook), bool(teams_webhook), bool(email_config)])
        logger.info(f"DQNotifier initialised | {n_channels} channel(s) configured.")

    # ------------------------------------------------------------------
    # Channel senders
    # ------------------------------------------------------------------

    def _send_slack(self, title: str, body: str, severity: str) -> bool:
        try:
            import requests  # noqa: PLC0415
        except ImportError:
            logger.error("'requests' not installed. Run: pip install 'dq_framework[notifications]'")
            return False
        color   = _SEVERITY_COLORS.get(severity, "#888888")
        payload = {
            "attachments": [{
                "color":     color,
                "title":     f"[{severity}] {title}",
                "text":      body,
                "mrkdwn_in": ["text"],
            }]
        }
        try:
            resp = requests.post(self.slack_webhook, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("Slack notification sent.")
            return True
        except Exception as e:
            logger.exception(f"Slack notification failed: {e}")
            return False

    def _send_teams(self, title: str, body: str, severity: str) -> bool:
        try:
            import requests  # noqa: PLC0415
        except ImportError:
            logger.error("'requests' not installed. Run: pip install 'dq_framework[notifications]'")
            return False
        payload = {
            "@type":    "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": _SEVERITY_COLORS.get(severity, "#888888").lstrip("#"),
            "summary":    title,
            "sections":   [{"activityTitle": f"**[{severity}] {title}**", "activityText": body}],
        }
        try:
            resp = requests.post(self.teams_webhook, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("Teams notification sent.")
            return True
        except Exception as e:
            logger.exception(f"Teams notification failed: {e}")
            return False

    def _send_email(self, title: str, body: str, severity: str) -> bool:
        cfg  = self.email_config
        host = cfg.get("smtp_host")
        if not host or not cfg.get("recipients"):
            logger.warning("Email config incomplete — skipping email notification.")
            return False
        try:
            msg             = MIMEMultipart("alternative")
            msg["Subject"]  = f"[DQ {severity}] {title}"
            msg["From"]     = cfg["sender"]
            msg["To"]       = ", ".join(cfg["recipients"])
            msg.attach(MIMEText(body, "plain"))
            port    = cfg.get("smtp_port", 587)
            use_tls = cfg.get("use_tls", True)
            with smtplib.SMTP(host, port) as server:
                if use_tls:
                    server.starttls()
                if cfg.get("password"):
                    server.login(cfg["sender"], cfg["password"])
                server.sendmail(cfg["sender"], cfg["recipients"], msg.as_string())
            logger.info(f"Email notification sent to {cfg['recipients']}.")
            return True
        except Exception as e:
            logger.exception(f"Email notification failed: {e}")
            return False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send(self, title: str, body: str, severity: str = "INFO") -> dict[str, bool]:
        """
        Sends a notification to all configured channels.

        Args:
            title    : Short notification title.
            body     : Notification body (markdown supported in Slack/Teams).
            severity : ``"INFO"``, ``"WARNING"``, or ``"CRITICAL"``.

        Returns:
            ``{channel: success}`` for each configured channel.
        """
        results: dict[str, bool] = {}
        if self.slack_webhook:
            results["slack"] = self._send_slack(title, body, severity)
        if self.teams_webhook:
            results["teams"] = self._send_teams(title, body, severity)
        if self.email_config:
            results["email"] = self._send_email(title, body, severity)
        if not results:
            logger.warning("DQNotifier.send() called but no channels are configured.")
        return results

    def send_alert_summary(
        self,
        alert_results: dict,
        table_name:    str = "",
    ) -> dict[str, bool]:
        """
        Formats and sends a summary from :meth:`DQAlertSystem.save_alerts` output.

        Args:
            alert_results : Return value of ``save_alerts()`` —
                            ``{"metric_drift": bool, "count_drift": bool, "column_drift": bool}``.
            table_name    : Optional table name for context in the notification.

        Returns:
            ``{channel: success}``
        """
        scope = f" for `{table_name}`" if table_name else ""
        lines = [f"DQ alert run complete{scope}.", ""]
        any_ok = False
        for check, ok in alert_results.items():
            lines.append(f"{'✓' if ok else '✗'} {check.replace('_', ' ').title()}: {'written' if ok else 'FAILED'}")
            if ok:
                any_ok = True
        return self.send(
            title    = f"DQ Alerts{scope}",
            body     = "\n".join(lines),
            severity = "WARNING" if not any_ok else "INFO",
        )

    def send_sla_breach(
        self,
        table_name:     str,
        dq_score:       float,
        threshold:      float,
        breach_type:    str = "QUALITY_BELOW_THRESHOLD",
        sla_owner:      str = "",
    ) -> dict[str, bool]:
        """
        Sends a targeted SLA breach notification.

        Args:
            table_name  : Table that breached its SLA.
            dq_score    : Actual DQ score observed.
            threshold   : The SLA threshold that was breached.
            breach_type : ``"QUALITY_BELOW_THRESHOLD"`` or ``"NULL_RATE_EXCEEDED"``.
            sla_owner   : Handle/email stamped in the message body.
        """
        owner_line = f"\nOwner: {sla_owner}" if sla_owner else ""
        body = (
            f"Table: `{table_name}`\n"
            f"Breach: {breach_type}\n"
            f"Score: {dq_score:.2f}  (threshold: {threshold:.2f}){owner_line}"
        )
        return self.send(title=f"SLA Breach — {table_name}", body=body, severity="CRITICAL")


__all__ = ["DQNotifier"]
