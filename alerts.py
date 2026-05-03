import ctypes
import os
import smtplib
import sys
from email.message import EmailMessage

from run_logging import log_event


def _env_bool(name, default=False):
    raw_value = os.getenv(name, "").strip().lower()
    if raw_value == "":
        return default
    return raw_value in {"1", "true", "yes", "on"}


def _parse_recipients(raw_value):
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _show_popup(subject, message):
    try:
        ctypes.windll.user32.MessageBoxW(0, message, subject, 0x10 | 0x40)
        return True
    except Exception:
        return False


def _send_email(subject, message, extra_lines=None):
    smtp_host = os.getenv("HAVER_ALERT_SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("HAVER_ALERT_SMTP_PORT", "587"))
    smtp_username = os.getenv("HAVER_ALERT_SMTP_USERNAME", "").strip()
    smtp_password = os.getenv("HAVER_ALERT_SMTP_PASSWORD", "").strip()
    from_addr = os.getenv("HAVER_ALERT_FROM", smtp_username).strip()
    to_addrs = _parse_recipients(os.getenv("HAVER_ALERT_TO", ""))
    use_starttls = _env_bool("HAVER_ALERT_SMTP_STARTTLS", True)

    if not smtp_host or not from_addr or not to_addrs:
        return False

    body_lines = [message]
    if extra_lines:
        body_lines.extend(extra_lines)

    email_message = EmailMessage()
    email_message["Subject"] = subject
    email_message["From"] = from_addr
    email_message["To"] = ", ".join(to_addrs)
    email_message.set_content("\n".join(body_lines))

    with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as client:
        if use_starttls:
            client.starttls()
        if smtp_username and smtp_password:
            client.login(smtp_username, smtp_password)
        client.send_message(email_message)

    return True


def send_alert(logger, subject, message, **context):
    """Send an alert via log, popup, and/or SMTP email."""
    log_event(logger, "error", subject, alert_message=message, **context)

    transports = []
    popup_default = sys.stdout.isatty() or sys.stderr.isatty()
    if _env_bool("HAVER_ALERT_POPUP", popup_default):
        if _show_popup(subject, message):
            transports.append("popup")

    try:
        if _send_email(subject, message, [f"{key}={value}" for key, value in context.items()]):
            transports.append("email")
    except Exception as exc:
        log_event(logger, "warning", "Alert email delivery failed", error=str(exc))

    if not transports:
        log_event(logger, "warning", "Alert delivered via log only", subject=subject)

    return transports
