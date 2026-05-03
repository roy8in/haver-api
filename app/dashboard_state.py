import getpass
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
STATE_DIR = BASE_DIR / "state"
LATEST_STATUS_PATH = STATE_DIR / "haver_status.json"
EVENTS_PATH = STATE_DIR / "haver_events.jsonl"
LATEST_FAILURE_PATH = STATE_DIR / "haver_latest_failure.json"
FAILURES_PATH = STATE_DIR / "haver_failures.jsonl"
SCHEMA_VERSION = 1


def _json_default(value):
    if isinstance(value, (datetime, Path)):
        return str(value)
    return str(value)


def _env_bool(name, default=False):
    raw_value = os.getenv(name, "").strip().lower()
    if raw_value == "":
        return default
    return raw_value in {"1", "true", "yes", "on"}


def _safe_record_value(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    return value


def _compact_summary(summary):
    return {key: _safe_record_value(value) for key, value in summary.items()}


def _failure_category(error_stage, error_message, login_required=False):
    message = (error_message or "").lower()
    stage = (error_stage or "").lower()

    if login_required or stage == "haver_preflight" or "authentication failed" in message:
        return "login_required"
    if stage == "haver_initialize" and "timed out" in message:
        return "timeout"
    if stage in {"metadata_upload", "series_upload"}:
        return "db_upload_failed"
    if stage == "metadata_fetch" and "no metadata collected" in message:
        return "metadata_empty"
    if stage == "metadata_fetch":
        return "metadata_fetch_failed"
    if stage == "series_fetch":
        return "series_fetch_failed"
    if stage == "processing":
        return "processing_failed"
    if stage == "environment_setup":
        return "environment_setup_failed"
    return "unexpected_exception"


def _build_retry(summary):
    return {
        "enabled": bool(summary.get("haver_init_attempts", 1) > 1),
        "max_attempts": summary.get("haver_init_attempts", 1),
        "attempts_used": summary.get("haver_init_attempts_used", 1),
        "timeout_sec": summary.get("haver_init_timeout_sec", ""),
        "delay_sec": summary.get("haver_init_retry_delay_sec", ""),
    }


def _build_timings(summary):
    timings = summary.get("stage_timings_sec", {})
    return {
        "total_sec": summary.get("duration_sec", ""),
        "stages": timings,
        "slowest_stage": summary.get("slowest_stage", ""),
    }


def _build_db(summary):
    return {
        "stored_metadata_count": summary.get("stored_metadata_count", 0),
        "stored_value_ticker_count": summary.get("stored_value_ticker_count", 0),
        "metadata_table_present": summary.get("metadata_table_present", False),
        "values_table_present": summary.get("values_table_present", False),
    }


def _build_files(run_context):
    return {
        "app_log": str(run_context.get("app_log_path", "")),
        "summary_log": str(run_context.get("summary_log_path", "")),
        "status_json": str(LATEST_STATUS_PATH),
        "events_jsonl": str(EVENTS_PATH),
        "latest_failure_json": str(LATEST_FAILURE_PATH),
        "failure_events_jsonl": str(FAILURES_PATH),
    }


def _build_common_record(source, record_type, status, run_context=None, login_status=None, alert_transports=None, extra=None):
    run_context = run_context or {}
    login_status = login_status or {}
    alert_transports = list(alert_transports or [])
    extra = extra or {}
    record = {
        "schema_version": SCHEMA_VERSION,
        "project": "haver-api",
        "record_type": record_type,
        "source": source,
        "node_role": os.getenv("HAVER_NODE_ROLE", "company"),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "user": getpass.getuser(),
        "status": status,
        "run_id": run_context.get("run_id", extra.get("run_id", "")),
        "start_time": _safe_record_value(run_context.get("run_started_at", extra.get("start_time", ""))),
        "end_time": _safe_record_value(extra.get("end_time", "")),
        "duration_sec": extra.get("duration_sec", ""),
        "error_stage": extra.get("error_stage", ""),
        "error_message": extra.get("error_message", ""),
        "haver": {
            "authenticated": login_status.get("authenticated"),
            "direct_state": _safe_record_value(login_status.get("direct_state")),
            "login_required": login_status.get("login_required"),
            "ready": login_status.get("ready"),
            "note": login_status.get("note", ""),
        },
        "alerts": {
            "transports": alert_transports,
            "enabled": bool(alert_transports),
        },
        "files": _build_files(run_context),
    }
    record.update(extra)
    return record


def build_run_record(summary, run_context, login_status=None, alert_transports=None, publish_enabled=False):
    metrics = {
        "ticker_total": summary.get("ticker_total", 0),
        "metadata_rows": summary.get("metadata_rows", 0),
        "rows_uploaded_metadata": summary.get("rows_uploaded_metadata", 0),
        "ticker_skipped": summary.get("ticker_skipped", 0),
        "ticker_backfill": summary.get("ticker_backfill", 0),
        "ticker_fetched": summary.get("ticker_fetched", 0),
        "chunks_total": summary.get("chunks_total", 0),
        "chunks_failed": summary.get("chunks_failed", 0),
        "rows_uploaded_values": summary.get("rows_uploaded_values", 0),
        "rows_uploaded_di": summary.get("rows_uploaded_di", 0),
    }
    record = _build_common_record(
        "main.py",
        "run",
        summary.get("status", "FAILED"),
        run_context=run_context,
        login_status=login_status,
        alert_transports=alert_transports,
        extra=_compact_summary(summary),
    )
    record["metrics"] = metrics
    record["timings"] = _build_timings(summary)
    record["retry"] = _build_retry(summary)
    record["db"] = _build_db(summary)
    record["failure"] = {
        "category": "" if summary.get("status") == "SUCCESS" else summary.get(
            "failure_category",
            _failure_category(summary.get("error_stage"), summary.get("error_message"), login_status.get("login_required") if login_status else False),
        ),
        "stage": summary.get("error_stage", ""),
        "message": summary.get("error_message", ""),
    }
    record["publish"] = {
        "enabled": publish_enabled,
        "status": summary.get("publish_status", ""),
        "message": summary.get("publish_message", ""),
    }
    record["health"] = "ok" if summary.get("status") == "SUCCESS" and summary.get("chunks_failed", 0) == 0 else "issue"
    return record


def build_preflight_record(run_context, login_status, status, message, alert_transports=None):
    record = _build_common_record(
        "scripts/haver_preflight.py",
        "preflight",
        status,
        run_context=run_context,
        login_status=login_status,
        alert_transports=alert_transports,
        extra={
            "error_message": message,
            "preflight": {
                "login_required": login_status.get("login_required"),
                "ready": login_status.get("ready"),
            },
        },
    )
    record["failure"] = {
        "category": "login_required" if login_status.get("login_required") else ("unknown" if status != "READY" else ""),
        "stage": "haver_preflight" if status != "READY" else "",
        "message": message if status != "READY" else "",
    }
    record["timings"] = {
        "total_sec": "",
        "stages": {},
        "slowest_stage": "",
    }
    record["health"] = "ok" if status == "READY" else "issue"
    return record


def write_status(record):
    STATE_DIR.mkdir(exist_ok=True)
    with LATEST_STATUS_PATH.open("w", encoding="utf-8") as handle:
        json.dump(record, handle, ensure_ascii=False, indent=2, default=_json_default)
        handle.write("\n")

    event = {
        "generated_at": record.get("generated_at"),
        "schema_version": record.get("schema_version", SCHEMA_VERSION),
        "project": record.get("project", "haver-api"),
        "record_type": record.get("record_type", "run"),
        "source": record.get("source", ""),
        "run_id": record.get("run_id", ""),
        "status": record.get("status", ""),
        "health": record.get("health", ""),
        "error_stage": record.get("error_stage", ""),
        "error_message": record.get("error_message", ""),
        "haver": record.get("haver", {}),
        "metrics": record.get("metrics", {}),
        "timings": record.get("timings", {}),
        "failure": record.get("failure", {}),
        "retry": record.get("retry", {}),
        "db": record.get("db", {}),
        "publish": record.get("publish", {}),
    }
    with EVENTS_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False, default=_json_default) + "\n")

    is_failure = record.get("health") == "issue" or record.get("status") not in {"SUCCESS", "READY"}
    if is_failure:
        failure_record = {
            "generated_at": record.get("generated_at"),
            "schema_version": record.get("schema_version", SCHEMA_VERSION),
            "project": record.get("project", "haver-api"),
            "record_type": record.get("record_type", "run"),
            "source": record.get("source", ""),
            "run_id": record.get("run_id", ""),
            "status": record.get("status", ""),
            "health": record.get("health", ""),
            "error_stage": record.get("error_stage", ""),
            "error_message": record.get("error_message", ""),
            "haver": record.get("haver", {}),
            "metrics": record.get("metrics", {}),
            "timings": record.get("timings", {}),
            "failure": record.get("failure", {}),
            "retry": record.get("retry", {}),
            "db": record.get("db", {}),
            "alerts": record.get("alerts", {}),
            "files": record.get("files", {}),
            "publish": record.get("publish", {}),
        }
        with LATEST_FAILURE_PATH.open("w", encoding="utf-8") as handle:
            json.dump(failure_record, handle, ensure_ascii=False, indent=2, default=_json_default)
            handle.write("\n")
        with FAILURES_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(failure_record, ensure_ascii=False, default=_json_default) + "\n")

    return {
        "latest_path": LATEST_STATUS_PATH,
        "events_path": EVENTS_PATH,
        "latest_failure_path": LATEST_FAILURE_PATH,
        "failure_events_path": FAILURES_PATH,
    }


def _git(*args, cwd=None):
    return subprocess.run(
        ["git", *args],
        cwd=cwd or BASE_DIR,
        check=True,
        capture_output=True,
        text=True,
    )


def publish_status(logger, paths=None):
    """Commit and push the dashboard state files when publishing is enabled."""
    if not _env_bool("HAVER_GITHUB_PUBLISH_ENABLED", False):
        return {"enabled": False, "committed": False, "pushed": False, "message": "Publishing disabled."}

    paths = paths or [
        str(LATEST_STATUS_PATH.relative_to(BASE_DIR)),
        str(EVENTS_PATH.relative_to(BASE_DIR)),
        str(LATEST_FAILURE_PATH.relative_to(BASE_DIR)),
        str(FAILURES_PATH.relative_to(BASE_DIR)),
        "docs/haver-status.schema.json",
        "docs/dashboard-data-contract.md",
        "state/README.md",
    ]

    try:
        _git("add", "--", *paths)
        diff_check = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=BASE_DIR,
            check=False,
            capture_output=True,
            text=True,
        )
        if diff_check.returncode == 0:
            return {"enabled": True, "committed": False, "pushed": False, "message": "No dashboard state changes to publish."}

        commit_message = os.getenv("HAVER_GITHUB_COMMIT_MESSAGE", "Update dashboard state").strip()
        _git("commit", "-m", commit_message)

        remote = os.getenv("HAVER_GITHUB_PUSH_REMOTE", "origin").strip() or "origin"
        branch = os.getenv("HAVER_GITHUB_PUSH_BRANCH", "").strip()
        if not branch:
            branch_proc = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=BASE_DIR,
                check=True,
                capture_output=True,
                text=True,
            )
            branch = branch_proc.stdout.strip()

        _git("push", remote, branch)
        return {"enabled": True, "committed": True, "pushed": True, "message": f"Published dashboard state to {remote}/{branch}."}
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        message = stderr or stdout or str(exc)
        if logger is not None:
            from app.run_logging import log_event

            log_event(logger, "warning", "Dashboard state publish failed", error=message)
        return {"enabled": True, "committed": False, "pushed": False, "message": message}
