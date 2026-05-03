import os
import sys
import threading
import time
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

from alerts import send_alert
import dashboard_state
import data_processor as processor
import db_handler as db
import haver_provider as haver
from run_logging import append_summary, log_event, setup_run_logging


BASE_DIR = Path(__file__).resolve().parent


def _standardize_mod(val):
    """Normalize Haver metadata timestamps before comparing."""
    if val is None:
        return ""

    s = str(val).replace("T", " ").strip()
    if s.lower() in {"", "none", "nan", "nat"}:
        return ""

    if len(s) > 10 and s.startswith("2") and s[4:5] != "-":
        idx = s.find("20")
        if idx != -1:
            s = s[idx:]

    if "." in s:
        s = s.split(".", 1)[0]

    return s


def _parse_metadata_date(value, default_value):
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return pd.Timestamp(default_value)
    return parsed


def _call_with_timeout(func, timeout_seconds, label):
    """Run a callable in a daemon thread and return (result, timed_out)."""
    outcome = {"result": None, "error": None}

    def runner():
        try:
            outcome["result"] = func()
        except Exception as exc:  # pragma: no cover - defensive wrapper
            outcome["error"] = exc

    thread = threading.Thread(target=runner, name=label, daemon=True)
    thread.start()
    thread.join(timeout_seconds)

    if thread.is_alive():
        return None, True, None
    if outcome["error"] is not None:
        return None, False, outcome["error"]
    return outcome["result"], False, None


def _get_int_env(name, default, minimum=1):
    raw_value = os.getenv(name, "")
    if raw_value == "":
        return default

    try:
        parsed = int(raw_value)
    except ValueError:
        return default

    return max(minimum, parsed)


def _get_bool_env(name, default=False):
    raw_value = os.getenv(name, "").strip().lower()
    if raw_value == "":
        return default
    return raw_value in {"1", "true", "yes", "on"}


def _record_stage_timing(summary, stage_name, started_at):
    elapsed = round(time.perf_counter() - started_at, 3)
    summary.setdefault("stage_timings_sec", {})[stage_name] = elapsed
    return time.perf_counter()


def _classify_failure(error_stage, error_message, login_required=False):
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


def _metadata_failure_details(login_status):
    """Return a more specific metadata failure message when login is not ready."""
    if login_status and login_status.get("login_required"):
        return "Haver login is required before metadata can be collected.", "haver_login"
    return "No metadata collected.", "metadata_fetch"


def _initialize_haver_with_retry(logger, timeout_seconds, max_attempts, retry_delay_seconds):
    last_error_message = ""

    for attempt in range(1, max_attempts + 1):
        log_event(
            logger,
            "info",
            "Starting Haver initialization attempt",
            attempt=attempt,
            max_attempts=max_attempts,
            timeout_sec=timeout_seconds,
            retry_delay_sec=retry_delay_seconds if attempt < max_attempts else 0,
        )
        haver_initialized, timed_out, init_error = _call_with_timeout(
            haver.initialize,
            timeout_seconds,
            f"haver_initialize_attempt_{attempt}",
        )

        if timed_out:
            last_error_message = f"Haver initialization timed out after {timeout_seconds} seconds on attempt {attempt}."
        elif init_error is not None:
            last_error_message = str(init_error)
        elif not haver_initialized:
            last_error_message = "Haver provider initialization returned False."
        else:
            return True, attempt, ""

        log_event(
            logger,
            "warning",
            "Haver initialization attempt failed",
            attempt=attempt,
            max_attempts=max_attempts,
            error_message=last_error_message,
        )

        if attempt < max_attempts:
            time.sleep(retry_delay_seconds)

    return False, max_attempts, last_error_message


def _alert_haver_login_issue(logger, message, **context):
    return send_alert(logger, "Haver login required", message, **context)


def _build_sync_tasks(meta_df, db_metadata, db_max_dates):
    end_col = next((c for c in ["enddate", "end", "finish", "last"] if c in meta_df.columns), None)
    start_col = next((c for c in ["startdate", "start", "begin"] if c in meta_df.columns), None)

    sync_tasks = []
    skipped_up_to_date = 0
    kept_for_backfill = 0

    for _, row in meta_df.iterrows():
        pk = row["ticker_pk"]
        new_mod = _standardize_mod(row.get("datetimemod", ""))
        old_mod = _standardize_mod(db_metadata.get(pk, ""))

        m_start = _parse_metadata_date(row[start_col], "1900-01-01") if start_col else pd.Timestamp("1900-01-01")
        m_end = _parse_metadata_date(row[end_col], datetime.now()) if end_col else pd.Timestamp(datetime.now())

        db_last = None
        if pk in db_max_dates:
            parsed_db_last = pd.to_datetime(db_max_dates.get(pk), errors="coerce")
            if not pd.isna(parsed_db_last):
                db_last = parsed_db_last

        if db_last is None:
            fetch_start = m_start
        else:
            fetch_start = db_last - timedelta(days=180)
            if db_last >= m_end and old_mod == new_mod:
                skipped_up_to_date += 1
                continue
            if old_mod == new_mod and db_last < m_end:
                kept_for_backfill += 1

        sync_tasks.append(
            {
                "pk": pk,
                "freq": row.get("frequency", row.get("freq", "ALL")),
                "start": fetch_start,
            }
        )

    return sync_tasks, skipped_up_to_date, kept_for_backfill


def run_sync():
    run_context = setup_run_logging()
    logger = run_context["logger"]
    init_timeout = int(os.getenv("HAVER_INIT_TIMEOUT_SECONDS", "30"))
    init_attempts = _get_int_env("HAVER_INIT_MAX_ATTEMPTS", 2)
    init_retry_delay = _get_int_env("HAVER_INIT_RETRY_DELAY_SECONDS", 5, minimum=0)
    require_auth_ready = _get_bool_env("HAVER_REQUIRE_AUTH_READY", False)
    login_status = None
    alert_transports = []
    publish_enabled = _get_bool_env("HAVER_GITHUB_PUBLISH_ENABLED", False)
    publish_result = {"enabled": publish_enabled, "committed": False, "pushed": False, "message": "Publishing disabled."}
    summary = {
        "run_id": run_context["run_id"],
        "start_time": run_context["run_started_at"].isoformat(timespec="seconds"),
        "status": "FAILED",
        "ticker_total": 0,
        "metadata_rows": 0,
        "rows_uploaded_metadata": 0,
        "ticker_skipped": 0,
        "ticker_backfill": 0,
        "ticker_fetched": 0,
        "chunks_total": 0,
        "chunks_failed": 0,
        "rows_uploaded_values": 0,
        "rows_uploaded_di": 0,
        "error_stage": "",
        "error_message": "",
        "failure_category": "",
        "stage_timings_sec": {},
        "slowest_stage": "",
        "haver_init_attempts": init_attempts,
        "haver_init_attempts_used": 0,
        "haver_init_timeout_sec": init_timeout,
        "haver_init_retry_delay_sec": init_retry_delay,
        "stored_metadata_count": 0,
        "stored_value_ticker_count": 0,
        "metadata_table_present": False,
        "values_table_present": False,
        "publish_status": "",
        "publish_message": "",
    }
    run_started_perf = time.perf_counter()

    log_event(
        logger,
        "info",
        "Starting sync run",
        run_id=summary["run_id"],
        app_log_path=run_context["app_log_path"],
        summary_log_path=run_context["summary_log_path"],
        cwd=os.getcwd(),
        script_dir=BASE_DIR,
        python_executable=sys.executable,
        haver_init_timeout_sec=init_timeout,
        haver_init_attempts=init_attempts,
        haver_init_retry_delay_sec=init_retry_delay,
        haver_require_auth_ready=require_auth_ready,
    )

    try:
        summary["error_stage"] = "environment_setup"
        stage_started = time.perf_counter()
        db.setup_environment()
        stage_started = _record_stage_timing(summary, "environment_setup", stage_started)

        stage_started = time.perf_counter()
        login_status = haver.log_login_status(level="warning" if not require_auth_ready else "info")
        stage_started = _record_stage_timing(summary, "login_preflight", stage_started)
        if login_status["login_required"]:
            message = "Haver session is not authenticated. A login prompt may appear during initialization."
            if require_auth_ready:
                summary["error_stage"] = "haver_preflight"
                summary["error_message"] = message
                summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], True)
                alert_transports = _alert_haver_login_issue(
                    logger,
                    "Haver login is required before scheduled execution.",
                    run_id=summary["run_id"],
                    direct_state=login_status["direct_state"],
                    authenticated=login_status["authenticated"],
                    note=login_status["note"],
                )
                return False
            log_event(
                logger,
                "warning",
                message,
                direct_state=login_status["direct_state"],
                authenticated=login_status["authenticated"],
                note=login_status["note"],
            )

        stage_started = time.perf_counter()
        haver_initialized, init_attempt, init_error_message = _initialize_haver_with_retry(
            logger,
            init_timeout,
            init_attempts,
            init_retry_delay,
        )
        summary["haver_init_attempts_used"] = init_attempt
        stage_started = _record_stage_timing(summary, "haver_initialize", stage_started)
        if not haver_initialized:
            summary["error_stage"] = "haver_initialize"
            summary["error_message"] = init_error_message or "Haver initialization failed."
            summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], login_status.get("login_required") if login_status else False)
            if login_status["login_required"] or "Authentication failed" in summary["error_message"]:
                alert_transports = _alert_haver_login_issue(
                    logger,
                    "Haver login appears to be required and initialization did not complete.",
                    run_id=summary["run_id"],
                    error_message=summary["error_message"],
                    direct_state=login_status["direct_state"],
                    authenticated=login_status["authenticated"],
                    note=login_status["note"],
                )
            log_event(
                logger,
                "error",
                "Haver initialization failed after retries",
                attempts=init_attempt,
                error_message=summary["error_message"],
            )
            return False
        log_event(logger, "info", "Haver initialization complete", attempts=init_attempt)

        summary["error_stage"] = "ticker_load"
        stage_started = time.perf_counter()
        tickers_csv = pd.read_csv(BASE_DIR / "tickers.csv")
        ticker_list = tickers_csv["ticker"].tolist()
        summary["ticker_total"] = len(ticker_list)
        stage_started = _record_stage_timing(summary, "ticker_load", stage_started)
        log_event(logger, "info", "Loaded tickers.csv", ticker_total=summary["ticker_total"])

        summary["error_stage"] = "db_state_load"
        stage_started = time.perf_counter()
        db_metadata = db.get_stored_metadata()
        db_max_dates = db.get_ticker_max_dates()
        summary["stored_metadata_count"] = len(db_metadata)
        summary["stored_value_ticker_count"] = len(db_max_dates)
        summary["metadata_table_present"] = bool(db_metadata)
        summary["values_table_present"] = bool(db_max_dates)
        stage_started = _record_stage_timing(summary, "db_state_load", stage_started)

        summary["error_stage"] = "metadata_fetch"
        stage_started = time.perf_counter()
        meta_df = haver.fetch_metadata(ticker_list)
        stage_started = _record_stage_timing(summary, "metadata_fetch", stage_started)
        if meta_df.empty:
            summary["error_message"], summary["error_stage"] = _metadata_failure_details(login_status)
            summary["failure_category"] = _classify_failure(
                summary["error_stage"],
                summary["error_message"],
                login_status.get("login_required") if login_status else False,
            )
            if summary["failure_category"] == "login_required":
                alert_transports = _alert_haver_login_issue(
                    logger,
                    summary["error_message"],
                    run_id=summary["run_id"],
                    direct_state=login_status["direct_state"] if login_status else None,
                    authenticated=login_status["authenticated"] if login_status else None,
                    note=login_status["note"] if login_status else "",
                )
            log_event(logger, "warning", summary["error_message"])
            return False

        meta_df.columns = [c.lower() for c in meta_df.columns]
        summary["metadata_rows"] = len(meta_df)
        stage_started = time.perf_counter()
        db.create_table_with_types(meta_df, "haver_metadata")
        summary["rows_uploaded_metadata"] = db.upsert_data(meta_df, "haver_metadata")
        stage_started = _record_stage_timing(summary, "metadata_upload", stage_started)
        if summary["rows_uploaded_metadata"] == 0:
            summary["error_stage"] = "metadata_upload"
            summary["error_message"] = "Metadata upload failed; DB API accepted 0 rows."
            summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], False)
            log_event(logger, "error", summary["error_message"])
            return False
        log_event(
            logger,
            "info",
            "Metadata sync complete",
            metadata_rows=summary["metadata_rows"],
            rows_uploaded_metadata=summary["rows_uploaded_metadata"],
        )

        summary["error_stage"] = "task_build"
        stage_started = time.perf_counter()
        sync_tasks, skipped_up_to_date, kept_for_backfill = _build_sync_tasks(meta_df, db_metadata, db_max_dates)
        stage_started = _record_stage_timing(summary, "task_build", stage_started)
        summary["ticker_skipped"] = skipped_up_to_date
        summary["ticker_backfill"] = kept_for_backfill
        summary["ticker_fetched"] = len(sync_tasks)
        log_event(
            logger,
            "info",
            "Built sync tasks",
            ticker_skipped=skipped_up_to_date,
            ticker_backfill=kept_for_backfill,
            ticker_fetched=len(sync_tasks),
        )

        task_df = pd.DataFrame(sync_tasks)
        if task_df.empty:
            summary["status"] = "SUCCESS"
            summary["error_stage"] = ""
            log_event(logger, "info", "Everything is up-to-date. No data to fetch.")
            return True

        summary["error_stage"] = "series_fetch"
        stage_started = time.perf_counter()
        task_df = task_df.sort_values("start")

        for freq, group in task_df.groupby("freq"):
            tickers_in_freq = group.to_dict("records")
            total_count = len(tickers_in_freq)
            log_event(logger, "info", "Processing frequency group", frequency=freq, ticker_count=total_count)

            chunk_size = 50
            for i in range(0, total_count, chunk_size):
                chunk_tasks = tickers_in_freq[i:i + chunk_size]
                chunk_tickers = [t["pk"] for t in chunk_tasks]
                min_start = min(t["start"] for t in chunk_tasks).strftime("%Y-%m-%d")
                summary["chunks_total"] += 1

                log_event(
                    logger,
                    "info",
                    "Fetching chunk",
                    frequency=freq,
                    chunk_index=i // chunk_size + 1,
                    chunk_size=len(chunk_tickers),
                    min_start=min_start,
                )
                long_df = haver.fetch_series_data(chunk_tickers, min_start)

                if long_df.empty:
                    summary["chunks_failed"] += 1
                    log_event(
                        logger,
                        "warning",
                        "No data fetched for chunk",
                        frequency=freq,
                        chunk_index=i // chunk_size + 1,
                    )
                    continue

                db.create_table_with_types(long_df, "haver_values")
                uploaded = db.upsert_data(long_df, "haver_values")
                summary["rows_uploaded_values"] += uploaded
                if uploaded == 0:
                    summary["chunks_failed"] += 1
                    log_event(
                        logger,
                        "error",
                        "Chunk upload failed",
                        frequency=freq,
                        chunk_index=i // chunk_size + 1,
                    )
                else:
                    log_event(
                        logger,
                        "info",
                        "Chunk upload complete",
                        frequency=freq,
                        chunk_index=i // chunk_size + 1,
                        rows_uploaded=uploaded,
                    )
        stage_started = _record_stage_timing(summary, "series_fetch", stage_started)

        summary["error_stage"] = "processing"
        stage_started = time.perf_counter()
        processing_stats = processor.run_processing()
        stage_started = _record_stage_timing(summary, "processing", stage_started)
        summary["rows_uploaded_di"] = processing_stats.get("rows_uploaded_di", 0)
        log_event(logger, "info", "Derived processing complete", rows_uploaded_di=summary["rows_uploaded_di"])

        if summary["chunks_failed"]:
            summary["error_stage"] = "series_upload"
            summary["error_message"] = f"{summary['chunks_failed']} chunk(s) failed to fetch or upload."
            summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], False)
            log_event(logger, "error", summary["error_message"])
            return False

        summary["status"] = "SUCCESS"
        summary["error_stage"] = ""
        return True
    except Exception as exc:
        summary["error_message"] = str(exc)
        summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], login_status.get("login_required") if login_status else False)
        log_event(
            logger,
            "exception",
            "Sync run failed with unhandled exception",
            error_stage=summary["error_stage"] or "unknown",
            error=str(exc),
        )
        return False
    finally:
        finished_at = datetime.now()
        summary["end_time"] = finished_at.isoformat(timespec="seconds")
        summary["duration_sec"] = round((finished_at - run_context["run_started_at"]).total_seconds(), 2)
        summary["stage_timings_sec"]["total"] = round(time.perf_counter() - run_started_perf, 3)
        if summary["stage_timings_sec"]:
            slowest_stage = max(summary["stage_timings_sec"], key=summary["stage_timings_sec"].get)
            summary["slowest_stage"] = f"{slowest_stage}:{summary['stage_timings_sec'][slowest_stage]}"
        if not summary["failure_category"] and summary["status"] != "SUCCESS":
            summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], login_status.get("login_required") if login_status else False)
        summary["publish_status"] = "enabled" if publish_enabled else "disabled"
        append_summary(run_context["summary_log_path"], summary)
        status_record = dashboard_state.build_run_record(
            summary,
            run_context,
            login_status=login_status,
            alert_transports=alert_transports,
            publish_enabled=publish_enabled,
        )
        try:
            dashboard_state.write_status(status_record)
            publish_result = dashboard_state.publish_status(logger)
            summary["publish_status"] = "pushed" if publish_result.get("pushed") else ("enabled" if publish_result.get("enabled") else "disabled")
            summary["publish_message"] = publish_result.get("message", "")
            if publish_result.get("enabled") and not publish_result.get("pushed"):
                log_event(
                    logger,
                    "warning",
                    "Dashboard state publish not completed",
                    publish_message=publish_result.get("message", ""),
                )
        except Exception as exc:
            log_event(logger, "warning", "Dashboard state write/publish failed", error=str(exc))
        log_event(
            logger,
            "info",
            "Finished sync run",
            run_id=summary["run_id"],
            status=summary["status"],
            duration_sec=summary["duration_sec"],
            rows_uploaded_values=summary["rows_uploaded_values"],
            rows_uploaded_di=summary["rows_uploaded_di"],
            chunks_failed=summary["chunks_failed"],
        )


if __name__ == "__main__":
    run_sync()
