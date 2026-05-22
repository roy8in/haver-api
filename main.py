"""Haver 데이터 수집, DB 업로드, 후처리, 상태 기록을 순서대로 실행하는 메인 스크립트입니다."""

import csv
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
import excel_export
import haver_provider as haver
from run_logging import append_summary, log_event, setup_run_logging


BASE_DIR = Path(__file__).resolve().parent
TICKER_AWARE_DB_TABLES = [
    "haver_metadata",
    "haver_values",
    "haver_diff3m_policy_rate",
    "haver_inflation_mom",
    "haver_inflation_yoy",
    "haver_inflation_3m_annualized",
]


def _standardize_mod(val):
    """Haver 메타데이터 수정시각을 비교 가능한 문자열로 정규화합니다."""
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
    """함수를 데몬 스레드에서 실행하고 결과, 타임아웃 여부, 오류를 반환합니다."""
    outcome = {"result": None, "error": None}

    def runner():
        try:
            outcome["result"] = func()
        except Exception as exc:
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


def _get_cli_flag(flag_name):
    return flag_name in sys.argv[1:]


def _has_successful_run_on_date(summary_log_path, target_date):
    summary_path = Path(summary_log_path)
    if not summary_path.exists():
        return False

    try:
        with summary_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if row.get("status") != "SUCCESS":
                    continue
                started = pd.to_datetime(row.get("start_time", ""), errors="coerce")
                if pd.isna(started):
                    continue
                if started.date() == target_date:
                    return True
    except OSError:
        return False

    return False


def _record_stage_timing(summary, stage_name, started_at):
    elapsed = round(time.perf_counter() - started_at, 3)
    summary.setdefault("stage_timings_sec", {})[stage_name] = elapsed
    return time.perf_counter()


def _ticker_series_part(ticker):
    ticker = str(ticker).strip()
    if not ticker or "@" not in ticker:
        return ""

    ticker = ticker.split("@", 1)[0]
    if "%" in ticker:
        ticker = ticker.split("%", 1)[0]
    if "(" in ticker:
        ticker = ticker.split("(", 1)[0]

    return ticker.strip()


def _filter_valid_tickers(ticker_list, logger=None):
    """Haver 조회 전에 빈 값과 구조적으로 잘못된 티커를 제외합니다."""
    valid_tickers = []
    skipped_tickers = []

    for ticker in ticker_list:
        normalized = str(ticker).strip()
        if normalized.lower() in {"", "none", "nan", "nat"}:
            continue

        if "@" not in normalized:
            valid_tickers.append(normalized)
            continue

        series_part = _ticker_series_part(normalized)
        if not series_part or len(series_part) > 8:
            skipped_tickers.append(normalized)
            continue

        valid_tickers.append(normalized)

    if skipped_tickers and logger is not None:
        log_event(
            logger,
            "warning",
            "Skipped invalid ticker codes before metadata fetch",
            skipped_count=len(skipped_tickers),
            skipped_sample=", ".join(skipped_tickers[:20]),
        )

    return valid_tickers, skipped_tickers


def _classify_failure(error_stage, error_message, login_required=False):
    message = (error_message or "").lower()
    stage = (error_stage or "").lower()

    if stage == "ticker_validation":
        return "ticker_validation_failed"
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


def _build_sync_tasks(meta_df, db_metadata, db_max_dates, full_refresh=False):
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
        if not full_refresh and pk in db_max_dates:
            parsed_db_last = pd.to_datetime(db_max_dates.get(pk), errors="coerce")
            if not pd.isna(parsed_db_last):
                db_last = parsed_db_last

        if full_refresh:
            fetch_start = m_start
        elif db_last is None:
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


def _cleanup_removed_tickers(meta_df, logger, summary):
    if meta_df is None or meta_df.empty or "ticker_pk" not in meta_df.columns:
        return True

    success = True
    keep_tickers = (
        meta_df["ticker_pk"]
        .dropna()
        .astype(str)
        .map(str.strip)
        .loc[lambda s: s != ""]
        .drop_duplicates()
        .tolist()
    )

    for table_name in TICKER_AWARE_DB_TABLES:
        if not db.prune_rows_not_in_tickers(table_name, keep_tickers):
            success = False

    summary["ticker_cleanup_count"] = len(keep_tickers)
    log_event(
        logger,
        "info",
        "Pruned DB tables to match current ticker list",
        ticker_count=len(keep_tickers),
        table_count=len(TICKER_AWARE_DB_TABLES),
    )
    return success


def _freq_label(value):
    if pd.isna(value) or str(value).strip() == "":
        return "ALL"
    return str(value).strip()


def _build_excel_full_export_tasks(meta_df):
    start_col = next((c for c in ["startdate", "start", "begin"] if c in meta_df.columns), None)

    full_tasks = []
    for _, row in meta_df.iterrows():
        if "ticker_pk" not in row or pd.isna(row["ticker_pk"]):
            continue

        start_value = _parse_metadata_date(row[start_col], "1900-01-01") if start_col else pd.Timestamp("1900-01-01")
        full_tasks.append(
            {
                "pk": row["ticker_pk"],
                "freq": _freq_label(row.get("frequency", row.get("freq", "ALL"))),
                "start": start_value,
            }
        )

    return full_tasks


def _fetch_excel_full_export_frames(meta_df, logger, chunk_size=50):
    full_tasks = _build_excel_full_export_tasks(meta_df)
    if not full_tasks:
        return {}, 0

    full_frames_by_freq = {}
    failed_chunks = 0
    task_df = pd.DataFrame(full_tasks).sort_values("start")

    for freq, group in task_df.groupby("freq"):
        freq_label = _freq_label(freq)
        tickers_in_freq = group.to_dict("records")
        total_count = len(tickers_in_freq)
        log_event(logger, "info", "Building full Excel baseline from Haver", frequency=freq_label, ticker_count=total_count)

        for i in range(0, total_count, chunk_size):
            chunk_tasks = tickers_in_freq[i:i + chunk_size]
            chunk_tickers = [task["pk"] for task in chunk_tasks]
            min_start = min(task["start"] for task in chunk_tasks).strftime("%Y-%m-%d")

            log_event(
                logger,
                "info",
                "Fetching full Excel baseline chunk",
                frequency=freq_label,
                chunk_index=i // chunk_size + 1,
                chunk_size=len(chunk_tickers),
                min_start=min_start,
            )
            long_df = haver.fetch_series_data(chunk_tickers, min_start)
            if long_df.empty:
                failed_chunks += 1
                log_event(
                    logger,
                    "error",
                    "Full Excel baseline chunk returned no data",
                    frequency=freq_label,
                    chunk_index=i // chunk_size + 1,
                )
                continue

            full_frames_by_freq.setdefault(freq_label, []).append(long_df)

    return full_frames_by_freq, failed_chunks


def _write_excel_export(meta_df, series_frames_by_freq, excel_export_path, logger, summary, full_refresh=False):
    summary["excel_export_status"] = "enabled"
    merge_existing = True

    try:
        missing_excel_sheets = excel_export.get_missing_frequency_sheets(meta_df, excel_export_path)
        if not series_frames_by_freq and not missing_excel_sheets and excel_export_path.exists():
            summary["excel_export_status"] = "unchanged"
            log_event(
                logger,
                "info",
                "Excel export already up to date; no series updates to apply",
                output_path=str(excel_export_path),
            )
            return True
        if missing_excel_sheets:
            merge_existing = False
            log_event(
                logger,
                "info",
                "Excel export is missing frequency sheets; rebuilding full workbook from Haver",
                output_path=str(excel_export_path),
                missing_sheets=", ".join(missing_excel_sheets),
            )
            full_frames_by_freq, failed_full_export_chunks = _fetch_excel_full_export_frames(meta_df, logger)
            if failed_full_export_chunks:
                summary["excel_export_status"] = "failed"
                summary["error_stage"] = "excel_export"
                summary["error_message"] = f"{failed_full_export_chunks} full Excel baseline chunk(s) failed."
                summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], False)
                log_event(logger, "error", summary["error_message"])
                return False
            series_frames_by_freq = full_frames_by_freq

        export_result = excel_export.export_series_workbook(
            meta_df,
            series_frames_by_freq,
            excel_export_path,
            merge_existing=merge_existing and not full_refresh,
        )
        summary["excel_export_status"] = "written" if export_result.get("written") else "skipped"
        if not export_result.get("written"):
            log_event(
                logger,
                "warning",
                "Excel export skipped because there were no frames to write",
                output_path=str(excel_export_path),
            )
    except Exception as exc:
        summary["excel_export_status"] = "failed"
        summary["error_stage"] = "excel_export"
        summary["error_message"] = str(exc)
        summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], False)
        log_event(
            logger,
            "error",
            "Excel export failed",
            output_path=str(excel_export_path),
            error=str(exc),
        )
        return False

    return True


def run_sync():
    """전체 Haver 동기화 절차를 실행합니다."""
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
    excel_export_enabled = _get_bool_env("HAVER_EXCEL_EXPORT_ENABLED", True)
    full_refresh = _get_bool_env("HAVER_FULL_REFRESH", False) or _get_cli_flag("--full-refresh")
    excel_export_path = Path(os.getenv("HAVER_EXCEL_OUTPUT_PATH", str(BASE_DIR / "state" / "haver_series_export.xlsx")))
    allow_multiple_daily_runs = _get_bool_env("HAVER_ALLOW_MULTIPLE_RUNS_PER_DAY", False)
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
        "excel_export_status": "",
        "excel_export_path": str(excel_export_path),
        "allow_multiple_daily_runs": allow_multiple_daily_runs,
        "full_refresh": full_refresh,
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
        full_refresh=full_refresh,
    )

    try:
        summary["error_stage"] = "daily_run_guard"
        stage_started = time.perf_counter()
        already_ran_today = _has_successful_run_on_date(
            run_context["summary_log_path"],
            run_context["run_started_at"].date(),
        )
        stage_started = _record_stage_timing(summary, "daily_run_guard", stage_started)
        if already_ran_today and not allow_multiple_daily_runs and not full_refresh:
            summary["status"] = "SUCCESS"
            summary["error_stage"] = ""
            summary["excel_export_status"] = "skipped"
            log_event(
                logger,
                "info",
                "Skipping sync because a successful run already completed today",
                run_date=run_context["run_started_at"].date().isoformat(),
            )
            return True

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
        ticker_list, invalid_tickers = _filter_valid_tickers(ticker_list, logger)
        summary["ticker_invalid"] = len(invalid_tickers)
        stage_started = _record_stage_timing(summary, "ticker_load", stage_started)
        if invalid_tickers:
            summary["error_stage"] = "ticker_validation"
            summary["error_message"] = (
                f"{len(invalid_tickers)} invalid ticker(s) found in tickers.csv. "
                "Fix them before rerunning main.py."
            )
            summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], False)
            log_event(
                logger,
                "error",
                summary["error_message"],
                invalid_count=len(invalid_tickers),
                invalid_sample=", ".join(invalid_tickers[:20]),
            )
            return False
        log_event(
            logger,
            "info",
            "Loaded tickers.csv",
            ticker_total=summary["ticker_total"],
            ticker_valid=len(ticker_list),
            ticker_invalid=summary["ticker_invalid"],
        )

        summary["error_stage"] = "db_state_load"
        stage_started = time.perf_counter()
        db_metadata = db.get_stored_metadata()
        db_max_dates = db.get_ticker_max_dates()
        if db_metadata is None or db_max_dates is None:
            summary["error_message"] = "DB state could not be loaded; aborting before Haver upload decisions."
            summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], False)
            log_event(logger, "error", summary["error_message"])
            return False
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
            summary["error_message"] = "No metadata collected."
            summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], False)
            log_event(logger, "warning", summary["error_message"])
            return False

        meta_df.columns = [c.lower() for c in meta_df.columns]
        summary["metadata_rows"] = len(meta_df)
        stage_started = time.perf_counter()
        db.create_table_with_types(meta_df, "haver_metadata")
        summary["rows_uploaded_metadata"] = db.upsert_data(meta_df, "haver_metadata")
        stage_started = _record_stage_timing(summary, "metadata_upload", stage_started)
        if summary["rows_uploaded_metadata"] != len(meta_df):
            summary["error_stage"] = "metadata_upload"
            summary["error_message"] = f"Metadata upload incomplete; uploaded {summary['rows_uploaded_metadata']} of {len(meta_df)} rows."
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

        summary["error_stage"] = "ticker_cleanup"
        stage_started = time.perf_counter()
        if not _cleanup_removed_tickers(meta_df, logger, summary):
            summary["error_message"] = "Failed to prune one or more DB tables to the current ticker list."
            summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], False)
            log_event(logger, "error", summary["error_message"])
            return False
        stage_started = _record_stage_timing(summary, "ticker_cleanup", stage_started)

        summary["error_stage"] = "task_build"
        stage_started = time.perf_counter()
        sync_tasks, skipped_up_to_date, kept_for_backfill = _build_sync_tasks(meta_df, db_metadata, db_max_dates, full_refresh=full_refresh)
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
            if excel_export_enabled:
                if not _write_excel_export(meta_df, {}, excel_export_path, logger, summary):
                    return False
            else:
                summary["excel_export_status"] = "disabled"
            summary["status"] = "SUCCESS"
            summary["error_stage"] = ""
            log_event(logger, "info", "Everything is up-to-date. No data to fetch.")
            return True

        summary["error_stage"] = "series_fetch"
        stage_started = time.perf_counter()
        task_df = task_df.sort_values("start")
        series_frames_by_freq = {}

        for freq, group in task_df.groupby("freq"):
            tickers_in_freq = group.to_dict("records")
            total_count = len(tickers_in_freq)
            freq_label = "ALL" if pd.isna(freq) or str(freq).strip() == "" else str(freq)
            log_event(logger, "info", "Processing frequency group", frequency=freq_label, ticker_count=total_count)

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
                    frequency=freq_label,
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
                        frequency=freq_label,
                        chunk_index=i // chunk_size + 1,
                    )
                    continue

                series_frames_by_freq.setdefault(freq_label, []).append(long_df)

                db.create_table_with_types(long_df, "haver_values")
                uploaded = db.upsert_data(long_df, "haver_values")
                summary["rows_uploaded_values"] += uploaded
                if uploaded != len(long_df):
                    summary["chunks_failed"] += 1
                    log_event(
                        logger,
                        "error",
                        "Chunk upload failed",
                        frequency=freq_label,
                        chunk_index=i // chunk_size + 1,
                        rows_uploaded=uploaded,
                        expected_rows=len(long_df),
                    )
                else:
                    log_event(
                        logger,
                        "info",
                        "Chunk upload complete",
                        frequency=freq_label,
                        chunk_index=i // chunk_size + 1,
                        rows_uploaded=uploaded,
                    )
        stage_started = _record_stage_timing(summary, "series_fetch", stage_started)

        if summary["chunks_failed"]:
            summary["error_stage"] = "series_upload"
            summary["error_message"] = f"{summary['chunks_failed']} chunk(s) failed to fetch or upload."
            summary["failure_category"] = _classify_failure(summary["error_stage"], summary["error_message"], False)
            log_event(logger, "error", summary["error_message"])
            return False

        if excel_export_enabled:
            if not _write_excel_export(meta_df, series_frames_by_freq, excel_export_path, logger, summary, full_refresh=full_refresh):
                return False
        else:
            summary["excel_export_status"] = "disabled"

        summary["error_stage"] = "processing"
        stage_started = time.perf_counter()
        processing_stats = processor.run_processing()
        stage_started = _record_stage_timing(summary, "processing", stage_started)
        summary["rows_uploaded_di"] = processing_stats.get("rows_uploaded_di", 0)
        log_event(logger, "info", "Derived processing complete", rows_uploaded_di=summary["rows_uploaded_di"])

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
