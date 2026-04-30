import os
import sys
import threading
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

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
    }

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
    )

    try:
        summary["error_stage"] = "environment_setup"
        db.setup_environment()
        log_event(logger, "info", "Starting Haver initialization", timeout_sec=init_timeout)
        haver_initialized, timed_out, init_error = _call_with_timeout(
            haver.initialize,
            init_timeout,
            "haver_initialize",
        )
        if timed_out:
            summary["error_stage"] = "haver_initialize"
            summary["error_message"] = f"Haver initialization timed out after {init_timeout} seconds."
            log_event(logger, "error", summary["error_message"])
            return False
        if init_error is not None:
            summary["error_stage"] = "haver_initialize"
            summary["error_message"] = str(init_error)
            log_event(logger, "error", "Haver initialization raised an exception", error=str(init_error))
            return False
        if not haver_initialized:
            summary["error_stage"] = "haver_initialize"
            summary["error_message"] = "Haver provider initialization returned False."
            log_event(logger, "error", summary["error_message"])
            return False
        log_event(logger, "info", "Haver initialization complete")

        summary["error_stage"] = "ticker_load"
        tickers_csv = pd.read_csv(BASE_DIR / "tickers.csv")
        ticker_list = tickers_csv["ticker"].tolist()
        summary["ticker_total"] = len(ticker_list)
        log_event(logger, "info", "Loaded tickers.csv", ticker_total=summary["ticker_total"])

        summary["error_stage"] = "db_state_load"
        db_metadata = db.get_stored_metadata()
        db_max_dates = db.get_ticker_max_dates()

        summary["error_stage"] = "metadata_fetch"
        meta_df = haver.fetch_metadata(ticker_list)
        if meta_df.empty:
            summary["error_message"] = "No metadata collected."
            log_event(logger, "warning", summary["error_message"])
            return False

        meta_df.columns = [c.lower() for c in meta_df.columns]
        summary["metadata_rows"] = len(meta_df)
        db.create_table_with_types(meta_df, "haver_metadata")
        summary["rows_uploaded_metadata"] = db.upsert_data(meta_df, "haver_metadata")
        if summary["rows_uploaded_metadata"] == 0:
            summary["error_stage"] = "metadata_upload"
            summary["error_message"] = "Metadata upload failed; DB API accepted 0 rows."
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
        sync_tasks, skipped_up_to_date, kept_for_backfill = _build_sync_tasks(meta_df, db_metadata, db_max_dates)
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

        summary["error_stage"] = "processing"
        processing_stats = processor.run_processing()
        summary["rows_uploaded_di"] = processing_stats.get("rows_uploaded_di", 0)
        log_event(logger, "info", "Derived processing complete", rows_uploaded_di=summary["rows_uploaded_di"])

        if summary["chunks_failed"]:
            summary["error_stage"] = "series_upload"
            summary["error_message"] = f"{summary['chunks_failed']} chunk(s) failed to fetch or upload."
            log_event(logger, "error", summary["error_message"])
            return False

        summary["status"] = "SUCCESS"
        summary["error_stage"] = ""
        return True
    except Exception as exc:
        summary["error_message"] = str(exc)
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
        append_summary(run_context["summary_log_path"], summary)
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
