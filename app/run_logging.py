import csv
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = BASE_DIR / "logs"
LOGGER_NAME = "haver_api"
SUMMARY_HEADERS = [
    "run_id",
    "start_time",
    "end_time",
    "duration_sec",
    "status",
    "ticker_total",
    "metadata_rows",
    "rows_uploaded_metadata",
    "ticker_skipped",
    "ticker_backfill",
    "ticker_fetched",
    "chunks_total",
    "chunks_failed",
    "rows_uploaded_values",
    "rows_uploaded_di",
    "error_stage",
    "error_message",
]


def _format_extra(extra):
    if not extra:
        return ""
    parts = [f"{key}={value}" for key, value in extra.items()]
    return " | " + ", ".join(parts)


def setup_run_logging():
    LOG_DIR.mkdir(exist_ok=True)

    run_started_at = datetime.now()
    run_id = run_started_at.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    app_log_path = LOG_DIR / f"app_{run_started_at.strftime('%Y-%m-%d')}.log"
    summary_log_path = LOG_DIR / f"summary_{run_started_at.strftime('%Y-%m')}.csv"

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = logging.FileHandler(app_log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
        except Exception:
            pass

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return {
        "run_id": run_id,
        "run_started_at": run_started_at,
        "app_log_path": app_log_path,
        "summary_log_path": summary_log_path,
        "logger": logger,
    }


def get_logger(name):
    return logging.getLogger(f"{LOGGER_NAME}.{name}")


def log_event(logger, level, message, **extra):
    log_method = getattr(logger, level.lower())
    log_method(f"{message}{_format_extra(extra)}")


def append_summary(summary_path, summary_row):
    summary_path.parent.mkdir(exist_ok=True)
    file_exists = summary_path.exists()

    with summary_path.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=SUMMARY_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow({header: summary_row.get(header, "") for header in SUMMARY_HEADERS})
