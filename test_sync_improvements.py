import csv
import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


if "Haver" not in sys.modules:
    haver_stub = types.SimpleNamespace(
        direct=lambda *_args, **_kwargs: None,
        metadata=lambda *_args, **_kwargs: pd.DataFrame(),
        data=lambda *_args, **_kwargs: pd.DataFrame(),
    )
    sys.modules["Haver"] = haver_stub


main = importlib.import_module("main")
db_handler = importlib.import_module("db_handler")
haver_provider = importlib.import_module("haver_provider")
data_processor = importlib.import_module("data_processor")
run_logging = importlib.import_module("run_logging")


class SyncTaskTests(unittest.TestCase):
    def test_build_sync_tasks_keeps_backfill_when_metadata_unchanged_but_db_is_behind(self):
        meta_df = pd.DataFrame(
            [
                {
                    "ticker_pk": "db:test",
                    "datetimemod": "2026-04-20T00:00:00",
                    "startdate": "2020-01-01",
                    "enddate": "2026-04-20",
                    "frequency": "M",
                }
            ]
        )

        tasks, skipped_up_to_date, kept_for_backfill = main._build_sync_tasks(
            meta_df,
            {"db:test": "2026-04-20 00:00:00"},
            {"db:test": "2026-03-01"},
        )

        self.assertEqual(skipped_up_to_date, 0)
        self.assertEqual(kept_for_backfill, 1)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["pk"], "db:test")

    def test_build_sync_tasks_skips_only_when_db_is_current_and_metadata_matches(self):
        meta_df = pd.DataFrame(
            [
                {
                    "ticker_pk": "db:test",
                    "datetimemod": "2026-04-20T00:00:00",
                    "startdate": "2020-01-01",
                    "enddate": "2026-04-20",
                    "frequency": "M",
                }
            ]
        )

        tasks, skipped_up_to_date, kept_for_backfill = main._build_sync_tasks(
            meta_df,
            {"db:test": "2026-04-20 00:00:00"},
            {"db:test": "2026-04-20"},
        )

        self.assertEqual(tasks, [])
        self.assertEqual(skipped_up_to_date, 1)
        self.assertEqual(kept_for_backfill, 0)


class DbHandlerTests(unittest.TestCase):
    def test_upsert_uses_do_nothing_when_only_key_columns_exist(self):
        df = pd.DataFrame([{"date": "2026-04-01"}])

        with patch.object(db_handler, "send_sql") as send_sql:
            uploaded = db_handler.upsert_data(df, "haver_di_test", chunk_size=1000)

        sent_sql = send_sql.call_args[0][0]
        self.assertIn("DO NOTHING", sent_sql)
        self.assertNotIn("DO UPDATE SET ;", sent_sql)
        self.assertEqual(uploaded, 1)


class HaverProviderTests(unittest.TestCase):
    def test_process_haver_data_returns_empty_on_column_mismatch(self):
        raw = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = haver_provider._process_haver_data(raw, ["ticker_a"])
        self.assertTrue(result.empty)


class DataProcessorTests(unittest.TestCase):
    def test_fetch_raw_data_handles_duplicate_rows(self):
        payload = {
            "data": {
                "rows": [
                    ["2026-01-31", "db:vpmm", "49"],
                    ["2026-01-31", "db:vpmm", "50"],
                    ["2026-02-28", "db:vpmm", "52"],
                ]
            }
        }

        with patch.object(data_processor.db, "send_sql", return_value=payload):
            result = data_processor.fetch_raw_data("vpmm")

        self.assertEqual(list(result.columns), ["db:vpmm"])
        self.assertEqual(result.iloc[0, 0], 50.0)
        self.assertEqual(result.iloc[1, 0], 52.0)


class LoggingTests(unittest.TestCase):
    def test_append_summary_writes_headers_and_row(self):
        summary_path = Path("test_summary_output.csv")
        row = {
            "run_id": "run-1",
            "start_time": "2026-04-22T06:00:00",
            "end_time": "2026-04-22T06:10:00",
            "duration_sec": 600,
            "status": "SUCCESS",
        }

        try:
            run_logging.append_summary(summary_path, row)

            with summary_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
        finally:
            if summary_path.exists():
                summary_path.unlink()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["run_id"], "run-1")
        self.assertEqual(rows[0]["status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
