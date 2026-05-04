import csv
import importlib
import json
import shutil
import sys
import types
import unittest
import time
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
dashboard_state = importlib.import_module("dashboard_state")
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

    def test_summarize_error_report_counts_codelists(self):
        report = {
            "databasepath": "remote (DLX Direct)",
            "codelists": {
                "databaseaccess": ["usecon:gdp", "g10:n111rtar"],
                "codesnotfound": [],
            },
        }

        summary = haver_provider._summarize_error_report(report)

        self.assertEqual(summary["databaseaccess_count"], 2)
        self.assertEqual(summary["databaseaccess_sample"], "usecon:gdp, g10:n111rtar")
        self.assertEqual(summary["codesnotfound_count"], 0)
        self.assertEqual(summary["databasepath"], "remote (DLX Direct)")

    def test_get_login_status_marks_unauthenticated_session_as_login_required(self):
        original_haver = haver_provider.Haver
        original_haveraux = haver_provider.Haveraux
        try:
            haver_provider.Haver = types.SimpleNamespace(direct=lambda *_args, **_kwargs: False)
            haver_provider.Haveraux = types.SimpleNamespace(authenticated_=False)

            status = haver_provider.get_login_status()
        finally:
            haver_provider.Haver = original_haver
            haver_provider.Haveraux = original_haveraux

        self.assertFalse(status["ready"])
        self.assertFalse(status["login_required"])
        self.assertEqual(status["authenticated"], False)

    def test_get_login_status_treats_direct_ready_as_ready(self):
        original_haver = haver_provider.Haver
        original_haveraux = haver_provider.Haveraux
        try:
            haver_provider.Haver = types.SimpleNamespace(direct=lambda *_args, **_kwargs: True)
            haver_provider.Haveraux = types.SimpleNamespace(authenticated_=False)

            status = haver_provider.get_login_status()
        finally:
            haver_provider.Haver = original_haver
            haver_provider.Haveraux = original_haveraux

        self.assertTrue(status["ready"])
        self.assertFalse(status["login_required"])

    def test_preflight_login_warns_when_readiness_is_unknown(self):
        with patch.object(haver_provider, "get_login_status", return_value={
            "direct_state": False,
            "authenticated": False,
            "ready": False,
            "login_required": False,
            "note": "Haver login state could not be confirmed yet.",
        }), patch.object(haver_provider, "log_event") as log_event:
            allowed, status = haver_provider.preflight_login()

        self.assertFalse(allowed)
        self.assertTrue(log_event.called)


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

    def test_call_with_timeout_times_out_for_slow_callable(self):
        def slow_call():
            time.sleep(0.2)
            return True

        result, timed_out, error = main._call_with_timeout(slow_call, 0.05, "slow_call")

        self.assertIsNone(result)
        self.assertTrue(timed_out)
        self.assertIsNone(error)

    def test_initialize_haver_with_retry_retries_after_timeout(self):
        outcomes = iter(
            [
                (None, True, None),
                (True, False, None),
            ]
        )

        def fake_call_with_timeout(*_args, **_kwargs):
            return next(outcomes)

        with patch.object(main, "_call_with_timeout", side_effect=fake_call_with_timeout), patch.object(main.time, "sleep") as sleep:
            result, attempts, error_message = main._initialize_haver_with_retry(
                run_logging.get_logger("test"),
                30,
                2,
                1,
            )

        self.assertTrue(result)
        self.assertEqual(attempts, 2)
        self.assertEqual(error_message, "")
        sleep.assert_called_once_with(1)

    def test_alert_haver_login_issue_calls_send_alert(self):
        with patch.object(main, "send_alert", return_value=["popup"]) as send_alert:
            transports = main._alert_haver_login_issue(
                run_logging.get_logger("test"),
                "Login is required",
                run_id="run-1",
                authenticated=False,
            )

        self.assertEqual(transports, ["popup"])
        send_alert.assert_called_once()


class DashboardStateTests(unittest.TestCase):
    def test_write_status_creates_latest_and_events_files(self):
        summary = {
            "run_id": "run-1",
            "start_time": "2026-05-04T06:00:00",
            "status": "SUCCESS",
            "ticker_total": 10,
            "metadata_rows": 8,
            "rows_uploaded_metadata": 8,
            "ticker_skipped": 2,
            "ticker_backfill": 1,
            "ticker_fetched": 7,
            "chunks_total": 1,
            "chunks_failed": 0,
            "rows_uploaded_values": 100,
            "rows_uploaded_di": 12,
            "error_stage": "",
            "error_message": "",
        }
        run_context = {
            "run_id": "run-1",
            "run_started_at": pd.Timestamp("2026-05-04T06:00:00"),
            "app_log_path": Path("logs/app_test.log"),
            "summary_log_path": Path("logs/summary_test.csv"),
        }

        tmp_path = Path("test_dashboard_state_tmp")
        try:
            tmp_path.mkdir(exist_ok=True)
            with patch.object(dashboard_state, "STATE_DIR", tmp_path), patch.object(dashboard_state, "LATEST_STATUS_PATH", tmp_path / "haver_status.json"), patch.object(dashboard_state, "EVENTS_PATH", tmp_path / "haver_events.jsonl"):
                record = dashboard_state.build_run_record(summary, run_context, login_status={"authenticated": True, "direct_state": True, "login_required": False, "ready": True, "note": "ready"}, alert_transports=["popup"], publish_enabled=True)
                dashboard_state.write_status(record)

                latest_path = tmp_path / "haver_status.json"
                events_path = tmp_path / "haver_events.jsonl"

                self.assertTrue(latest_path.exists())
                self.assertTrue(events_path.exists())

                with latest_path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)

                self.assertEqual(payload["run_id"], "run-1")
                self.assertEqual(payload["publish"]["enabled"], True)
                self.assertEqual(payload["metrics"]["rows_uploaded_metadata"], 8)
                self.assertIn("stages", payload["timings"])
                self.assertEqual(payload["failure"]["category"], "")
                self.assertEqual(payload["retry"]["max_attempts"], 1)
                self.assertEqual(payload["db"]["stored_metadata_count"], 0)

                with events_path.open("r", encoding="utf-8") as handle:
                    lines = handle.readlines()

                self.assertEqual(len(lines), 1)
                event = json.loads(lines[0])
                self.assertEqual(event["run_id"], "run-1")
        finally:
            if tmp_path.exists():
                shutil.rmtree(tmp_path)

    def test_write_status_records_failures_separately(self):
        summary = {
            "run_id": "run-fail",
            "start_time": "2026-05-04T06:10:00",
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
            "error_stage": "haver_initialize",
            "error_message": "Haver login required.",
        }
        run_context = {
            "run_id": "run-fail",
            "run_started_at": pd.Timestamp("2026-05-04T06:10:00"),
            "app_log_path": Path("logs/app_test.log"),
            "summary_log_path": Path("logs/summary_test.csv"),
        }

        tmp_path = Path("test_dashboard_state_tmp")
        try:
            tmp_path.mkdir(exist_ok=True)
            with patch.object(dashboard_state, "STATE_DIR", tmp_path), patch.object(dashboard_state, "LATEST_STATUS_PATH", tmp_path / "haver_status.json"), patch.object(dashboard_state, "EVENTS_PATH", tmp_path / "haver_events.jsonl"), patch.object(dashboard_state, "LATEST_FAILURE_PATH", tmp_path / "haver_latest_failure.json"), patch.object(dashboard_state, "FAILURES_PATH", tmp_path / "haver_failures.jsonl"):
                record = dashboard_state.build_run_record(summary, run_context, login_status={"authenticated": False, "direct_state": False, "login_required": True, "ready": False, "note": "not ready"}, alert_transports=["popup"], publish_enabled=False)
                dashboard_state.write_status(record)

                failure_path = tmp_path / "haver_latest_failure.json"
                failure_events_path = tmp_path / "haver_failures.jsonl"

                self.assertTrue(failure_path.exists())
                self.assertTrue(failure_events_path.exists())

                with failure_path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)

                self.assertEqual(payload["run_id"], "run-fail")
                self.assertEqual(payload["error_stage"], "haver_initialize")
                self.assertEqual(payload["failure"]["category"], "login_required")

                with failure_events_path.open("r", encoding="utf-8") as handle:
                    lines = handle.readlines()

                self.assertEqual(len(lines), 1)
                failure_event = json.loads(lines[0])
                self.assertEqual(failure_event["status"], "FAILED")
        finally:
            if tmp_path.exists():
                shutil.rmtree(tmp_path)


if __name__ == "__main__":
    unittest.main()
