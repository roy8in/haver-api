# Dashboard Data Contract

This repository writes the current Haver execution state to `state/haver_status.json` and appends a compact run event to `state/haver_events.jsonl`.

The home dashboard can consume those files directly from GitHub:

- `state/haver_status.json` for the latest run or preflight check
- `state/haver_events.jsonl` for a chronological stream of recent run summaries
- `state/haver_latest_failure.json` for the latest failure card
- `state/haver_failures.jsonl` for a failure-only history stream
- `docs/haver-status.schema.json` for the schema contract

## Record Types

- `run`: full sync execution from `main.py`
- `preflight`: login readiness check from `scripts/haver_preflight.py`

## Typical Fields

- `run_id`
- `start_time`
- `end_time`
- `duration_sec`
- `status`
- `error_stage`
- `error_message`
- `haver.authenticated`
- `haver.login_required`
- `metrics.metadata_rows`
- `metrics.rows_uploaded_values`
- `metrics.rows_uploaded_di`
- `metrics.chunks_failed`
- `publish.enabled`
- `files.latest_failure_json`
- `files.failure_events_jsonl`

## Consumption Notes

- Read `state/haver_status.json` for the latest state card.
- Read `state/haver_events.jsonl` for timeline widgets or recent history.
- Treat missing fields as optional and fall back to display defaults.
