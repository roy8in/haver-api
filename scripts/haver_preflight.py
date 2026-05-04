import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


import dashboard_state
import haver_provider as haver
from run_logging import setup_run_logging


def main():
    run_context = setup_run_logging()
    logger = run_context["logger"]

    status = haver.log_login_status(level="info")
    if not status["ready"]:
        message = "Unable to confirm Haver login readiness before the scheduled sync starts."
        record = dashboard_state.build_preflight_record(run_context, status, "UNKNOWN", message)
        try:
            dashboard_state.write_status(record)
        except Exception as exc:
            logger.warning("Dashboard state write failed | error=%s", exc)
        dashboard_state.publish_status(logger)
        return 0

    record = dashboard_state.build_preflight_record(
        run_context,
        status,
        "READY",
        "Haver login is ready for the scheduled sync.",
    )
    try:
        dashboard_state.write_status(record)
    except Exception as exc:
        logger.warning("Dashboard state write failed | error=%s", exc)
    dashboard_state.publish_status(logger)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
