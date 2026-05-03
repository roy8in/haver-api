import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from app.alerts import send_alert
from app import dashboard_state
from app import haver_provider as haver
from app.run_logging import setup_run_logging


def main():
    run_context = setup_run_logging()
    logger = run_context["logger"]

    status = haver.log_login_status(level="info")
    if status["login_required"]:
        message = "Haver login is required before the scheduled sync starts."
        record = dashboard_state.build_preflight_record(run_context, status, "BLOCKED", message)
        try:
            dashboard_state.write_status(record)
        except Exception as exc:
            logger.warning("Dashboard state write failed | error=%s", exc)
        send_alert(
            logger,
            "Haver login required",
            message,
            run_id=run_context["run_id"],
            direct_state=status["direct_state"],
            authenticated=status["authenticated"],
            note=status["note"],
        )
        dashboard_state.publish_status(logger)
        return 2

    if not status["ready"]:
        message = "Unable to confirm Haver login readiness before the scheduled sync starts."
        record = dashboard_state.build_preflight_record(run_context, status, "UNKNOWN", message)
        try:
            dashboard_state.write_status(record)
        except Exception as exc:
            logger.warning("Dashboard state write failed | error=%s", exc)
        send_alert(
            logger,
            "Haver login state unknown",
            message,
            run_id=run_context["run_id"],
            direct_state=status["direct_state"],
            authenticated=status["authenticated"],
            note=status["note"],
        )
        dashboard_state.publish_status(logger)
        return 3

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
