"""Haver 클라이언트 초기화, 메타데이터 조회, 시계열 수집을 담당합니다."""

import os
import warnings

import pandas as pd
import Haver

try:
    import Haver._Haveraux as Haveraux
except Exception:
    Haveraux = None

from run_logging import get_logger, log_event


logger = get_logger("haver")


def _summarize_error_report(report):
    codelists = report.get("codelists", {}) if isinstance(report, dict) else {}
    summary = {}
    for key, value in codelists.items():
        if isinstance(value, list):
            summary[f"{key}_count"] = len(value)
            if value:
                summary[f"{key}_sample"] = ", ".join(str(item) for item in value[:10])
    if "databasepath" in report:
        summary["databasepath"] = report.get("databasepath")
    return summary


def _safe_direct_state():
    try:
        return Haver.direct()
    except Exception as exc:
        return f"error: {exc}"


def _safe_haver_path():
    try:
        return Haver.path()
    except Exception as exc:
        return f"error: {exc}"


def ensure_database_path(logger=None):
    """Haver DB 경로가 비어 있을 때 환경 변수 기반으로 사용 가능한 경로를 복구합니다."""
    current_path = _safe_haver_path()
    if current_path not in ("", None):
        return True, current_path

    requested_path = os.getenv("HAVER_PATH", "").strip()
    if requested_path:
        try:
            Haver.path(requested_path)
            current_path = _safe_haver_path()
            if current_path not in ("", None):
                if logger is not None:
                    log_event(logger, "info", "Configured Haver database path from HAVER_PATH", haver_path=current_path)
                return True, current_path
        except Exception as exc:
            if logger is not None:
                log_event(logger, "warning", "Failed to configure Haver database path from HAVER_PATH", error=str(exc), haver_path=requested_path)

    dlxpar = os.getenv("DLXPAR", "").strip()
    if dlxpar:
        try:
            Haver.path("ini")
            current_path = _safe_haver_path()
            if current_path not in ("", None):
                if logger is not None:
                    log_event(logger, "info", "Configured Haver database path from DLXPAR", dlxpar_present=True)
                return True, current_path
        except Exception as exc:
            if logger is not None:
                log_event(logger, "warning", "Failed to configure Haver database path from DLXPAR", error=str(exc), dlxpar_present=True)

    dlxdb = os.getenv("DLXDB", "").strip()
    if dlxdb:
        try:
            Haver.path("auto")
            current_path = _safe_haver_path()
            if current_path not in ("", None):
                if logger is not None:
                    log_event(logger, "info", "Configured Haver database path from DLXDB", dlxdb_present=True)
                return True, current_path
        except Exception as exc:
            if logger is not None:
                log_event(logger, "warning", "Failed to configure Haver database path from DLXDB", error=str(exc), dlxdb_present=True)

    if logger is not None:
        log_event(
            logger,
            "warning",
            "Haver database path is not configured",
            haver_path_env=bool(requested_path),
            dlxpar_env=bool(dlxpar),
            dlxdb_env=bool(dlxdb),
            current_path=current_path,
        )
    return False, current_path


def get_login_status():
    """현재 Haver 로그인 상태를 가능한 범위에서 확인해 반환합니다."""
    direct_state = _safe_direct_state()
    authenticated = getattr(Haveraux, "authenticated_", None) if Haveraux is not None else None

    if authenticated is True:
        login_required = False
        ready = True
        note = "Haver session is authenticated."
    elif direct_state is True:
        login_required = False
        ready = True
        note = "Haver session appears ready."
    else:
        login_required = False
        ready = False
        note = "Haver login state could not be confirmed yet."

    return {
        "direct_state": direct_state,
        "authenticated": authenticated,
        "login_required": login_required,
        "ready": ready,
        "note": note,
    }


def log_login_status(status=None, level="info"):
    """Haver 로그인 상태를 일관된 형식으로 로그에 남깁니다."""
    if status is None:
        status = get_login_status()

    log_event(
        logger,
        level,
        "Haver login status snapshot",
        direct_state=status["direct_state"],
        authenticated=status["authenticated"],
        login_required=status["login_required"],
        ready=status["ready"],
        note=status["note"],
    )
    return status


def _metadata_request(ticker_list, log_error=True):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        meta_df = Haver.metadata(ticker_list)
    if isinstance(meta_df, pd.DataFrame):
        return meta_df
    if log_error and isinstance(meta_df, dict):
        log_event(logger, "warning", "Metadata fetch returned Haver error report", **_summarize_error_report(meta_df))
    return pd.DataFrame()


def _series_request(ticker_list, start_date):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        data = Haver.data(ticker_list, startdate=start_date, dates=True)

    if isinstance(data, dict):
        log_event(
            logger,
            "warning",
            "Series fetch returned Haver error report",
            ticker_count=len(ticker_list),
            start_date=start_date,
            **_summarize_error_report(data),
        )
        raise RuntimeError("Haver series query returned an error report.")

    return data


def initialize():
    """Haver 클라이언트를 초기화합니다."""
    try:
        haver_path = os.getenv("HAVER_PATH", "").strip()
        direct_before = _safe_direct_state()
        if haver_path:
            Haver.path(haver_path)
            log_event(
                logger,
                "info",
                "Configured Haver database path",
                haver_path=haver_path,
                direct_before=direct_before,
            )
        Haver.direct(1)
        ensure_database_path(logger)
        log_event(
            logger,
            "info",
            "Initialized Haver client",
            direct_before=direct_before,
            direct_after=_safe_direct_state(),
            haver_path=haver_path,
        )
        return True
    except Exception as e:
        log_event(logger, "error", "Haver initialization error", error=str(e), direct_state=_safe_direct_state())
        return False


def preflight_login():
    """로그인 팝업을 직접 띄우지 않고 Haver 사용 가능 상태를 점검합니다."""
    status = get_login_status()
    if not status["ready"]:
        log_event(
            logger,
            "warning",
            "Unable to confirm Haver login readiness",
            direct_state=status["direct_state"],
            authenticated=status["authenticated"],
            note=status["note"],
        )
        return False, status

    log_event(
        logger,
        "info",
        "Haver login preflight passed",
        direct_state=status["direct_state"],
        authenticated=status["authenticated"],
        note=status["note"],
    )
    return True, status


def fetch_metadata(ticker_list):
    """요청한 티커 목록의 Haver 메타데이터를 조회합니다."""
    log_event(logger, "info", "Fetching metadata", ticker_count=len(ticker_list))
    try:
        path_ready, path_value = ensure_database_path(logger)
        if not path_ready:
            log_event(logger, "warning", "Proceeding with metadata fetch while Haver database path is unresolved", haver_path=path_value)
        meta_df = pd.DataFrame()
        batch_error = None

        try:
            meta_df = _metadata_request(ticker_list)
        except Exception as exc:
            batch_error = exc
            log_event(logger, "warning", "Metadata batch raised; retrying", error=str(exc))

        if meta_df.empty:
            log_event(logger, "warning", "Metadata fetch failed; forcing DLX Direct reconnect and retrying")
            try:
                Haver.direct("force")
            except Exception as exc:
                log_event(logger, "warning", "DLX Direct force reconnect failed", error=str(exc))
            else:
                log_event(logger, "info", "DLX Direct force reconnect issued", direct_state=_safe_direct_state())
            try:
                meta_df = _metadata_request(ticker_list)
            except Exception as exc:
                batch_error = exc
                log_event(logger, "warning", "Metadata batch raised after reconnect", error=str(exc))

        if meta_df.empty and len(ticker_list) > 1:
            log_event(logger, "warning", "Metadata batch failed; retrying per ticker", ticker_count=len(ticker_list))
            recovered = []
            failed = []
            for ticker in ticker_list:
                try:
                    single_df = _metadata_request([ticker], log_error=False)
                except Exception as exc:
                    failed.append(ticker)
                    log_event(logger, "warning", "Metadata per-ticker request raised", ticker=ticker, error=str(exc))
                    continue
                if single_df.empty:
                    failed.append(ticker)
                else:
                    recovered.append(single_df)
            if failed:
                log_event(
                    logger,
                    "warning",
                    "Metadata per-ticker retry had failures",
                    failed_count=len(failed),
                    failed_sample=", ".join(str(ticker) for ticker in failed[:20]),
                )
            if recovered:
                meta_df = pd.concat(recovered, ignore_index=True)

        if not isinstance(meta_df, pd.DataFrame) or meta_df.empty:
            if batch_error is not None:
                log_event(logger, "warning", "Metadata fetch completed without rows after batch error", error=str(batch_error))
            log_event(logger, "warning", "Metadata fetch returned no rows")
            return pd.DataFrame()

        meta_df = meta_df.copy()
        meta_df.columns = [c.lower() for c in meta_df.columns]
        required_columns = {"database", "code"}
        missing_columns = required_columns.difference(meta_df.columns)
        if missing_columns:
            log_event(logger, "error", "Metadata response missing required columns", missing_columns=sorted(missing_columns))
            return pd.DataFrame()

        meta_df["ticker_pk"] = _build_ticker_pks(meta_df, ticker_list)
        log_event(logger, "info", "Metadata fetch complete", metadata_rows=len(meta_df))
        return meta_df
    except Exception as e:
        log_event(logger, "error", "Exception in fetch_metadata", error=str(e))
        return pd.DataFrame()


def _build_ticker_pks(meta_df, ticker_list):
    res_pks = []
    lower_lookup = [(orig, orig.lower()) for orig in ticker_list]

    for _, row in meta_df.iterrows():
        database = str(row["database"]).lower()
        code = str(row["code"]).lower()

        if "(" in code or "%" in code:
            matched_orig = None
            code_prefix = code.split("(", 1)[0]
            for original, original_lower in lower_lookup:
                if database in original_lower and code_prefix in original_lower:
                    matched_orig = original
                    break
            res_pks.append(matched_orig or f"{database}:{code}")
        else:
            res_pks.append(f"{database}:{code}")

    return res_pks


def fetch_series_data(ticker_chunk, start_date):
    """시계열 데이터를 조회하고, 청크 실패 시 티커별 재시도로 복구합니다."""
    log_event(logger, "info", "Fetching series chunk", ticker_count=len(ticker_chunk), start_date=start_date)
    try:
        data = _series_request(ticker_chunk, start_date)
        processed = _process_haver_data(data, ticker_chunk)
        log_event(logger, "info", "Chunk fetch complete", ticker_count=len(ticker_chunk), rows=len(processed))
        return processed
    except Exception as exc:
        log_event(
            logger,
            "warning",
            "Chunk fetch failed, switching to per-ticker fallback",
            ticker_count=len(ticker_chunk),
            start_date=start_date,
            error=str(exc),
        )

    combined_results = []
    failed_tickers = []
    for ticker in ticker_chunk:
        try:
            single_data = _series_request([ticker], start_date)
            processed = _process_haver_data(single_data, [ticker])
            if not processed.empty:
                combined_results.append(processed)
        except Exception as exc:
            failed_tickers.append((ticker, str(exc)))

    if failed_tickers:
        failed_names = ", ".join(ticker for ticker, _ in failed_tickers[:10])
        log_event(
            logger,
            "warning",
            "Per-ticker fallback had failures",
            failed_count=len(failed_tickers),
            failed_names=failed_names,
        )

    if combined_results:
        combined = pd.concat(combined_results, ignore_index=True)
        log_event(logger, "info", "Per-ticker fallback complete", rows=len(combined))
        return combined
    return pd.DataFrame()


def _process_haver_data(data, ticker_names):
    """Haver 응답 데이터를 date, ticker_pk, value 구조의 long 데이터프레임으로 변환합니다."""
    if data is None:
        return pd.DataFrame()

    if isinstance(data, pd.Series):
        data = data.to_frame()
    if not isinstance(data, pd.DataFrame) or data.empty:
        return pd.DataFrame()

    normalized = data.copy()
    if normalized.shape[1] != len(ticker_names):
        log_event(
            logger,
            "warning",
            "Haver data column mismatch",
            expected_columns=len(ticker_names),
            actual_columns=normalized.shape[1],
        )
        return pd.DataFrame()

    normalized.columns = ticker_names
    long_df = normalized.reset_index().rename(columns={"index": "date"})
    if "date" not in long_df.columns:
        first_column = long_df.columns[0]
        long_df = long_df.rename(columns={first_column: "date"})

    long_df = pd.melt(long_df, id_vars=["date"], var_name="ticker_pk", value_name="value")
    long_df = long_df.dropna(subset=["value"])
    if long_df.empty:
        return long_df

    long_df["date"] = pd.to_datetime(long_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    long_df = long_df.dropna(subset=["date"])
    return long_df
