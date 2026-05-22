"""원격 PostgreSQL API에 SQL을 전달하고 테이블 생성/업서트를 담당합니다."""

import os
from pathlib import Path

import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv

from run_logging import get_logger, log_event


logger = get_logger("db")

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(env_path)

POSTGRE_API_URL = os.getenv("POSTGRE_API_URL")
POSTGRE_API_KEY = os.getenv("POSTGRE_API_KEY")
CERT_PATH = os.getenv("CERT_PATH_ENV")
VERIFY_SSL = os.getenv("POSTGRE_VERIFY_SSL", "true").strip().lower() not in {"0", "false", "no"}
REQUEST_TIMEOUT = int(os.getenv("POSTGRE_TIMEOUT_SECONDS", "60"))
POSTGRE_HEADER = {
    "x-api-key": POSTGRE_API_KEY,
    "Content-Type": "application/json",
}

if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def setup_environment():
    """외부 DB 요청에 사용할 인증서 번들 설정을 적용합니다."""
    log_event(
        logger,
        "info",
        "Configured DB transport settings",
        verify_ssl=VERIFY_SSL,
        cert_path=CERT_PATH or "",
        cert_path_exists=bool(CERT_PATH and os.path.exists(CERT_PATH)),
    )
    if CERT_PATH and os.path.exists(CERT_PATH):
        os.environ["REQUESTS_CA_BUNDLE"] = CERT_PATH
        log_event(logger, "info", "Configured certificate bundle", cert_path=CERT_PATH)
    else:
        os.environ.pop("REQUESTS_CA_BUNDLE", None)
        if VERIFY_SSL:
            log_event(
                logger,
                "warning",
                "No certificate bundle configured while SSL verification is enabled",
            )
        else:
            log_event(logger, "warning", "SSL verification disabled for DB requests")


def _request_verify_value():
    if not VERIFY_SSL:
        return False
    if CERT_PATH and os.path.exists(CERT_PATH):
        return CERT_PATH
    return True


def send_sql(sql_text):
    """PostgreSQL API로 SQL 문자열을 전송합니다."""
    if not POSTGRE_API_URL or not POSTGRE_API_KEY:
        log_event(logger, "error", "API URL or key missing in .env")
        return None

    payload = {"sql": sql_text}
    try:
        response = requests.post(
            POSTGRE_API_URL,
            json=payload,
            headers=POSTGRE_HEADER,
            verify=_request_verify_value(),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as exc:
        body_preview = exc.response.text[:500] if exc.response is not None else ""
        log_event(
            logger,
            "error",
            "API request failed with HTTP error",
            status_code=exc.response.status_code if exc.response is not None else "unknown",
            response_preview=body_preview,
        )
    except requests.RequestException as exc:
        log_event(logger, "error", "API request failed", error=str(exc))
    except ValueError as exc:
        log_event(logger, "error", "API returned invalid JSON", error=str(exc))
    return None


def _extract_rows(res):
    """API 응답 payload에서 rows 목록을 추출합니다."""
    if not res or not isinstance(res, dict):
        return []

    data = res.get("data", {})
    if isinstance(data, dict):
        return data.get("rows", [])
    if isinstance(data, list):
        return data
    return []


def _table_exists(table_name):
    check_sql = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
    res = send_sql(check_sql)
    if res is None:
        log_event(logger, "error", "Unable to verify table existence", table_name=table_name)
        return None

    rows = _extract_rows(res)
    if not rows:
        log_event(logger, "error", "Table existence query returned no rows", table_name=table_name)
        return None

    try:
        return bool(rows[0][0])
    except (IndexError, TypeError):
        log_event(logger, "error", "Table existence query returned unexpected rows", table_name=table_name, rows=rows)
        return None


def get_ticker_max_dates():
    """ticker_pk별로 DB에 저장된 최신 날짜를 반환합니다."""
    exists = _table_exists("haver_values")
    if exists is None:
        return None
    if not exists:
        return {}

    res = send_sql("SELECT ticker_pk, MAX(date) FROM haver_values GROUP BY ticker_pk")
    if res is None:
        log_event(logger, "error", "Unable to load ticker max dates")
        return None

    result_rows = _extract_rows(res)
    max_dates = {}
    for row in result_rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        max_dates[str(row[0])] = str(row[1])
    log_event(logger, "info", "Loaded ticker max dates", ticker_count=len(max_dates))
    return max_dates


def get_stored_metadata():
    """저장된 메타데이터 수정시각을 ticker_pk 기준으로 반환합니다."""
    exists = _table_exists("haver_metadata")
    if exists is None:
        return None
    if not exists:
        return {}

    res = send_sql('SELECT "ticker_pk", "datetimemod" FROM haver_metadata')
    if res is None:
        log_event(logger, "error", "Unable to load stored metadata")
        return None

    result_rows = _extract_rows(res)
    metadata = {}
    for row in result_rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        metadata[str(row[0])] = str(row[1]) if row[1] else ""
    log_event(logger, "info", "Loaded stored metadata", ticker_count=len(metadata))
    return metadata


def _normalize_ticker_values(tickers):
    normalized = []
    seen = set()

    for ticker in tickers:
        value = str(ticker).strip()
        if not value or value.lower() in {"none", "nan", "nat"}:
            continue
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)

    return normalized


def prune_rows_not_in_tickers(table_name, tickers, column_name="ticker_pk"):
    """현재 티커 목록에 없는 ticker_pk 행을 대상 테이블에서 삭제합니다."""
    if column_name != "ticker_pk":
        raise ValueError("prune_rows_not_in_tickers only supports the ticker_pk column")

    keep_tickers = _normalize_ticker_values(tickers)
    if keep_tickers:
        keep_sql = ", ".join(_to_sql_literal(ticker) for ticker in keep_tickers)
        delete_sql = f'DELETE FROM {table_name} WHERE "{column_name}" NOT IN ({keep_sql})'
    else:
        delete_sql = f"DELETE FROM {table_name}"

    res = send_sql(delete_sql)
    if res is None:
        log_event(logger, "error", "Failed to prune table by ticker list", table_name=table_name)
        return False

    log_event(
        logger,
        "info",
        "Pruned table by ticker list",
        table_name=table_name,
        keep_count=len(keep_tickers),
    )
    return True


def create_table_with_types(df, table_name):
    """pandas dtype을 바탕으로 대상 테이블을 생성합니다."""
    columns_sql = []
    for col_name, dtype in df.dtypes.items():
        col_lower = col_name.lower()
        sql_type = "TEXT"

        if col_lower == "date":
            sql_type = "DATE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = "TIMESTAMP"
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = "BOOLEAN"
        elif pd.api.types.is_integer_dtype(dtype):
            sql_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "DOUBLE PRECISION"

        if col_lower == "ticker_pk" and table_name == "haver_metadata":
            sql_type += " PRIMARY KEY"

        columns_sql.append(f'"{col_name}" {sql_type}')

    pk_constraint = ""
    if table_name in {"haver_values", "haver_diff3m_policy_rate"} or table_name.startswith("haver_inflation_"):
        pk_constraint = ', PRIMARY KEY ("ticker_pk", "date")'
    elif table_name.startswith("haver_di_"):
        pk_constraint = ', PRIMARY KEY ("date")'

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns_sql)}
        {pk_constraint}
    );
    """
    send_sql(create_sql)
    log_event(logger, "info", "Ensured table exists", table_name=table_name, column_count=len(df.columns))


def _to_sql_literal(val):
    if pd.isna(val):
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return str(val)

    safe_str = str(val).replace("'", "''")
    return f"'{safe_str}'"


def _conflict_target_for(table_name):
    if table_name == "haver_metadata":
        return '"ticker_pk"'
    if table_name in {"haver_values", "haver_diff3m_policy_rate"} or table_name.startswith("haver_inflation_"):
        return '"ticker_pk", "date"'
    if table_name.startswith("haver_di_"):
        return '"date"'
    return '"date"'


def upsert_data(df, table_name, chunk_size=1000):
    """데이터프레임 행을 대상 테이블에 upsert합니다."""
    if df.empty:
        log_event(logger, "warning", "Skipping upsert for empty dataframe", table_name=table_name)
        return 0

    total_rows = len(df)
    rows_uploaded = 0
    conflict_target = _conflict_target_for(table_name)
    update_columns = [c for c in df.columns if c.lower() not in {"ticker_pk", "date"}]
    update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_columns)
    conflict_action = f"DO UPDATE SET {update_set}" if update_set else "DO NOTHING"

    for start in range(0, total_rows, chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        values_list = []

        for row in chunk.itertuples(index=False, name=None):
            row_values = [_to_sql_literal(val) for val in row]
            values_list.append(f"({', '.join(row_values)})")

        col_names = ", ".join(f'"{c}"' for c in df.columns)
        all_values = ", ".join(values_list)
        upsert_sql = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES {all_values}
        ON CONFLICT ({conflict_target})
        {conflict_action};
        """
        res = send_sql(upsert_sql)
        if not res:
            log_event(
                logger,
                "error",
                "Upsert chunk failed",
                table_name=table_name,
                chunk_start=start,
                chunk_rows=len(chunk),
            )
            continue

        rows_uploaded += len(chunk)

    log_event(
        logger,
        "info",
        "Completed upsert",
        table_name=table_name,
        rows_uploaded=rows_uploaded,
        total_rows=total_rows,
    )
    return rows_uploaded
