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
    """Apply certificate bundle settings for outbound requests."""
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
    """Send SQL to the PostgreSQL API."""
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
    """Extract rows from the API response payload."""
    if not res or not isinstance(res, dict):
        return []

    data = res.get("data", {})
    if isinstance(data, dict):
        return data.get("rows", [])
    if isinstance(data, list):
        return data
    return []


def get_ticker_max_dates():
    """Return the latest stored date for each ticker."""
    check_sql = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'haver_values')"
    rows = _extract_rows(send_sql(check_sql))

    if not rows:
        return {}
    try:
        if not rows[0][0]:
            return {}
    except (IndexError, TypeError):
        return {}

    result_rows = _extract_rows(send_sql("SELECT ticker_pk, MAX(date) FROM haver_values GROUP BY ticker_pk"))
    max_dates = {}
    for row in result_rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        max_dates[str(row[0])] = str(row[1])
    log_event(logger, "info", "Loaded ticker max dates", ticker_count=len(max_dates))
    return max_dates


def get_stored_metadata():
    """Return stored datetimemod values by ticker."""
    check_sql = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'haver_metadata')"
    rows = _extract_rows(send_sql(check_sql))

    if not rows:
        return {}
    try:
        if not rows[0][0]:
            return {}
    except (IndexError, TypeError):
        return {}

    result_rows = _extract_rows(send_sql('SELECT "ticker_pk", "datetimemod" FROM haver_metadata'))
    metadata = {}
    for row in result_rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        metadata[str(row[0])] = str(row[1]) if row[1] else ""
    log_event(logger, "info", "Loaded stored metadata", ticker_count=len(metadata))
    return metadata


def create_table_with_types(df, table_name):
    """Create a table based on inferred pandas dtypes."""
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
    if table_name in {"haver_values", "haver_diff3m_policy_rate"}:
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
    if table_name in {"haver_values", "haver_diff3m_policy_rate"}:
        return '"ticker_pk", "date"'
    if table_name.startswith("haver_di_"):
        return '"date"'
    return '"date"'


def upsert_data(df, table_name, chunk_size=1000):
    """Upsert dataframe rows into the target table."""
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
