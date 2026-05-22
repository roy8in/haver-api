"""DB에 저장된 Haver 원자료를 가공 지표로 변환하고 결과 테이블에 업로드합니다."""

import pandas as pd

import db_handler as db
from processors.inflation import build_inflation_metric_frames
from processors.policy_rate import process_policy_rate
from processors.pmi import process_pmi
from run_logging import get_logger, log_event


logger = get_logger("processor")


def fetch_raw_data(suffix, database_filter=None):
    """
    ticker_pk에 지정한 suffix가 포함된 원자료를 조회합니다.

    반환값은 날짜를 인덱스로 하고 ticker_pk를 컬럼으로 갖는 wide 데이터프레임입니다.
    """
    if database_filter:
        sql = f"""
        SELECT v.date, v.ticker_pk, v.value
        FROM haver_values v
        JOIN haver_metadata m ON v.ticker_pk = m.ticker_pk
        WHERE v.ticker_pk ILIKE '%%{suffix}%%'
          AND m.database = '{database_filter}'
        ORDER BY v.date ASC
        """
    else:
        sql = f"""
        SELECT date, ticker_pk, value
        FROM haver_values
        WHERE ticker_pk ILIKE '%%{suffix}%%'
        ORDER BY date ASC
        """
    rows = db._extract_rows(db.send_sql(sql))

    log_event(
        logger,
        "info",
        "Fetched raw rows for processor",
        suffix=suffix,
        database_filter=database_filter or "",
        row_count=len(rows),
    )
    if not rows:
        return pd.DataFrame()

    data = []
    for row in rows:
        if isinstance(row, dict):
            data.append(row)
        elif isinstance(row, (list, tuple)) and len(row) >= 3:
            data.append({"date": row[0], "ticker_pk": row[1], "value": row[2]})

    df = pd.DataFrame(data)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date"])

    duplicate_count = df.duplicated(subset=["date", "ticker_pk"]).sum()
    if duplicate_count:
        log_event(
            logger,
            "warning",
            "Found duplicate date/ticker rows during processing",
            suffix=suffix,
            duplicate_count=duplicate_count,
        )

    pivot_df = (
        df.sort_values(["date", "ticker_pk"])
        .pivot_table(index="date", columns="ticker_pk", values="value", aggfunc="last")
        .sort_index()
    )

    resampled = pivot_df.resample("M").last()
    resampled = resampled.ffill(limit=1)
    log_event(logger, "info", "Prepared resampled processor dataframe", suffix=suffix, row_count=len(resampled))
    return resampled


def fetch_inflation_raw_data():
    """파생 인플레이션 지표 계산에 필요한 CPI/Core CPI 원자료와 메타데이터를 조회합니다."""
    sql = """
    SELECT v.date, v.ticker_pk, v.value, m.descriptor, m.database, m.code, m.geography1
    FROM haver_values v
    JOIN haver_metadata m ON v.ticker_pk = m.ticker_pk
    WHERE LOWER(m.database) IN ('g10', 'emerge')
      AND (
           RIGHT(UPPER(m.code), 2) = 'PC'
        OR RIGHT(UPPER(m.code), 3) = 'PCX'
      )
    ORDER BY v.date ASC, v.ticker_pk ASC
    """
    rows = db._extract_rows(db.send_sql(sql))

    log_event(logger, "info", "Fetched inflation raw rows", row_count=len(rows))
    if not rows:
        return pd.DataFrame()

    data = []
    for row in rows:
        if isinstance(row, dict):
            data.append(row)
        elif isinstance(row, (list, tuple)) and len(row) >= 7:
            data.append(
                {
                    "date": row[0],
                    "ticker_pk": row[1],
                    "value": row[2],
                    "descriptor": row[3],
                    "database": row[4],
                    "code": row[5],
                    "geography1": row[6],
                }
            )

    df = pd.DataFrame(data)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "ticker_pk", "value"])
    if "descriptor" in df.columns:
        df["descriptor"] = df["descriptor"].fillna("").astype(str)
    else:
        df["descriptor"] = ""
    if "database" in df.columns:
        df["database"] = df["database"].fillna("").astype(str)
    else:
        df["database"] = ""
    if "code" in df.columns:
        df["code"] = df["code"].fillna("").astype(str)
    else:
        df["code"] = ""
    if "geography1" in df.columns:
        df["geography1"] = df["geography1"].fillna("").astype(str)
    else:
        df["geography1"] = ""
    return df


def run_processing():
    """전체 파생 지표 계산을 실행하고 결과를 DB에 업로드합니다."""
    stats = {
        "rows_uploaded_di": 0,
        "policy_rate_di_rows": 0,
        "policy_rate_dm_rows": 0,
        "policy_rate_em_rows": 0,
        "policy_rate_diff_rows": 0,
        "inflation_mom_rows": 0,
        "inflation_yoy_rows": 0,
        "inflation_annualized_3m_rows": 0,
        "mfg_pmi_rows": 0,
        "srv_pmi_rows": 0,
    }

    log_event(logger, "info", "Starting data processing")

    log_event(logger, "info", "Processing Policy Rate", suffix="rtar")
    rtar_raw = fetch_raw_data("rtar")
    di_rtar, diff3m_rtar = process_policy_rate(rtar_raw)

    if not di_rtar.empty:
        db.create_table_with_types(di_rtar, "haver_di_policy_rate")
        uploaded = db.upsert_data(di_rtar, "haver_di_policy_rate")
        stats["rows_uploaded_di"] += uploaded
        stats["policy_rate_di_rows"] = uploaded
        log_event(logger, "info", "Uploaded Policy Rate DI", row_count=uploaded)

    if not diff3m_rtar.empty:
        db.create_table_with_types(diff3m_rtar, "haver_diff3m_policy_rate")
        uploaded = db.upsert_data(diff3m_rtar, "haver_diff3m_policy_rate")
        stats["rows_uploaded_di"] += uploaded
        stats["policy_rate_diff_rows"] = uploaded
        log_event(logger, "info", "Uploaded Policy Rate 3M Diff", row_count=uploaded)

    log_event(logger, "info", "Processing Policy Rate DM", suffix="rtar", database_filter="g10")
    rtar_dm_raw = fetch_raw_data("rtar", database_filter="g10")
    di_rtar_dm, _ = process_policy_rate(rtar_dm_raw)

    if not di_rtar_dm.empty:
        db.create_table_with_types(di_rtar_dm, "haver_di_policy_rate_dm")
        uploaded = db.upsert_data(di_rtar_dm, "haver_di_policy_rate_dm")
        stats["rows_uploaded_di"] += uploaded
        stats["policy_rate_dm_rows"] = uploaded
        log_event(logger, "info", "Uploaded Policy Rate DM DI", row_count=uploaded)

    log_event(logger, "info", "Processing Policy Rate EM", suffix="rtar", database_filter="emerge")
    rtar_em_raw = fetch_raw_data("rtar", database_filter="emerge")
    di_rtar_em, _ = process_policy_rate(rtar_em_raw)

    if not di_rtar_em.empty:
        db.create_table_with_types(di_rtar_em, "haver_di_policy_rate_em")
        uploaded = db.upsert_data(di_rtar_em, "haver_di_policy_rate_em")
        stats["rows_uploaded_di"] += uploaded
        stats["policy_rate_em_rows"] = uploaded
        log_event(logger, "info", "Uploaded Policy Rate EM DI", row_count=uploaded)

    log_event(logger, "info", "Processing Inflation Derived Metrics")
    inflation_raw = fetch_inflation_raw_data()
    inflation_frames = build_inflation_metric_frames(inflation_raw)

    inflation_table_map = [
        ("mom", "haver_inflation_mom", "Uploaded Inflation MoM"),
        ("yoy", "haver_inflation_yoy", "Uploaded Inflation YoY"),
        ("annualized_3m", "haver_inflation_3m_annualized", "Uploaded Inflation 3M Annualized"),
    ]

    for metric_key, table_name, log_message in inflation_table_map:
        metric_df = inflation_frames.get(metric_key, pd.DataFrame())
        if metric_df.empty:
            continue

        db.create_table_with_types(metric_df, table_name)
        uploaded = db.upsert_data(metric_df, table_name)
        stats["rows_uploaded_di"] += uploaded
        if metric_key == "mom":
            stats["inflation_mom_rows"] = uploaded
        elif metric_key == "yoy":
            stats["inflation_yoy_rows"] = uploaded
        elif metric_key == "annualized_3m":
            stats["inflation_annualized_3m_rows"] = uploaded
        log_event(logger, "info", log_message, row_count=uploaded)

    log_event(logger, "info", "Processing Manufacturing PMI", suffix="vpmm")
    vpm_raw = fetch_raw_data("vpmm")
    di_vpm = process_pmi(vpm_raw)

    if not di_vpm.empty:
        db.create_table_with_types(di_vpm, "haver_di_mfg_pmi")
        uploaded = db.upsert_data(di_vpm, "haver_di_mfg_pmi")
        stats["rows_uploaded_di"] += uploaded
        stats["mfg_pmi_rows"] = uploaded
        log_event(logger, "info", "Uploaded Manufacturing PMI DI", row_count=uploaded)

    log_event(logger, "info", "Processing Services PMI", suffix="vpms")
    vpms_raw = fetch_raw_data("vpms")
    di_vpms = process_pmi(vpms_raw)

    if not di_vpms.empty:
        db.create_table_with_types(di_vpms, "haver_di_srv_pmi")
        uploaded = db.upsert_data(di_vpms, "haver_di_srv_pmi")
        stats["rows_uploaded_di"] += uploaded
        stats["srv_pmi_rows"] = uploaded
        log_event(logger, "info", "Uploaded Services PMI DI", row_count=uploaded)

    log_event(logger, "info", "Completed data processing", rows_uploaded_di=stats["rows_uploaded_di"])
    return stats


if __name__ == "__main__":
    db.setup_environment()
    run_processing()
