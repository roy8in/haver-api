import pandas as pd

from app import db_handler as db
from app.processors.policy_rate import process_policy_rate
from app.processors.pmi import process_pmi
from app.run_logging import get_logger, log_event


logger = get_logger("processor")


def fetch_raw_data(suffix):
    """
    Fetch ticker data for tickers whose PK contains the requested suffix.
    Returns a wide dataframe indexed by date.
    """
    sql = f"""
    SELECT date, ticker_pk, value
    FROM haver_values
    WHERE ticker_pk ILIKE '%%{suffix}%%'
    ORDER BY date ASC
    """
    rows = db._extract_rows(db.send_sql(sql))

    log_event(logger, "info", "Fetched raw rows for processor", suffix=suffix, row_count=len(rows))
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


def run_processing():
    """Run derived indicator processing and upload results."""
    stats = {
        "rows_uploaded_di": 0,
        "policy_rate_di_rows": 0,
        "policy_rate_diff_rows": 0,
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
