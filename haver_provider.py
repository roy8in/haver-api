import pandas as pd
import Haver

from run_logging import get_logger, log_event


logger = get_logger("haver")


def initialize():
    """Initialize the Haver client."""
    try:
        Haver.direct(1)
        log_event(logger, "info", "Initialized Haver client")
        return True
    except Exception as e:
        log_event(logger, "error", "Haver initialization error", error=str(e))
        return False


def fetch_metadata(ticker_list):
    """Fetch metadata for the requested ticker list."""
    log_event(logger, "info", "Fetching metadata", ticker_count=len(ticker_list))
    try:
        meta_df = Haver.metadata(ticker_list)
        if isinstance(meta_df, dict):
            log_event(logger, "error", "Metadata fetch returned error payload")
            return pd.DataFrame()
        if not isinstance(meta_df, pd.DataFrame) or meta_df.empty:
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
    """Fetch time-series data, falling back to per-ticker requests on chunk failure."""
    log_event(logger, "info", "Fetching series chunk", ticker_count=len(ticker_chunk), start_date=start_date)
    try:
        data = Haver.data(ticker_chunk, startdate=start_date, dates=True)
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
            single_data = Haver.data([ticker], startdate=start_date, dates=True)
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
    """Convert Haver data into a long-form dataframe."""
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
