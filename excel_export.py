"""수집한 Haver 시계열과 메타데이터를 주기별 Excel 워크북으로 내보냅니다."""

import os
from pathlib import Path
import re

import pandas as pd

from run_logging import get_logger, log_event


logger = get_logger("excel_export")

FREQ_COLUMN_CANDIDATES = ("frequency", "freq")
METADATA_SHEET_NAME = "Metadata"
NAME_COLUMN_CANDIDATES = (
    "descriptor",
    "name",
    "title",
    "series_name",
    "seriesname",
    "description",
    "long_name",
    "fullname",
    "label",
    "code",
)
HEADER_COLUMN_CANDIDATES = ("descriptor",) + NAME_COLUMN_CANDIDATES


def _pick_first_existing_column(df, candidates):
    for column in candidates:
        if column in df.columns:
            return column
    return None


def _normalize_label(value, fallback):
    if pd.isna(value):
        return fallback

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return fallback
    return text


def _metadata_value_map(meta_df, value_col, freq_value=None, freq_col=None):
    if meta_df.empty or value_col is None:
        return {}

    filtered = meta_df
    if freq_col and freq_col in meta_df.columns and freq_value is not None:
        filtered = meta_df[meta_df[freq_col].map(_normalize_freq_value) == _normalize_freq_value(freq_value)]

    if "ticker_pk" not in filtered.columns:
        return {}

    value_map = {}
    for _, row in filtered.iterrows():
        ticker = _normalize_label(row.get("ticker_pk"), "")
        if not ticker:
            continue
        value_map[ticker] = _normalize_label(row.get(value_col), ticker)

    return value_map


def _sanitize_sheet_name(name, used_names=None):
    clean_name = re.sub(r"[\[\]\:\*\?\/\\]", "_", str(name)).strip()
    clean_name = clean_name[:31] or "Sheet"

    if used_names is None:
        return clean_name

    candidate = clean_name
    counter = 1
    while candidate in used_names:
        suffix = f"_{counter}"
        candidate = f"{clean_name[:31 - len(suffix)]}{suffix}"
        counter += 1

    used_names.add(candidate)
    return candidate


def _normalize_freq_value(value):
    if pd.isna(value) or str(value).strip() == "":
        return "ALL"
    return str(value).strip()


def _normalize_frequency_date(date_series, freq_value):
    freq_label = _normalize_freq_value(freq_value).upper()
    dates = pd.to_datetime(date_series, errors="coerce")

    if freq_label == "M":
        return dates.dt.to_period("M").dt.to_timestamp("M").dt.date
    if freq_label == "Q":
        return dates.dt.to_period("Q").dt.to_timestamp("Q").dt.date
    if freq_label in {"A", "Y", "ANNUAL", "YEARLY"}:
        return dates.dt.to_period("Y").dt.to_timestamp("Y").dt.date
    return dates.dt.date


def _current_tickers_by_frequency(meta_df):
    if meta_df is None or meta_df.empty or "ticker_pk" not in meta_df.columns:
        return {}

    freq_col = _pick_first_existing_column(meta_df, FREQ_COLUMN_CANDIDATES)
    grouped = {}

    if freq_col and freq_col in meta_df.columns:
        for freq_value, group in meta_df.groupby(meta_df[freq_col].map(_normalize_freq_value), dropna=False):
            grouped[_normalize_freq_value(freq_value)] = [
                str(value).strip()
                for value in group["ticker_pk"].dropna().astype(str).tolist()
                if str(value).strip()
            ]
    else:
        grouped["ALL"] = [
            str(value).strip()
            for value in meta_df["ticker_pk"].dropna().astype(str).tolist()
            if str(value).strip()
        ]

    return grouped


def expected_frequency_sheets(meta_df):
    if meta_df is None or meta_df.empty:
        return []

    freq_col = _pick_first_existing_column(meta_df, FREQ_COLUMN_CANDIDATES)
    if freq_col is None:
        return ["ALL"]

    freqs = [_normalize_freq_value(value) for value in meta_df[freq_col].dropna().unique()]
    return sorted(set(freqs))


def get_missing_frequency_sheets(meta_df, output_path):
    output_path = Path(output_path)
    expected_sheets = set(expected_frequency_sheets(meta_df))
    if not expected_sheets:
        return []
    if not output_path.exists():
        return sorted(expected_sheets)

    try:
        with pd.ExcelFile(output_path) as workbook:
            existing_sheets = set(workbook.sheet_names)
            if _workbook_uses_legacy_ticker_headers(workbook, meta_df):
                return ["header_format"]
    except Exception as exc:
        log_event(logger, "warning", "Unable to inspect existing Excel export", output_path=str(output_path), error=str(exc))
        return sorted(expected_sheets)

    return sorted(expected_sheets.difference(existing_sheets))


def _workbook_uses_legacy_ticker_headers(workbook, meta_df):
    if meta_df is None or meta_df.empty or "ticker_pk" not in meta_df.columns:
        return False

    ticker_values = set(meta_df["ticker_pk"].dropna().astype(str))
    for sheet_name in workbook.sheet_names:
        if sheet_name == METADATA_SHEET_NAME:
            continue
        try:
            frame = pd.read_excel(workbook, sheet_name=sheet_name, header=[0, 1], index_col=0, nrows=0)
        except Exception:
            continue
        first_level_values = {str(col[0]) for col in frame.columns if isinstance(col, tuple)}
        if first_level_values.intersection(ticker_values):
            return True

    return False


def _build_frequency_frames(meta_df, series_frames_by_freq):
    if not series_frames_by_freq:
        return {}

    meta_df = meta_df if meta_df is not None else pd.DataFrame()
    freq_col = _pick_first_existing_column(meta_df, FREQ_COLUMN_CANDIDATES) if not meta_df.empty else None
    sheet_frames = {}

    for freq_value, frame_list in series_frames_by_freq.items():
        if not frame_list:
            continue

        combined = pd.concat(frame_list, ignore_index=True)
        if combined.empty:
            continue

        required_columns = {"date", "ticker_pk", "value"}
        if not required_columns.issubset(combined.columns):
            log_event(
                logger,
                "warning",
                "Skipping Excel export frame with missing columns",
                frequency=str(freq_value),
                columns=list(combined.columns),
            )
            continue

        combined = combined.copy()
        combined["date"] = _normalize_frequency_date(combined["date"], freq_value)
        combined = combined.dropna(subset=["date", "ticker_pk"])
        if combined.empty:
            continue

        if freq_col and freq_col in meta_df.columns:
            freq_subset = meta_df[meta_df[freq_col].map(_normalize_freq_value) == _normalize_freq_value(freq_value)]
        else:
            freq_subset = meta_df

        descriptor_col = _pick_first_existing_column(meta_df, HEADER_COLUMN_CANDIDATES)
        descriptor_map = _metadata_value_map(meta_df, descriptor_col, freq_value=freq_value, freq_col=freq_col)

        pivot_df = (
            combined.sort_values(["date", "ticker_pk"])
            .pivot_table(index="date", columns="ticker_pk", values="value", aggfunc="last")
            .sort_index()
        )

        if pivot_df.empty:
            continue

        ordered_tickers = [
            ticker
            for ticker in freq_subset.get("ticker_pk", pd.Series(dtype=str)).astype(str).tolist()
            if ticker in pivot_df.columns
        ]
        if not ordered_tickers:
            ordered_tickers = list(pivot_df.columns)

        pivot_df = pivot_df.reindex(columns=ordered_tickers)
        pivot_df.columns = pd.MultiIndex.from_tuples(
            [(descriptor_map.get(ticker, ticker), ticker) for ticker in pivot_df.columns],
            names=["descriptor", "ticker_pk"],
        )
        pivot_df.index.name = "date"
        sheet_frames[_normalize_freq_value(freq_value)] = pivot_df

    return sheet_frames


def _prune_existing_frames_to_current_metadata(existing_frames, meta_df):
    if not existing_frames:
        return {}

    allowed_tickers_by_freq = _current_tickers_by_frequency(meta_df)
    if not allowed_tickers_by_freq:
        return existing_frames

    pruned_frames = {}
    for sheet_name, frame in existing_frames.items():
        freq_label = _normalize_freq_value(sheet_name)
        allowed_tickers = allowed_tickers_by_freq.get(freq_label)
        if allowed_tickers is None:
            continue

        if isinstance(frame.columns, pd.MultiIndex):
            available_tickers = [ticker for ticker in frame.columns.get_level_values(-1).astype(str)]
            keep_columns = [column for column in frame.columns if str(column[-1]) in allowed_tickers]
            if not keep_columns:
                continue
            pruned = frame.loc[:, keep_columns]
        else:
            keep_columns = [column for column in frame.columns if str(column) in allowed_tickers]
            if not keep_columns:
                continue
            pruned = frame.loc[:, keep_columns]

        pruned_frames[freq_label] = pruned

    return pruned_frames


def _read_existing_sheet_frames(output_path):
    output_path = Path(output_path)
    if not output_path.exists():
        return {}

    sheet_frames = {}
    try:
        with pd.ExcelFile(output_path) as workbook:
            for sheet_name in workbook.sheet_names:
                if sheet_name == METADATA_SHEET_NAME:
                    continue
                try:
                    frame = pd.read_excel(workbook, sheet_name=sheet_name, header=[0, 1], index_col=0)
                except Exception as exc:
                    log_event(logger, "warning", "Unable to read existing Excel sheet", output_path=str(output_path), sheet_name=sheet_name, error=str(exc))
                    continue

                if frame.empty:
                    continue

                frame.index = pd.to_datetime(frame.index, errors="coerce")
                frame = frame[~frame.index.isna()]
                frame.index = frame.index.date
                frame.index.name = "date"
                sheet_frames[sheet_name] = frame
    except Exception as exc:
        log_event(logger, "warning", "Unable to read existing Excel export", output_path=str(output_path), error=str(exc))
        return {}

    return sheet_frames


def _merge_sheet_frames(existing_frames, update_frames):
    merged_frames = dict(existing_frames)

    for freq_value, update_frame in update_frames.items():
        if update_frame.empty:
            continue

        freq_label = _normalize_freq_value(freq_value)
        existing_frame = merged_frames.get(freq_label)
        if existing_frame is None or existing_frame.empty:
            merged_frames[freq_label] = update_frame
            continue

        combined = update_frame.combine_first(existing_frame).sort_index()
        combined = combined.reindex(sorted(combined.columns), axis=1)
        combined.index.name = "date"
        merged_frames[freq_label] = combined

    return merged_frames


def export_series_workbook(meta_df, series_frames_by_freq, output_path, merge_existing=True):
    """Haver 시계열 데이터를 주기별 시트가 있는 워크북으로 저장합니다."""
    output_path = Path(output_path)
    sheet_frames = _build_frequency_frames(meta_df, series_frames_by_freq)
    if merge_existing:
        existing_frames = _read_existing_sheet_frames(output_path)
        existing_frames = _prune_existing_frames_to_current_metadata(existing_frames, meta_df)
        sheet_frames = _merge_sheet_frames(existing_frames, sheet_frames)

    if not sheet_frames:
        log_event(logger, "warning", "No series frames available for Excel export", output_path=str(output_path))
        return {
            "written": False,
            "sheet_count": 0,
            "output_path": str(output_path),
        }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output_path = output_path.with_name(f"{output_path.name}.tmp")
    if output_path.exists():
        log_event(logger, "info", "Overwriting existing Excel export file", output_path=str(output_path))
    used_sheet_names = set()

    with pd.ExcelWriter(temp_output_path, engine="openpyxl") as writer:
        if meta_df is not None and not meta_df.empty:
            meta_sheet = meta_df.copy()
            meta_sheet.to_excel(writer, sheet_name=_sanitize_sheet_name(METADATA_SHEET_NAME, used_sheet_names), index=False)

        for freq_value, frame in sheet_frames.items():
            sheet_name = _sanitize_sheet_name(freq_value, used_sheet_names)
            frame.to_excel(writer, sheet_name=sheet_name)

    os.replace(temp_output_path, output_path)

    log_event(
        logger,
        "info",
        "Excel export complete",
        output_path=str(output_path),
        sheet_count=len(sheet_frames),
    )
    return {
        "written": True,
        "sheet_count": len(sheet_frames),
        "output_path": str(output_path),
    }
