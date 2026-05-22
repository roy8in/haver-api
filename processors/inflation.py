"""CPI/Core CPI 원자료에서 MoM, YoY, 3개월 연율화 지표를 생성합니다."""

import pandas as pd


def _series_family_for_code(code, descriptor=""):
    code_text = str(code).upper().strip()
    descriptor_text = str(descriptor).lower().strip()

    if code_text.endswith("PCX"):
        return "core_cpi"
    if code_text.endswith("PC"):
        return "cpi"
    if "core" in descriptor_text:
        return "core_cpi"
    if "cpi" in descriptor_text:
        return "cpi"
    return "unknown"


def _region_for_ticker(ticker_pk):
    ticker = str(ticker_pk).upper()
    if ticker.endswith("@G10"):
        return "dm"
    if ticker.endswith("@EMERGE"):
        return "em"
    return ""


def _prepare_monthly_series(group):
    cleaned = (
        group.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .copy()
    )
    series = pd.Series(cleaned["value"].values, index=pd.DatetimeIndex(cleaned["date"]))
    series = pd.to_numeric(series, errors="coerce").sort_index()
    return series.resample("M").last().ffill(limit=1)


def _metric_frame_from_series(raw_df, metric_func):
    if raw_df.empty:
        return pd.DataFrame()

    frames = []
    group_cols = ["ticker_pk"]
    if "code" in raw_df.columns:
        group_cols.append("code")
    if "geography1" in raw_df.columns:
        group_cols.append("geography1")
    if "descriptor" in raw_df.columns:
        group_cols.append("descriptor")
    if "database" in raw_df.columns:
        group_cols.append("database")

    for keys, group in raw_df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)

        key_map = dict(zip(group_cols, keys))
        ticker_pk = key_map.get("ticker_pk")
        if not ticker_pk:
            continue

        monthly = _prepare_monthly_series(group)
        metric_series = metric_func(monthly)
        if metric_series.empty:
            continue

        descriptor = key_map.get("descriptor", "")
        database = key_map.get("database", "")
        code = key_map.get("code", "")
        geography1 = key_map.get("geography1", "")
        frame = pd.DataFrame(
            {
                "date": metric_series.index.strftime("%Y-%m-%d"),
                "ticker_pk": ticker_pk,
                "code": code,
                "geography1": geography1,
                "descriptor": descriptor,
                "series_family": _series_family_for_code(code, descriptor),
                "region": _region_for_ticker(ticker_pk) or ("dm" if str(database).lower() == "g10" else "em" if str(database).lower() == "emerge" else ""),
                "value": metric_series.values,
            }
        )
        frame = frame.dropna(subset=["value"])
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(["date", "ticker_pk"]).reset_index(drop=True)
    return result


def build_inflation_metric_frames(raw_df):
    """
    월별 CPI/Core CPI 레벨 원자료에서 파생 인플레이션 지표 테이블을 생성합니다.

    반환값은 지표명(mom, yoy, annualized_3m)을 키로 갖는 데이터프레임 딕셔너리입니다.
    """
    if raw_df.empty:
        return {
            "mom": pd.DataFrame(),
            "yoy": pd.DataFrame(),
            "annualized_3m": pd.DataFrame(),
        }

    cleaned = raw_df.copy()
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
    cleaned["value"] = pd.to_numeric(cleaned["value"], errors="coerce")
    cleaned = cleaned.dropna(subset=["date", "ticker_pk", "value"])
    if "code" not in cleaned.columns:
        cleaned["code"] = ""
    else:
        cleaned["code"] = cleaned["code"].fillna("").astype(str)
    if "geography1" not in cleaned.columns:
        cleaned["geography1"] = ""
    else:
        cleaned["geography1"] = cleaned["geography1"].fillna("").astype(str)
    if "descriptor" not in cleaned.columns:
        cleaned["descriptor"] = ""
    else:
        cleaned["descriptor"] = cleaned["descriptor"].fillna("").astype(str)
    if "database" not in cleaned.columns:
        cleaned["database"] = ""
    else:
        cleaned["database"] = cleaned["database"].fillna("").astype(str)

    def mom(series):
        return series.pct_change(1) * 100

    def yoy(series):
        return series.pct_change(12) * 100

    def annualized_3m(series):
        return ((series / series.shift(3)) ** 4 - 1) * 100

    return {
        "mom": _metric_frame_from_series(cleaned, mom),
        "yoy": _metric_frame_from_series(cleaned, yoy),
        "annualized_3m": _metric_frame_from_series(cleaned, annualized_3m),
    }
