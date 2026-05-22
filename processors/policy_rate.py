"""정책금리 원자료에서 DI와 3개월 변화폭 테이블을 생성합니다."""

import numpy as np
import pandas as pd


def process_policy_rate(df):
    """
    정책금리의 확산지수와 국가별 3개월 변화폭을 계산합니다.

    DI = (상승 국가 수 - 하락 국가 수) / 전체 유효 국가 수
    Diff3M = 현재 금리 - 3개월 전 금리
    """
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # 국가별 3개월 전 대비 변화폭을 먼저 계산합니다.
    diff3m = df.diff(3)

    # 변화폭의 방향을 이용해 정책금리 확산지수를 계산합니다.
    def calculate_di(row):
        valid_row = row.dropna()
        if valid_row.empty:
            return np.nan
        up = (valid_row > 0).sum()
        down = (valid_row < 0).sum()
        return (up - down) / len(valid_row)

    di_series = diff3m.apply(calculate_di, axis=1)

    # 전체 DI는 날짜별 wide 형태로 저장합니다.
    di_df = di_series.to_frame(name="di").reset_index()
    di_df["date"] = di_df["date"].dt.strftime("%Y-%m-%d")
    di_df = di_df.dropna(subset=["di"])

    # 국가별 변화폭은 ticker_pk를 가진 long 형태로 저장합니다.
    diff3m_long = (
        diff3m.reset_index()
        .melt(id_vars="date", var_name="ticker_pk", value_name="value")
    )
    diff3m_long["date"] = diff3m_long["date"].dt.strftime("%Y-%m-%d")
    diff3m_long = diff3m_long.dropna(subset=["value"])

    return di_df, diff3m_long
