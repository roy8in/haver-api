import pandas as pd
import numpy as np


def process_policy_rate(df):
    """
    기준금리(Policy Rate) DI 및 3개월 변화분 계산
    - DI = (상승 국가 수 - 하락 국가 수) / 전체 국가 수
    - Diff3M = 현재 금리 - 3개월 전 금리
    """
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # 1. 3개월 변화분 (개별 국가)
    diff3m = df.diff(3)

    # 2. Diffusion Index 계산
    def calculate_di(row):
        valid_row = row.dropna()
        if valid_row.empty:
            return np.nan
        up = (valid_row > 0).sum()
        down = (valid_row < 0).sum()
        return (up - down) / len(valid_row)

    di_series = diff3m.apply(calculate_di, axis=1)

    # 결과 정리 (Wide-form for DI)
    di_df = di_series.to_frame(name='di').reset_index()
    di_df['date'] = di_df['date'].dt.strftime('%Y-%m-%d')
    di_df = di_df.dropna(subset=['di'])

    # 결과 정리 (Long-form for Diff3M)
    diff3m_long = (
        diff3m.reset_index()
        .melt(id_vars='date', var_name='ticker_pk', value_name='value')
    )
    diff3m_long['date'] = diff3m_long['date'].dt.strftime('%Y-%m-%d')
    diff3m_long = diff3m_long.dropna(subset=['value'])

    return di_df, diff3m_long
