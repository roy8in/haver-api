import pandas as pd
import numpy as np


def process_pmi(df):
    """
    PMI DI 및 3개월 이동평균 계산
    - DI = (값 > 50인 국가 수) / 전체 국가 수
    - DI_3MA = DI의 3개월 이동평균
    """
    if df.empty:
        return pd.DataFrame()

    # 1. 50 초과 여부 확인
    above50 = (df > 50).astype(float)
    # 실제 데이터가 없는 경우(NaN)는 0/1 판단에서 제외하기 위해 다시 NaN 처리
    above50[df.isna()] = np.nan

    # 2. Diffusion Index 계산
    def calculate_di(row):
        valid_row = row.dropna()
        if valid_row.empty:
            return np.nan
        return valid_row.mean()  # (True인 수) / (전체 수) 와 동일

    di_series = above50.apply(calculate_di, axis=1)

    # 3. 3개월 이동평균 (3-Month Moving Average)
    di_3ma = di_series.rolling(window=3).mean()

    # 결과 정리 (Wide-form)
    res_df = pd.DataFrame({
        'date': di_series.index,
        'di': di_series.values,
        'di_3ma': di_3ma.values
    })
    res_df['date'] = res_df['date'].dt.strftime('%Y-%m-%d')
    res_df = res_df.dropna(subset=['di'])

    return res_df
