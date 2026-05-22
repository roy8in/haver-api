"""PMI 원자료에서 50 초과 비율 기반 DI와 3개월 이동평균을 생성합니다."""

import numpy as np
import pandas as pd


def process_pmi(df):
    """
    PMI 확산지수와 3개월 이동평균을 계산합니다.

    DI = 50을 초과한 국가 수 / 전체 유효 국가 수
    DI_3MA = DI의 3개월 이동평균
    """
    if df.empty:
        return pd.DataFrame()

    # 50 초과 여부를 1과 0으로 변환하되, 원자료 결측치는 제외합니다.
    above50 = (df > 50).astype(float)
    above50[df.isna()] = np.nan

    # 국가별 50 초과 비율을 날짜별 확산지수로 사용합니다.
    def calculate_di(row):
        valid_row = row.dropna()
        if valid_row.empty:
            return np.nan
        return valid_row.mean()

    di_series = above50.apply(calculate_di, axis=1)

    # 단기 노이즈를 줄이기 위해 3개월 이동평균을 함께 제공합니다.
    di_3ma = di_series.rolling(window=3).mean()

    # DB 업로드에 맞는 날짜 컬럼 중심의 테이블로 정리합니다.
    res_df = pd.DataFrame({
        "date": di_series.index,
        "di": di_series.values,
        "di_3ma": di_3ma.values,
    })
    res_df["date"] = res_df["date"].dt.strftime("%Y-%m-%d")
    res_df = res_df.dropna(subset=["di"])

    return res_df
