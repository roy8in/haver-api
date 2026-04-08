import Haver
import pandas as pd
from datetime import datetime, timedelta

def initialize():
    """Haver 초기화 (웹 버전용)"""
    try:
        Haver.direct(1)
        return True
    except Exception as e:
        print(f"⚠️ Haver Initialization Error: {e}")
        return False

def fetch_metadata(ticker_list):
    """티커 리스트에 대한 메타데이터 수집"""
    try:
        meta_df = Haver.metadata(ticker_list)
        if not meta_df.empty:
            meta_df['ticker_pk'] = meta_df['database'] + ':' + meta_df['code']
        return meta_df
    except Exception as e:
        print(f"❌ Error fetching metadata: {e}")
        return pd.DataFrame()

def fetch_series_data(ticker_chunk, start_date):
    """시계열 데이터 수집 (에러 발생 시 개별 재시도 로직 포함)"""
    try:
        # 1. 청크 단위 수집 시도 (속도 최적화)
        data = Haver.data(ticker_chunk, startdate=start_date, dates=True)
        return _process_haver_data(data, ticker_chunk)
    
    except Exception as e:
        # 2. 에러 발생 시 개별 티커별로 재시도 (안정성 확보)
        # 50개 중 문제 있는 티커만 골라내고 나머지는 모두 수집
        combined_results = []
        
        for ticker in ticker_chunk:
            try:
                single_data = Haver.data([ticker], startdate=start_date, dates=True)
                processed = _process_haver_data(single_data, [ticker])
                if not processed.empty:
                    combined_results.append(processed)
            except Exception:
                # 데이터가 없는 티커 등은 로그 없이 스킵 (개별 에러는 무시)
                continue
        
        if combined_results:
            # 성공한 데이터들만 합쳐서 반환
            return pd.concat(combined_results, ignore_index=True)
        return pd.DataFrame()

def _process_haver_data(data, ticker_names):
    """Haver 데이터를 Long-form DataFrame으로 변환"""
    if data is None or data.empty:
        return pd.DataFrame()

    # 컬럼 정규화 (전달받은 티커 리스트와 결과 컬럼 매칭)
    # Haver.data 결과의 컬럼 순서가 입력 리스트와 동일하다고 가정
    data.columns = ticker_names
    
    long_df = data.reset_index().rename(columns={'index': 'date'})
    long_df = pd.melt(long_df, id_vars=['date'], var_name='ticker_pk', value_name='value')
    
    # 결측치 제거 및 날짜 형식 표준화
    long_df = long_df.dropna(subset=['value'])
    if not long_df.empty:
        long_df['date'] = pd.to_datetime(long_df['date']).dt.strftime('%Y-%m-%d')
    
    return long_df
