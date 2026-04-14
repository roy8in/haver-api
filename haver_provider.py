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
        
        # Haver API가 에러 리포트(dict)를 반환하는 경우 (잘못된 티커 포함 시)
        if isinstance(meta_df, dict):
            print(f"⚠️ Metadata fetch failed. Some tickers might be invalid. Run check_tickers.py.")
            return pd.DataFrame()
            
        if meta_df is not None and not meta_df.empty:
            meta_df.columns = [c.lower() for c in meta_df.columns]
            if 'database' in meta_df.columns and 'code' in meta_df.columns:
                meta_df['ticker_pk'] = meta_df['database'] + ':' + meta_df['code']
            return meta_df
        
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Exception in fetch_metadata: {e}")
        return pd.DataFrame()

def fetch_series_data(ticker_chunk, start_date):
    """시계열 데이터 수집"""
    try:
        data = Haver.data(ticker_chunk, startdate=start_date, dates=True)
        return _process_haver_data(data, ticker_chunk)
    except Exception as e:
        # 데이터 수집 실패 시 개별 재시도 로직은 안정성을 위해 유지하거나, 
        # 원하신다면 이 부분도 더 단순화할 수 있습니다. 
        # 현재는 청크 전체 실패 시 개별 조회를 시도하는 로직입니다.
        combined_results = []
        for ticker in ticker_chunk:
            try:
                single_data = Haver.data([ticker], startdate=start_date, dates=True)
                processed = _process_haver_data(single_data, [ticker])
                if not processed.empty:
                    combined_results.append(processed)
            except Exception:
                continue
        
        if combined_results:
            return pd.concat(combined_results, ignore_index=True)
        return pd.DataFrame()

def _process_haver_data(data, ticker_names):
    """Haver 데이터를 Long-form DataFrame으로 변환"""
    if data is None or data.empty:
        return pd.DataFrame()

    data.columns = ticker_names
    long_df = data.reset_index().rename(columns={'index': 'date'})
    long_df = pd.melt(long_df, id_vars=['date'], var_name='ticker_pk', value_name='value')
    
    long_df = long_df.dropna(subset=['value'])
    if not long_df.empty:
        long_df['date'] = pd.to_datetime(long_df['date']).dt.strftime('%Y-%m-%d')
    
    return long_df
