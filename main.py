import pandas as pd
from datetime import datetime, timedelta
import db_handler as db
import haver_provider as haver

def run_sync():
    # 0. 초기화
    db.setup_environment()
    if not haver.initialize():
        print("❌ Haver provider initialization failed. Aborting.")
        return

    # 1. 티커 리스트 로드
    try:
        tickers_csv = pd.read_csv('tickers.csv')
        ticker_list = tickers_csv['ticker'].tolist()
    except Exception as e:
        print(f"❌ Failed to load tickers.csv: {e}")
        return

    # 2. 메타데이터 동기화
    print(f"🔄 Syncing metadata for {len(ticker_list)} tickers...")
    meta_df = haver.fetch_metadata(ticker_list)
    if meta_df.empty:
        print("⚠️ No metadata collected.")
        return
    
    meta_df.columns = [c.lower() for c in meta_df.columns]
    db.create_table_with_types(meta_df, 'haver_metadata')
    db.upsert_data(meta_df, 'haver_metadata')
    print("✅ Metadata synced and uploaded.")

    # 3. DB 현황 파악 (각 티커별 마지막 수집일)
    db_max_dates = db.get_ticker_max_dates()
    
    # 4. 티커별 수집 작업 생성
    end_col = next((c for c in ['enddate', 'end', 'finish', 'last'] if c in meta_df.columns), None)
    start_col = next((c for c in ['startdate', 'start', 'begin'] if c in meta_df.columns), None)
    
    sync_tasks = []
    skipped_up_to_date = 0
    
    for _, row in meta_df.iterrows():
        pk = row['ticker_pk']
        m_start = pd.to_datetime(row[start_col]) if start_col else pd.to_datetime('1900-01-01')
        m_end = pd.to_datetime(row[end_col]) if end_col else datetime.now()
        
        db_last = pd.to_datetime(db_max_dates.get(pk)) if pk in db_max_dates else None
        
        if db_last is None:
            # 신규 티커: 메타데이터 시작일부터 전체 수집
            fetch_start = m_start
        else:
            # 기존 티커: 마지막 날짜 180일 전부터 (Revision 대비)
            fetch_start = db_last - timedelta(days=180)
            if db_last >= m_end:
                skipped_up_to_date += 1
                continue

        sync_tasks.append({
            'pk': pk,
            'freq': row.get('frequency', row.get('freq', 'ALL')),
            'start': fetch_start
        })

    if skipped_up_to_date > 0:
        print(f"ℹ️ {skipped_up_to_date} tickers are already up-to-date. Skipping.")

    # 5. 수집 실행 (최적화: 주기별로 묶고, 청크 내 최소 시작일 사용)
    task_df = pd.DataFrame(sync_tasks)
    if task_df.empty:
        print("✅ Everything is up-to-date. No data to fetch.")
        return

    for freq, group in task_df.groupby('freq'):
        tickers_in_freq = group.to_dict('records')
        total_count = len(tickers_in_freq)
        print(f"🔄 Processing {freq} frequency ({total_count} tickers)...")
        
        chunk_size = 50
        for i in range(0, total_count, chunk_size):
            chunk_tasks = tickers_in_freq[i:i + chunk_size]
            chunk_tickers = [t['pk'] for t in chunk_tasks]
            
            # 이 50개 티커 중 가장 빠른 날짜를 수집 시작일로 결정
            # (API 호출 횟수 최소화 전략)
            min_start = min([t['start'] for t in chunk_tasks]).strftime('%Y-%m-%d')
            
            print(f"   - Fetching chunk {i//chunk_size + 1}/{(total_count-1)//chunk_size + 1} from {min_start}...")
            long_df = haver.fetch_series_data(chunk_tickers, min_start)
            
            if not long_df.empty:
                db.create_table_with_types(long_df, 'haver_values')
                db.upsert_data(long_df, 'haver_values')
                print(f"     ✅ Uploaded {len(long_df)} rows.")
            else:
                print(f"     ⚠️ No data fetched for this chunk.")

if __name__ == "__main__":
    run_sync()
