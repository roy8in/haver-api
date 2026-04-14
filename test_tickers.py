import pandas as pd
from datetime import datetime, timedelta
import haver_provider as haver

def test_to_excel():
    # 1. Haver 초기화
    print("🔄 Initializing Haver...")
    if not haver.initialize():
        print("❌ Haver initialization failed.")
        return

    # 2. tickers.csv 로드 (전체 티커)
    try:
        tickers_df = pd.read_csv('tickers.csv')
        # CSV의 모든 티커를 수집 대상으로 삼음
        ticker_list = tickers_df.iloc[:, 0].dropna().unique().tolist()
        print(f"✅ Loaded {len(ticker_list)} unique tickers from tickers.csv")
    except Exception as e:
        print(f"❌ Failed to load tickers.csv: {e}")
        return

    # 3. 메타데이터(Metadata) 전체 조회
    print(f"🔄 Fetching metadata for {len(ticker_list)} tickers...")
    try:
        meta_df = haver.fetch_metadata(ticker_list)
        if meta_df.empty:
            print("⚠️ Metadata collection failed. Please check tickers with check_tickers.py.")
            return
        print(f"📊 Metadata collected for {len(meta_df)} tickers.")
    except Exception as e:
        print(f"❌ Error during metadata fetch: {e}")
        return

    # 4. 최근 1년 시계열 데이터(Series Data) 조회
    print("\n--- [Series Data Fetch (Last 365 Days)] ---")
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    all_series_data = []
    
    chunk_size = 50
    for i in range(0, len(ticker_list), chunk_size):
        chunk = ticker_list[i:i + chunk_size]
        print(f"   - Fetching chunk {i//chunk_size + 1}/{(len(ticker_list)-1)//chunk_size + 1}...")
        try:
            chunk_data = haver.fetch_series_data(chunk, one_year_ago)
            if not chunk_data.empty:
                all_series_data.append(chunk_data)
        except Exception as e:
            print(f"     ⚠️ Error in chunk {i//chunk_size + 1}: {e}")

    if not all_series_data:
        print("⚠️ No series data collected.")
        full_series_df = pd.DataFrame()
    else:
        full_series_df = pd.concat(all_series_data, ignore_index=True)
        print(f"📈 Total {len(full_series_df)} data points collected.")

    # 5. 엑셀 저장 (주기별 시트 분리)
    output_file = 'haver_full_test_results.xlsx'
    print(f"\n💾 Saving results to {output_file}...")
    
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 시트 1: 전체 메타데이터
            meta_df.to_excel(writer, sheet_name='All_Metadata', index=False)
            
            # 시트 2~N: 주기별 데이터 시트
            if not full_series_df.empty:
                freq_col = next((c for c in ['frequency', 'freq'] if c in meta_df.columns), None)
                if freq_col:
                    merged_df = pd.merge(full_series_df, meta_df[['ticker_pk', freq_col]], on='ticker_pk', how='left')
                    merged_df[freq_col] = merged_df[freq_col].fillna('Unknown')
                    
                    for freq, group in merged_df.groupby(freq_col):
                        sheet_name = f"Data_{freq}"[:31]
                        group.drop(columns=[freq_col]).to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"   ✅ Created sheet: {sheet_name}")
                else:
                    full_series_df.to_excel(writer, sheet_name='All_Data', index=False)

        print(f"\n✨ Test Completed! File saved as '{output_file}'")
        
    except Exception as e:
        print(f"❌ Failed to save Excel file: {e}")

if __name__ == "__main__":
    test_to_excel()
