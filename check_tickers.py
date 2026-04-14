import pandas as pd
import Haver
import haver_provider as haver
import os

def validate_tickers_internal(ticker_list):
    """
    Haver API를 직접 호출하여 티커의 유효성을 검증합니다.
    """
    try:
        # Haver.metadata는 문제 발생 시 에러 리포트(dict)를 반환함
        result = Haver.metadata(ticker_list)
        
        if isinstance(result, dict):
            codelists = result.get('codelists', {})
            valid = codelists.get('codesfound', [])
            invalid = codelists.get('codesnotfound', [])
            return valid, invalid
        
        if result is not None and not result.empty:
            return ticker_list, []
            
        return [], ticker_list
    except Exception as e:
        print(f"❌ API Error during validation: {e}")
        return [], ticker_list

def run_validation():
    print("="*50)
    print("🔍 Haver Ticker Validation Tool")
    print("="*50)

    # 1. Haver 초기화
    if not haver.initialize():
        print("❌ Haver initialization failed.")
        return

    # 2. tickers.csv 로드
    csv_file = 'tickers.csv'
    if not os.path.exists(csv_file):
        print(f"❌ Error: {csv_file} not found.")
        return

    try:
        df = pd.read_csv(csv_file)
        raw_list = df.iloc[:, 0].dropna().unique().tolist()
        print(f"📂 Loaded {len(raw_list)} unique tickers from {csv_file}")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    # 3. 유효성 검증 (내부 함수 호출)
    print("🔄 Checking with Haver API (DLX Direct)...")
    valid, invalid = validate_tickers_internal(raw_list)

    # 4. 결과 출력
    print("\n" + "-"*30)
    print(f"✅ Valid Tickers: {len(valid)}")
    print(f"❌ Invalid Tickers: {len(invalid)}")
    print("-"*30)

    if invalid:
        print("\n🚨 [ACTION REQUIRED] Please fix or remove these tickers in tickers.csv:")
        for t in invalid:
            print(f"   - {t}")
        
        try:
            with open('invalid_tickers.txt', 'w') as f:
                f.write('\n'.join(invalid))
            print(f"\n📝 List saved to 'invalid_tickers.txt'")
        except Exception:
            pass
    else:
        print("\n✨ All tickers are valid! Ready to run main.py or test_tickers.py.")
        if os.path.exists('invalid_tickers.txt'):
            os.remove('invalid_tickers.txt')

    print("\n" + "="*50)

if __name__ == "__main__":
    run_validation()
