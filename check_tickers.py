"""tickers.csv의 Haver 티커 유효성을 점검하는 보조 실행 스크립트입니다."""

import os

import Haver
import pandas as pd

import haver_provider as haver


def validate_tickers_internal(ticker_list):
    """Haver API를 직접 호출해 티커 목록의 유효성을 확인합니다."""
    try:
        # Haver.metadata는 실패 시 에러 리포트(dict)를 반환할 수 있습니다.
        result = Haver.metadata(ticker_list)

        if isinstance(result, dict):
            codelists = result.get("codelists", {})
            valid = codelists.get("codesfound", [])
            invalid = codelists.get("codesnotfound", [])
            return valid, invalid

        if result is not None and not result.empty:
            return ticker_list, []

        if len(ticker_list) > 1:
            valid = []
            invalid = []
            for ticker in ticker_list:
                try:
                    single_result = Haver.metadata([ticker])
                except Exception:
                    invalid.append(ticker)
                    continue

                if isinstance(single_result, dict):
                    invalid.append(ticker)
                elif single_result is not None and not single_result.empty:
                    valid.append(ticker)
                else:
                    invalid.append(ticker)
            return valid, invalid

        return [], ticker_list
    except Exception as e:
        print(f"API Error during validation: {e}")
        return [], ticker_list


def run_validation():
    """CSV 티커를 읽어 Haver 유효성 검사 결과를 콘솔에 출력합니다."""
    print("=" * 50)
    print("Haver Ticker Validation Tool")
    print("=" * 50)

    # Haver 클라이언트를 먼저 초기화합니다.
    if not haver.initialize():
        print("Haver initialization failed.")
        return

    # tickers.csv에서 중복을 제거한 티커 목록을 읽습니다.
    csv_file = "tickers.csv"
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found.")
        return

    try:
        df = pd.read_csv(csv_file)
        raw_list = df.iloc[:, 0].dropna().unique().tolist()
        print(f"Loaded {len(raw_list)} unique tickers from {csv_file}")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Haver API로 각 티커가 조회 가능한지 확인합니다.
    print("Checking with Haver API (DLX Direct)...")
    valid, invalid = validate_tickers_internal(raw_list)

    # 검증 결과를 요약하고, 실패 목록은 파일로 남깁니다.
    print("\n" + "-" * 30)
    print(f"Valid Tickers: {len(valid)}")
    print(f"Invalid Tickers: {len(invalid)}")
    print("-" * 30)

    if invalid:
        print("\nPlease fix or remove these tickers in tickers.csv:")
        for ticker in invalid:
            print(f"   - {ticker}")

        try:
            with open("invalid_tickers.txt", "w", encoding="utf-8") as handle:
                handle.write("\n".join(invalid))
            print("\nList saved to 'invalid_tickers.txt'")
        except Exception:
            pass
    else:
        print("\nAll tickers are valid! Ready to run main.py.")
        if os.path.exists("invalid_tickers.txt"):
            os.remove("invalid_tickers.txt")

    print("\n" + "=" * 50)


if __name__ == "__main__":
    run_validation()
