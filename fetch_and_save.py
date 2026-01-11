
import pandas as pd
import json
import os
import time
import pandas as pd
import json
import os
import time
# utils에 있는 강력한 병렬 처리 함수 가져오기
from utils import get_tickers_from_excel, get_market_cap_and_rs, get_tickers_from_google_sheet

# 설정
# 기존 엑셀 대신 구글 시트 사용
GOOGLE_SHEET_ID = "17JU4KoC-Out5NqGy3qtN7LSunMUsH5xS2qJSk1fBDGQ"
GOOGLE_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv"

OUTPUT_FILE = "static/result.json"

def main():
    if not os.path.exists('static'):
        os.makedirs('static')

    print(f"[{time.strftime('%X')}] 구글 시트 데이터 로드 중...")
    
    # 1. 구글 시트에서 가져오기
    ticker_info_list = get_tickers_from_google_sheet(GOOGLE_SHEET_URL)
    
    # 2. 실패 시 Fallback
    if not ticker_info_list:
        print("⚠️ 구글 시트 로드 실패 (Fallback 모드)")
        # 주요 나스닥/S&P500 티커 Fallback
        ticker_info_list = [
            {"Ticker": "AAPL", "Sector": "Technology", "Industry": "Consumer Electronics"},
            {"Ticker": "MSFT", "Sector": "Technology", "Industry": "Software - Infrastructure"},
            {"Ticker": "NVDA", "Sector": "Technology", "Industry": "Semiconductors"},
            {"Ticker": "GOOGL", "Sector": "Communication Services", "Industry": "Internet Content & Information"},
            {"Ticker": "AMZN", "Sector": "Consumer Cyclical", "Industry": "Internet Retail"},
            {"Ticker": "TSLA", "Sector": "Consumer Cyclical", "Industry": "Auto Manufacturers"},
            {"Ticker": "META", "Sector": "Communication Services", "Industry": "Internet Content & Information"},
            {"Ticker": "AMD", "Sector": "Technology", "Industry": "Semiconductors"},
            {"Ticker": "NFLX", "Sector": "Communication Services", "Industry": "Entertainment"},
            {"Ticker": "PLTR", "Sector": "Technology", "Industry": "Software - Infrastructure"}
        ]
    else:
        print(f"[{time.strftime('%X')}] 대상 티커: {len(ticker_info_list)}개")
    
    start_time = time.time()
    
    # 병렬 처리 함수 실행 (20개씩 동시 작업)
    try:
        results = get_market_cap_and_rs(ticker_info_list)
    except Exception as e:
        print(f"수집 중 에러 발생: {e}")
        results = []
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"[{time.strftime('%X')}] 수집 완료! 소요 시간: {duration:.1f}초, 성공: {len(results)}개")

    output_data = {
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "total_count": len(results),
        "data": results
    }
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
        
    print(f"결과 파일 저장 완료: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
