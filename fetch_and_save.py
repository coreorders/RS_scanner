
import pandas as pd
import yfinance as yf
import json
import os
import time
# requests 관련 import 제거 (yfinance 내부 사용)
from utils import get_tickers_from_excel

# 설정
SOURCE_EXCEL_FILE = "RS분석툴.xlsm"
OUTPUT_FILE = "static/result.json"

# 전역 로그 저장소
DEBUG_LOGS = []

def log(msg):
    print(msg)
    DEBUG_LOGS.append(msg)

def get_market_cap_and_rs(ticker_info_list):
    tickers = [item['Ticker'] for item in ticker_info_list]
    info_map = {item['Ticker']: item for item in ticker_info_list}
    
    log(f"[{time.strftime('%X')}] 총 {len(tickers)}개 티커 데이터 수집 시작...")
    
    # 1. Price Bulk Download
    search_tickers = tickers + ["QQQ"]
    
    try:
        # User-Agent 설정 없이 yfinance 내부 로직에 맡김 (Anti-blocking 자동 처리)
        data = yf.download(search_tickers, period="6mo", progress=True, threads=True)
    except Exception as e:
        log(f"다운로드 중 치명적 에러: {e}")
        return []

    if data.empty:
        log("yf.download 결과가 비어있습니다. (IP 차단 또는 티커 오류 가능성)")
        return []
    
    log(f"다운로드 완료. 데이터 Shape: {data.shape}, Columns: {list(data.columns)[:5]}...")
    
    if 'Adj Close' in data.columns:
        closes = data['Adj Close']
    elif 'Close' in data.columns:
        closes = data['Close']
    else:
        # MultiIndex 처리 (yfinance 최신 버전)
        # 만약 컬럼이 (Price, Ticker) 형태라면
        try:
             closes = data.xs('Adj Close', axis=1, level=0)
        except:
             try:
                 closes = data.xs('Close', axis=1, level=0)
             except:
                 closes = data
                 log("컬럼 구조 인식 불가. Raw data 사용.")

    if len(closes) < 61:
        log(f"데이터 행 수 부족: {len(closes)} (최소 61 필요)")
        return []

    latest = closes.iloc[-1]
    prev_60 = closes.iloc[-61]

    # QQQ
    q_cur = latest.get("QQQ", 0)
    q_prev = prev_60.get("QQQ", 0)
    q_chg = (q_cur / q_prev) if q_prev != 0 else 0
    log(f"QQQ 변동률 계산: {q_chg:.4f}")

    results = []
    
    for t in tickers:
        try:
            base = info_map.get(t, {})
            cur = latest.get(t, None)
            prev = prev_60.get(t, None)
            
            if pd.isna(cur) or pd.isna(prev) or prev == 0:
                continue
            
            chg = cur / prev
            rs = (chg / q_chg) - 1 if q_chg != 0 else 0
            
            try:
                stock = yf.Ticker(t)
                shares = stock.fast_info.get('shares', 0)
            except:
                shares = 0
                
            mcap = cur * shares
            
            results.append({
                "Ticker": t,
                "Price": round(float(cur), 2),
                "RS": round(float(rs), 4),
                "MarketCap": round(float(mcap), 0),
                "Shares": shares,
                "Sector": base.get("Sector", ""),
                "Industry": base.get("Industry", "")
            })
            
        except:
            continue
            
    return results

def main():
    if not os.path.exists('static'):
        os.makedirs('static')

    ticker_info_list = get_tickers_from_excel(SOURCE_EXCEL_FILE)
    
    if not ticker_info_list:
        log("⚠️ 엑셀 파일 로드 실패 (Fallback 모드 진입)")
        ticker_info_list = [
            {"Ticker": "AAPL", "Sector": "Technology", "Industry": "Consumer Electronics"},
            {"Ticker": "MSFT", "Sector": "Technology", "Industry": "Software"},
            {"Ticker": "TSLA", "Sector": "Consumer Cyclical", "Industry": "Auto Manufacturers"}
        ]
    else:
        log(f"엑셀 로드 성공: {len(ticker_info_list)}개")
    
    start_time = time.time()
    results = get_market_cap_and_rs(ticker_info_list)
    end_time = time.time()
    
    log(f"수집 완료! 소요 시간: {end_time - start_time:.1f}초, 성공: {len(results)}개")

    output_data = {
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "total_count": len(results),
        "logs": DEBUG_LOGS,  # 로그 포함
        "data": results
    }
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
