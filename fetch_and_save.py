
import pandas as pd
import yfinance as yf
import json
import os
import time
import requests
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils import get_tickers_from_excel

# 설정
SOURCE_EXCEL_FILE = "RS분석툴.xlsm"
OUTPUT_FILE = "static/result.json"

def get_session():
    """
    브라우저처럼 위장한 세션 생성
    """
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    ]
    
    session.headers.update({
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
    })
    return session

def get_market_cap_and_rs(ticker_info_list):
    tickers = [item['Ticker'] for item in ticker_info_list]
    info_map = {item['Ticker']: item for item in ticker_info_list}
    
    print(f"[{time.strftime('%X')}] 총 {len(tickers)}개 티커 데이터 수집 시작 (Anti-Blocking On)...")
    
    session = get_session()
    search_tickers = tickers + ["QQQ"]
    
    try:
        # session 파라미터를 통해 위장된 헤더 전달
        data = yf.download(search_tickers, period="6mo", progress=True, threads=True, session=session)
    except Exception as e:
        print(f"다운로드 실패: {e}")
        return []

    if data.empty:
        return []
    
    if 'Adj Close' in data.columns:
        closes = data['Adj Close']
    elif 'Close' in data.columns:
        closes = data['Close']
    else:
        closes = data

    if len(closes) < 61:
        return []

    latest = closes.iloc[-1]
    prev_60 = closes.iloc[-61]

    # QQQ
    q_cur = latest.get("QQQ", 0)
    q_prev = prev_60.get("QQQ", 0)
    q_chg = (q_cur / q_prev) if q_prev != 0 else 0
    print(f"QQQ 변동률: {q_chg:.4f}")

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
            
            # 주식 수 (yf.Ticker)
            # Ticker 호출 시에도 세션 적용은 어렵지만(내부 구현상), 
            # 일단 yfinance가 최근 업데이트에서 자동 핸들링함.
            # 실패 시 0 처리.
            try:
                # 개별 티커 호출은 느리므로 예외처리만 잘 해둠
                stock = yf.Ticker(t, session=session)
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
    # 실제 운영 시 제한 해제
    # ticker_info_list = ticker_info_list[:100] 
    
    print(f"대상 티커: {len(ticker_info_list)}개")

    start_time = time.time()
    results = get_market_cap_and_rs(ticker_info_list)
    end_time = time.time()
    
    print(f"수집 완료! 소요 시간: {end_time - start_time:.1f}초, 성공: {len(results)}개")

    output_data = {
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "total_count": len(results),
        "data": results
    }
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
