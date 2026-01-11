
import yfinance as yf
import pandas as pd
import requests
import concurrent.futures

def get_tickers_from_excel(file_path="RS분석툴.xlsm"):
    """
    엑셀 파일에서 티커 및 부가 정보(Sector, Industry)를 읽어옵니다.
    반환값: [{'Ticker': 'AAPL', 'Sector': 'Technoloy', 'Industry': 'Consumer Electronics'}, ...]
    """
    try:
        df = pd.read_excel(file_path, sheet_name=0)
        # 필요한 컬럼만 추출/확인
        result_list = []
        
        if 'Ticker' in df.columns:
            # NaN 제거
            df = df.dropna(subset=['Ticker'])
            
            for _, row in df.iterrows():
                t = str(row['Ticker']).strip().upper().replace('.', '-')
                # Sector/Industry가 있으면 가져오고 없으면 빈 문자열
                sec = row.get('Sector', '')
                ind = row.get('Industry', '')
                
                result_list.append({
                    "Ticker": t,
                    "Sector": sec if pd.notna(sec) else 'N/A',
                    "Industry": ind if pd.notna(ind) else 'N/A'
                })
                
            print(f"Loaded {len(result_list)} tickers with info from Excel.")
            return result_list
        else:
            print("Column 'Ticker' not found in Excel.")
            return []
    except Exception as e:
        print(f"Error reading Excel tickers: {e}")
        return []

def get_shares_outstanding(ticker):
    """
    개별 종목의 발행주식수를 가져옵니다 (fast_info 사용).
    """
    try:
        t = yf.Ticker(ticker)
        # fast_info 사용
        shares = t.fast_info.get('shares', 0)
        return ticker, shares
    except:
        return ticker, 0

def get_market_cap_and_rs(ticker_info_list, limit=None, progress_callback=None):
    """
    RS 및 시가총액 계산.
    ticker_info_list: [{'Ticker': '...', 'Sector': '...', 'Industry': '...'}, ...]
    progress_callback: function(int) -> update progress percentage
    """
    try:
        if progress_callback: progress_callback(0)
        
        if limit and len(ticker_info_list) > limit:
            ticker_info_list = ticker_info_list[:limit]
            
        tickers = [item['Ticker'] for item in ticker_info_list]
        info_map = {item['Ticker']: item for item in ticker_info_list}
            
        print(f"Processing {len(tickers)} tickers...")
        if progress_callback: progress_callback(5)
        
        # 1. Price Data (Bulk Download)
        all_tickers = tickers + ["QQQ"]
        print("Downloading price data...")
        # yfinance download logs progress to stdout, we can't easily capture it for web UI
        data = yf.download(all_tickers, period="6mo", progress=True, threads=True)
        
        if progress_callback: progress_callback(30)
        
        if 'Adj Close' in data.columns:
            closes = data['Adj Close']
        elif 'Close' in data.columns:
            closes = data['Close']
        else:
            closes = data

        if len(closes) < 61:
            raise ValueError("Not enough historical data.")

        latest_prices = closes.iloc[-1]
        prices_60_ago = closes.iloc[-61]
        
        # QQQ Data
        qqq_curr = latest_prices.get("QQQ", 0)
        qqq_prev = prices_60_ago.get("QQQ", 0)
        
        if qqq_prev == 0:
            qqq_change = 0 
        else:
            qqq_change = qqq_curr / qqq_prev
            
        # 2. Shares Outstanding (Parallel Fetch)
        print("Fetching shares outstanding...")
        shares_map = {}
        total_tickers = len(tickers)
        completed_shares = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_ticker = {executor.submit(get_shares_outstanding, t): t for t in tickers}
            for future in concurrent.futures.as_completed(future_to_ticker):
                t, s = future.result()
                shares_map[t] = s
                
                # Progress Update: 30% -> 90%
                completed_shares += 1
                if progress_callback:
                    # 30 + (completed / total) * 60
                    current_progress = 30 + int((completed_shares / total_tickers) * 60)
                    progress_callback(current_progress)
        
        results = []
        
        for ticker in tickers:
            if ticker == "QQQ": continue
            
            try:
                curr = latest_prices.get(ticker, None)
                prev = prices_60_ago.get(ticker, None)
                
                if pd.isna(curr) or pd.isna(prev) or prev == 0:
                    continue
                    
                stock_change = curr / prev
                
                if qqq_change == 0:
                    rs = 0
                else:
                    rs = (stock_change / qqq_change) - 1
                
                shares = shares_map.get(ticker, 0)
                market_cap = curr * shares
                
                # 기존 엑셀 정보 병합
                base_info = info_map.get(ticker, {})
                
                results.append({
                    "Ticker": ticker,
                    "RS": round(rs, 4),
                    "Price": round(curr, 2),
                    "MarketCap": round(market_cap, 0),
                    "Shares": shares,
                    "Sector": base_info.get("Sector", "N/A"),
                    "Industry": base_info.get("Industry", "N/A")
                })
                
            except Exception as e:
                continue
        
        # Market Cap Rank 계산
        # 1. 시가총액 내림차순 정렬
        results.sort(key=lambda x: x['MarketCap'], reverse=True)
        for idx, item in enumerate(results):
            item['MarketCapRank'] = idx + 1
            
        # 2. 최종 RS 내림차순 정렬 (기본 뷰)
        results.sort(key=lambda x: x['RS'], reverse=True)
        
        if progress_callback: progress_callback(100)
            
        return results

    except Exception as e:
        print(f"Error in calculation: {e}")
        return []

if __name__ == "__main__":
    t = get_tickers_from_excel()
    print(t[:5])
    res = get_market_cap_and_rs(t, limit=10)
    print(res)
