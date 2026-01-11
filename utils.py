
import yfinance as yf
import pandas as pd
import requests
import concurrent.futures

def get_tickers_from_google_sheet(url):
    """
    구글 스프레드시트 CSV URL에서 티커를 읽어옵니다.
    A열(첫 번째 열)을 티커로 간주하며, 첫 행이 'Ticker' 같은 헤더가 아니더라도 처리합니다.
    """
    try:
        # 헤더 없이 일단 읽음
        df = pd.read_csv(url, header=None)
        
        if df.empty:
            print("Google Sheet is empty.")
            return []
            
        # 첫 번째 열(Column 0)이 티커
        # 첫 행(Row 0)이 'Ticker', 'Symbol' 등 헤더 텍스트라면 제거
        first_val = str(df.iloc[0, 0]).upper()
        if first_val in ['TICKER', 'SYMBOL', 'CODE', '티커', '종목코드']:
            df = df.iloc[1:]
            
        result_list = []
        for _, row in df.iterrows():
            # A열 확보
            t = str(row[0]).strip().upper()
            if not t or t == 'NAN': continue
            
            # C/D열 등이 있으면 Sector/Industry로 쓸 수도 있지만, 
            # 사용자 요청은 'A열'만 언급했으므로 나머지는 기본 N/A 처리하되
            # 혹시 모르니 컬럼이 충분하면 가져옴
            sec = 'N/A'
            ind = 'N/A'
            if len(df.columns) >= 3:
                # 안전하게 가져오기
                try: 
                    s_val = str(row[1])
                    if s_val and s_val != 'nan': sec = s_val
                    
                    i_val = str(row[2])
                    if i_val and i_val != 'nan': ind = i_val
                except: pass
            
            result_list.append({
                "Ticker": t,
                "Sector": sec,
                "Industry": ind
            })
            
        print(f"Loaded {len(result_list)} tickers from Google Sheet.")
        return result_list
        
    except Exception as e:
        print(f"Error reading Google Sheet: {e}")
        return []

def get_tickers_from_excel(file_path="RS분석툴.xlsm"):
    """
    엑셀 파일에서 티커 및 부가 정보(Sector, Industry)를 읽어옵니다.
    반환값: [{'Ticker': 'AAPL', 'Sector': 'Technoloy', 'Industry': 'Consumer Electronics'}, ...]
    """
    try:
        df = pd.read_excel(file_path, sheet_name=0, engine='openpyxl')
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

import time
import random

def get_yf_session():
    """
    Create a custom session with a browser-like User-Agent
    to avoid being blocked by Yahoo Finance in GitHub Actions.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    })
    return session

def get_ticker_details(ticker, need_metadata=False):
    """
    개별 종목의 발행주식수와 메타데이터(Sector, Industry)를 가져옵니다.
    Rate Limit 방지를 위해 딜레이와 재시도를 포함합니다.
    """
    time.sleep(random.uniform(0.5, 1.0)) # Increased delay
    
    shares = 0
    sector = None
    industry = None
    
    for attempt in range(3): 
        try:
            t = yf.Ticker(ticker)
            # 1. Shares
            shares = t.fast_info.get('shares', 0)
            
            # 2. Metadata or fallback
            if need_metadata or shares == 0:
                try:
                    # [Safety Update] Disable t.info for Sector/Industry to prevent IP blocking
                    # Only fetch if absolutely necessary (e.g. shares=0), otherwise skip metadata
                    if shares == 0:
                         # Use fast_info fallback first if possible, or skip
                         pass
                    
                    # Disabled to prevent JSONDecodeError (blocking)
                    # if need_metadata:
                    #     info = t.info
                    #     sector = info.get('sector', 'N/A')
                    #     industry = info.get('industry', 'N/A')
                except:
                    pass
            
            if shares > 0:
                return ticker, shares, sector, industry
            time.sleep(1)
        except:
            time.sleep(1)
            pass
            
    return ticker, shares, sector, industry

def get_market_cap_and_rs(ticker_info_list, limit=None, progress_callback=None):
    if progress_callback: progress_callback(0)
        
    if limit:
        ticker_info_list = ticker_info_list[:limit]

    tickers = [item['Ticker'] for item in ticker_info_list]
    # Filter out invalid tickers
    tickers = [t for t in tickers if t not in ['TICKER', 'SYMBOL', 'CODE', '종목코드', '티커', 'NAN', 'N/A']]
    
    base_info_map = {item['Ticker']: item for item in ticker_info_list if item['Ticker'] in tickers}

    print(f"Fetching price data for {len(tickers)} tickers...")
    if progress_callback: progress_callback(5)
    
    # Session setup
    session = get_yf_session()
    
    # 1. Price Bulk Download
    search_tickers = tickers + ["QQQ"]
    data = pd.DataFrame()
    
    try:
        data = yf.download(search_tickers, period="6mo", progress=True, threads=False, session=session) # threads=False for safety
    except Exception as e:
        print(f"Bulk download failed: {e}")
        data = pd.DataFrame()

    closes = pd.DataFrame()
    use_bulk_data = False
    
    if not data.empty:
        try:
            if 'Adj Close' in data.columns:
                closes = data['Adj Close']
            elif 'Close' in data.columns:
                closes = data['Close']
            else:
                try: closes = data.xs('Adj Close', axis=1, level=0)
                except: closes = data
            
            if not closes.empty:
                use_bulk_data = True
        except:
            use_bulk_data = False
            
    if not use_bulk_data or len(closes) < 30:
        print("⚠️ Bulk data unavailable or insufficient. Switching to sequential price fetch.")
        pass

    results = []
    print("Fetching component details...")
    
    # QQQ Benchmark (Fallback)
    q_chg = 0
    if not use_bulk_data:
        try:
            # Use download with session for QQQ fallback too
            q_df = yf.download("QQQ", period="6mo", session=session, progress=False)
            if not q_df.empty:
                if 'Adj Close' in q_df.columns: q_hist = q_df['Adj Close']
                elif 'Close' in q_df.columns: q_hist = q_df['Close']
                else: q_hist = q_df.iloc[:,0] # Take first column if confused
                
                q_cur = float(q_hist.iloc[-1])
                q_prev = float(q_hist.iloc[-61]) if len(q_hist) >= 61 else float(q_hist.iloc[0])
                q_chg = (q_cur / q_prev) if q_prev != 0 else 0
        except Exception as e: 
            print(f"QQQ fetch failed: {e}")
            pass
    else:
         try:
             latest = closes.iloc[-1]
             prev_60_idx = -61 if len(closes) >= 61 else 0
             prev_60 = closes.iloc[prev_60_idx]
             q_cur = latest.get("QQQ", 0)
             q_prev = prev_60.get("QQQ", 0)
             q_chg = (q_cur / q_prev) if q_prev != 0 else 0
         except: pass
    
    # ... (Rest of the loop logic will be updated in next chunk if needed, but we focus on updating session usage) ...
    # Wait, replace_file_content needs to match exact content. 
    # I will replace up to the start of the loop to inject session usage.
    
    tasks = []
    max_workers = 1 
    processed_count = 0
    total_count = len(tickers)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {}
        for t in tickers:
            base = base_info_map.get(t, {})
            current_sec = base.get('Sector', 'N/A')
            current_ind = base.get('Industry', 'N/A')
            need_meta = (current_sec == 'N/A' or current_ind == 'N/A')
            
            future = executor.submit(get_ticker_details, t, need_meta)
            future_to_ticker[future] = t

        for future in concurrent.futures.as_completed(future_to_ticker):
            t = future_to_ticker[future]
            try:
                _, share_count, fetched_sec, fetched_ind = future.result()
                processed_count += 1
                if progress_callback:
                    current_progress = 30 + int((processed_count / total_count) * 60)
                    progress_callback(current_progress)

                cur_price = 0
                prev_price = 0
                
                if use_bulk_data:
                    try:
                        cur_price = float(closes[t].iloc[-1])
                        prev_60_idx = -61 if len(closes) >= 61 else 0
                        prev_price = float(closes[t].iloc[prev_60_idx])
                    except: pass
                
                # Fallback: Retry with session using yf.download (Ticker object doesn't accept session easily)
                if cur_price == 0 or prev_price == 0:
                    for _ in range(3): 
                        try:
                            # Use yf.download with session for single ticker to ensure User-Agent is sent
                            single_df = yf.download(t, period="6mo", session=session, progress=False, threads=False)
                            if not single_df.empty:
                                if 'Adj Close' in single_df.columns: s_hist = single_df['Adj Close']
                                elif 'Close' in single_df.columns: s_hist = single_df['Close']
                                else: s_hist = single_df.iloc[:,0]

                                cur_price = float(s_hist.iloc[-1])
                                p_idx = -61 if len(s_hist) >= 61 else 0
                                prev_price = float(s_hist.iloc[p_idx])
                                break 
                        except:
                            time.sleep(1)
                            pass
                        time.sleep(1) 
                
                if pd.isna(cur_price) or pd.isna(prev_price) or prev_price == 0:
                    print(f"Skipping {t}: No price data")
                    continue
                
                # RS 계산 (QQQ = 벤치마크 지수)
                chg = cur_price / prev_price
                rs = (chg / q_chg) - 1 if q_chg != 0 else 0
                
                # Market Cap
                mcap = cur_price * share_count
                
                # 메타데이터 병합
                base = base_info_map.get(t, {})
                final_sec = fetched_sec if fetched_sec and fetched_sec != 'N/A' else base.get('Sector', 'N/A')
                final_ind = fetched_ind if fetched_ind and fetched_ind != 'N/A' else base.get('Industry', 'N/A')
                
                results.append({
                    "Ticker": t,
                    "Price": round(float(cur_price), 2),
                    "RS": round(float(rs), 4),
                    "MarketCap": round(float(mcap), 0),
                    "Shares": share_count,
                    "Sector": final_sec,
                    "Industry": final_ind
                })
                
            except Exception as e:
                print(f"Error processing {t}: {e}")
                continue

    # Rank 부여
    results.sort(key=lambda x: x['MarketCap'], reverse=True)
    for i, r in enumerate(results):
        r['MarketCapRank'] = i + 1
        
    return results

if __name__ == "__main__":
    t = get_tickers_from_excel()
    print(t[:5])
    res = get_market_cap_and_rs(t, limit=10)
    print(res)
