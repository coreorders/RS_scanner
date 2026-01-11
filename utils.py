
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

def get_ticker_details(ticker, need_metadata=False):
    """
    개별 종목의 발행주식수와 메타데이터(Sector, Industry)를 가져옵니다.
    Rate Limit 방지를 위해 딜레이와 재시도를 포함합니다.
    """
    # Random delay 
    time.sleep(random.uniform(0.1, 0.3))
    
    shares = 0
    sector = None
    industry = None
    
    for attempt in range(3): # 3회 재시도
        try:
            t = yf.Ticker(ticker)
            
            # 1. Shares (fast_info 우선)
            shares = t.fast_info.get('shares', 0)
            
            # 2. Metadata (필요하거나 shares가 0일 때 info 확인)
            if need_metadata or shares == 0:
                try:
                    info = t.info
                    # Shares fallback
                    if shares == 0:
                        shares = info.get('sharesOutstanding', 0)
                        if shares == 0:
                            shares = info.get('impliedSharesOutstanding', 0)
                    
                    # Metadata fallback (없으면 info에서 가져옴)
                    if need_metadata:
                        sector = info.get('sector', 'N/A')
                        industry = info.get('industry', 'N/A')
                except:
                    pass
            
            # 성공 판별 (shares가 있으면 일단 성공으로 침)
            if shares > 0:
                return ticker, shares, sector, industry
            
            # 실패 시 대기 후 재시도
            time.sleep(1)
                
        except:
            time.sleep(1)
            pass
            
    return ticker, shares, sector, industry

def get_market_cap_and_rs(ticker_info_list, limit=None, progress_callback=None):
    """
    1. 가격 데이터 Bulk Download (속도 빠름)
    2. 발행주식수 & 메타데이터 개별 조회 (속도 느림, 안전하게 순차 처리)
    3. RS 및 시총 계산
    """
    if progress_callback: progress_callback(0)
        
    if limit:
        ticker_info_list = ticker_info_list[:limit]

    tickers = [item['Ticker'] for item in ticker_info_list]
    # 빠른 조회를 위해 Map 생성
    base_info_map = {item['Ticker']: item for item in ticker_info_list}

    print(f"Fetching price data for {len(tickers)} tickers...")
    if progress_callback: progress_callback(5)
    
    # 1. Price Bulk Download
    search_tickers = tickers + ["QQQ"]
    # yfinance 0.2.40+ handles limits internally, but for safety
    try:
        data = yf.download(search_tickers, period="6mo", progress=True, threads=True)
    except Exception as e:
        print(f"Error downloading price data: {e}")
        return []

    if data.empty:
        return []
    
    if progress_callback: progress_callback(30)

    # 컬럼 처리 (Adj Close 우선)
    if 'Adj Close' in data.columns:
        closes = data['Adj Close']
    elif 'Close' in data.columns:
        closes = data['Close']
    else:
        # MultiIndex 구조 처리
        try: closes = data.xs('Adj Close', axis=1, level=0)
        except: closes = data

    if len(closes) < 30: # 데이터가 너무 적으면 스킵
        raise ValueError("Not enough historical data.")

    latest = closes.iloc[-1]
    # 60일 전 가격 (없으면 가장 오래된 값)
    prev_60_idx = -61 if len(closes) >= 61 else 0
    prev_60 = closes.iloc[prev_60_idx]

    # QQQ Benchmark
    q_cur = latest.get("QQQ", 0)
    q_prev = prev_60.get("QQQ", 0)
    q_chg = (q_cur / q_prev) if q_prev != 0 else 0

    results = []
    
    print("Fetching component details (Shares & Metadata)...")
    
    # 2. 개별 종목 상세 정보 조회 (순차 처리 or 소수 병렬)
    # utils 함수 내에서는 병렬 처리를 위한 executor 구성을 유연하게
    
    tasks = []
    # 현재 설정: 안전제일 (순차 처리 권장)
    max_workers = 1 
    
    processed_count = 0
    total_count = len(tickers)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks
        future_to_ticker = {}
        for t in tickers:
            # 구글 시트에서 가져온 기본 정보
            base = base_info_map.get(t, {})
            current_sec = base.get('Sector', 'N/A')
            current_ind = base.get('Industry', 'N/A')
            
            # 메타데이터가 N/A면 API에서 가져오도록 요청 (need_metadata=True)
            need_meta = (current_sec == 'N/A' or current_ind == 'N/A')
            
            future = executor.submit(get_ticker_details, t, need_meta)
            future_to_ticker[future] = t

        for future in concurrent.futures.as_completed(future_to_ticker):
            t = future_to_ticker[future]
            try:
                # 결과: shares, sector, industry
                _, share_count, fetched_sec, fetched_ind = future.result()
                
                processed_count += 1
                # Progress Update: 30% -> 90%
                if progress_callback:
                    current_progress = 30 + int((processed_count / total_count) * 60)
                    progress_callback(current_progress)

                # 가격 데이터 확인
                cur_price = latest.get(t, None)
                prev_price = prev_60.get(t, None)
                
                if pd.isna(cur_price) or pd.isna(prev_price) or prev_price == 0:
                    continue
                
                # RS 계산
                chg = cur_price / prev_price
                rs = (chg / q_chg) - 1 if q_chg != 0 else 0
                
                # Market Cap
                mcap = cur_price * share_count
                
                # 메타데이터 병합 (API값이 있으면 덮어쓰기)
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
    except Exception as e:
        print(f"Error in calculation: {e}")
        return []

if __name__ == "__main__":
    t = get_tickers_from_excel()
    print(t[:5])
    res = get_market_cap_and_rs(t, limit=10)
    print(res)
