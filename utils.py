import pandas as pd
import requests
import yfinance as yf
import time
import io
import warnings
from concurrent.futures import ThreadPoolExecutor

# 경고 메시지 숨김 (Pyarrow 등)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

import json
import os

# 전역 캐시 변수
SECTOR_CACHE_FILE = "static/sector_search.json"
SECTOR_CACHE = {} # This line will be changed

def load_sector_cache():
    global SECTOR_CACHE
    if os.path.exists(SECTOR_CACHE_FILE):
        try:
            with open(SECTOR_CACHE_FILE, 'r', encoding='utf-8') as f:
                SECTOR_CACHE = json.load(f)
            print(f"Sector Cache Loaded: {len(SECTOR_CACHE)} items")
        except Exception as e:
            print(f"Cache Load Error: {e}")
            SECTOR_CACHE = {}

def save_sector_cache():
    try:
        if not os.path.exists('static'):
            os.makedirs('static')
        with open(SECTOR_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(SECTOR_CACHE, f, ensure_ascii=False, indent=2)
        print(f"Sector Cache Saved: {len(SECTOR_CACHE)} items")
    except Exception as e:
        print(f"Cache Save Error: {e}")

# 초기 로드
load_sector_cache()

def get_tickers_from_google_sheet(url):
    """
    구글 시트 CSV URL에서 티커 목록을 가져옵니다.
    A열에 티커가 있다고 가정합니다.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # CSV 데이터를 pandas DataFrame으로 읽기 (헤더 없음 가정)
        # 만약 첫 줄이 티커라면 header=None을 써야 함.
        # 사용자가 "A 열에서 티커를 긁어다가"라고 했고, 확인 결과 첫 줄부터 티커임 (LRN)
        df = pd.read_csv(io.StringIO(response.text), header=None)
        
        # 첫 번째 컬럼을 티커로 간주
        if df.empty:
            return []
            
        ticker_column = df.columns[0] # 0번 인덱스
        tickers = df[ticker_column].dropna().unique().tolist()
        
        # fetch_and_save.py 호환성을 위해 딕셔너리 리스트로 변환
        # (기존 로직이 {'Ticker': 'AAPL', ...} 형태를 기대할 수 있음)
        ticker_info_list = [{'Ticker': str(t).strip().upper()} for t in tickers if str(t).strip()]
        
        return ticker_info_list
        
    except Exception as e:
        print(f"구글 시트 로드 중 에러: {e}")
        return []

def get_tickers_from_excel(file_path):
    """
    레거시 호환성을 위한 엑셀 읽기 함수 (현재는 사용되지 않을 수 있음)
    """
    try:
        df = pd.read_excel(file_path, sheet_name=0)
        tickers = df.iloc[:, 0].dropna().tolist() # 첫 번째 컬럼
        return [{'Ticker': str(t).strip().upper()} for t in tickers]
    except Exception as e:
        print(f"엑셀 로드 에러: {e}")
        return []

def get_market_cap_and_rs(ticker_info_list, batch_size=20):
    """
    티커 리스트를 받아 Market Cap과 RS를 계산합니다.
    20개씩 배치로 처리하여 yfinance 부하를 조절합니다.
    """
    results = []
    total_tickers = len(ticker_info_list)
    
    # QQQ 데이터 미리 확보 (벤치마크)
    print("벤치마크 (QQQ) 데이터 다운로드 중...")
    try:
        qqq_data = yf.download("QQQ", period="6mo", progress=False)
        if len(qqq_data) < 61:
            print("경고: QQQ 데이터가 충분하지 않아 RS 계산이 부정확할 수 있습니다.")
    except Exception as e:
        print(f"QQQ 다운로드 실패: {e}")
        qqq_data = pd.DataFrame()

    for i in range(0, total_tickers, batch_size):
        batch = ticker_info_list[i:i+batch_size]
        batch_tickers = [item['Ticker'] for item in batch]
        print(f"Processing batch {i} to {min(i+batch_size, total_tickers)}: {batch_tickers}")
        
        try:
            # 1. 주가 데이터 일괄 다운로드 (Price & RS용)
            # 60영업일 전 데이터를 위해 충분히 6개월치를 가져옵니다.
            data = yf.download(batch_tickers, period="6mo", progress=False, group_by='ticker')
            
            # 2. 각 티커별 정보 처리
            # 메타데이터(시총 등)는 별도 호출이 필요할 수 있으나, 
            # yfinance 최신 버전에서는 download로 시총을 못 가져오므로 Ticker.info 접근 필요
            # 속도를 위해 ThreadPool 사용
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_ticker = {executor.submit(process_single_ticker, ticker, data, qqq_data): ticker for ticker in batch_tickers}
                
                for future in future_to_ticker:
                    res = future.result()
                    if res:
                        results.append(res)
                        
        except Exception as e:
            print(f"Batch 처리 중 에러: {e}")
            
        # 딜레이 (옵션)
        time.sleep(1)
    
    # --- Retry Logic (재시도) ---
    # 1. 실패하거나 RS가 NaN인 티커 식별
    # results에는 {Ticker, RS, ...} 딕셔너리가 들어있음.
    processed_tickers = set()
    for r in results:
        if r and 'Ticker' in r:
            rs_val = r.get('RS')
            # RS가 유효한 숫자이고 NaN이 아닌지 확인
            # json dump시 NaN은 'NaN' 등이 될 수 있음, 여기서는 float nan 체크
            is_valid = False
            if rs_val is not None:
                try:
                    # 문자열 'nan' 체크 및 float nan 체크
                    if str(rs_val).lower() != 'nan' and rs_val != 0: 
                        # 0도 재시도 대상에 포함할지? 사용자는 "nan 뜬거만"이라고 했지만
                        # 데이터 부족으로 0인 경우도 있을 수 있음. 
                        # 하지만 0은 데이터 부족(계산불가)이므로 재시도해도 똑같을 확률 높음.
                        # NaN(JSON 에러 유발자)만 타겟팅.
                         processed_tickers.add(r['Ticker'])
                    elif rs_val == 0:
                        # 0인 경우(데이터 부족)는 '처리됨'으로 간주할지?
                        # 사용자는 "nan 뜬거만"이라고 했음. 0은 정상 결과(데이터 부족)일 수 있음.
                        # 따라서 0은 성공으로 간주. NaN만 실패로 간주.
                        processed_tickers.add(r['Ticker'])
                except:
                    pass

    all_tickers = {item['Ticker'] for item in ticker_info_list}
    failed_tickers = list(all_tickers - processed_tickers)
    
    if failed_tickers:
        print(f"\n[Retry] RS 수집 실패/NaN {len(failed_tickers)}개 발견. 배치 재시도 중...")
        
        # 재시도도 배치로 처리
        retry_batch_size = 20
        for i in range(0, len(failed_tickers), retry_batch_size):
            batch = failed_tickers[i:i+retry_batch_size]
            print(f" -> Retry batch {batch}")
            
            try:
                # 배치 다운로드
                data = yf.download(batch, period="6mo", progress=False, group_by='ticker')
                
                # 병렬 처리 (메인 로직 재사용)
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_ticker = {executor.submit(process_single_ticker, t, data, qqq_data): t for t in batch}
                    
                    for future in future_to_ticker:
                        res = future.result()
                        if res:
                            rs_res = res.get('RS')
                            if rs_res is not None and str(rs_res).lower() != 'nan':
                                # 성공 시 기존 결과 제거 후 추가
                                results = [r for r in results if r['Ticker'] != res['Ticker']]
                                results.append(res)
                                print(f"    -> {res['Ticker']} 복구 성공 (RS: {res['RS']})")
                            else:
                                print(f"    -> {res['Ticker']} 복구 실패")
                                
            except Exception as e:
                print(f"Retry Batch 에러: {e}")
            
            time.sleep(1) # 배치 간 딜레이

    # 작업 완료 후 캐시 저장
    save_sector_cache()
    
    return results

def process_single_ticker(original_ticker, batch_data, qqq_data):
    """
    단일 티커에 대한 RS 계산 및 Info 처리를 수행합니다.
    """
    try:
        # Sanitize for API usage locally
        yf_ticker = original_ticker.replace('.', '-')
        
        # 데이터 추출 (MultiIndex 처리)
        if isinstance(batch_data.columns, pd.MultiIndex):
             # batch_data['Close'][ticker] 와 같은 형태로 접근
             if yf_ticker in batch_data.columns.levels[0]:
                 df = batch_data[yf_ticker]
             else:
                 # 티커가 하나뿐일 경우 구조가 다를 수 있음 처리
                 # download시 list로 넘겼으므로 보통 MultiIndex임.
                 # 데이터가 없는 경우
                 return None
        else:
            # 티커가 1개인 배치였을 경우
            df = batch_data
            
        # Close Price 확인
        if 'Close' not in df.columns or df.empty:
            return None
            
        hist = df['Close']
        idx_latest = -1
        idx_60ago = -61
        
        if len(hist) < 61:
            rs_val = None
        else:
            # RS 계산: [(전영업일 주가)/(60영업일전 주가)] / [(전영업일 QQQ)/(60영업일전 QQQ)] - 1
            
            stock_current = float(hist.iloc[idx_latest])
            stock_60ago = float(hist.iloc[idx_60ago])
            
            if qqq_data.empty or len(qqq_data) < 61:
                rs_val = 0
            else:
                # 안전한 float 변환 (Series vs Scalar)
                q_curr = qqq_data['Close'].iloc[idx_latest]
        # ... (RS Calculation logic remains same) ...
        # Calculate RS
        try:
            latest_price = df['Close'].iloc[-1]
            price_60_ago = df['Close'].iloc[0] # Approx 60 days
            
            stock_return = (latest_price - price_60_ago) / price_60_ago
            
            qqq_latest = qqq_data['Close'].iloc[-1]
            qqq_60_ago = qqq_data['Close'].iloc[0]
            qqq_return = (qqq_latest - qqq_60_ago) / qqq_60_ago
            
            rs_score = stock_return - qqq_return
        except:
            rs_score = 0
            latest_price = 0
        
        # 메타데이터 (Market Cap, Sector, Industry)
        # For Metadata, loop up using sanitied ticker
        t = yf.Ticker(yf_ticker)
        
        # 1. Sector/Industry (Cache Check) uses ORIGINAL ticker key usually, 
        # but for API fetch we must use yf_ticker.
        # Let's keep cache key as valid yf_ticker to avoid confusion, OR use original.
        # User list has original. Let's try to stick to original for cache key if possible, 
        # but the cached data implies 'what returns from API'.
        # Actually simplest is: Use ORIGINAL for UI/Result, use YF_TICKER for API.
        
        cached = SECTOR_CACHE.get(original_ticker) 
        sector = "N/A"
        industry = "N/A"
        
        # Check Cache
        if cached and cached.get('Sector') not in ['N/A', 'nan', 'NONE'] and cached.get('Industry') not in ['N/A', 'nan', 'NONE']:
            sector = cached['Sector']
            industry = cached['Industry']
        else:
            # Fetch Metadata
            if sector == "N/A" and industry == "N/A":
                try:
                    info = t.info 
                    
                    quote_type = info.get('quoteType', '').upper()
                    
                    if 'ETF' in quote_type:
                        sector = 'ETF'
                        industry = 'ETF' # User requested both to be ETF
                    elif 'ETN' in quote_type: 
                        sector = 'ETN'
                        industry = 'ETN' # User requested both to be ETN
                    else:
                        sector = info.get('sector', 'N/A')
                        industry = info.get('industry', 'N/A')

                    if not sector: sector = 'N/A'
                    if not industry: industry = 'N/A'
                        
                    # Save to Cache using ORIGINAL key for consistency
                    SECTOR_CACHE[original_ticker] = {'Sector': sector, 'Industry': industry}
                except:
                    sector = 'N/A'
                    industry = 'N/A'

        # 2. Market Cap (Fast Info)
        market_cap = 0
        try:
            market_cap = t.fast_info['market_cap']
        except:
            pass
            
        return {
            'Ticker': original_ticker, # Return original for UI
            'Price': float(latest_price),
            'Market Cap': f"{market_cap / 1e9:.2f}B" if market_cap else "N/A",
            'RS': float(rs_score),
            'Sector': sector,
            'Industry': industry
        }

    except Exception as e:
        print(f"Error processing {original_ticker}: {e}")
        return None
