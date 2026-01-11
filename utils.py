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
        
    return results

def process_single_ticker(ticker, batch_data, qqq_data):
    """
    단일 티커에 대한 RS 계산 및 Info 처리를 수행합니다.
    """
    try:
        # 데이터 추출 (MultiIndex 처리)
        if isinstance(batch_data.columns, pd.MultiIndex):
             # batch_data['Close'][ticker] 와 같은 형태로 접근
             if ticker in batch_data.columns.levels[0]:
                 df = batch_data[ticker]
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
                q_60 = qqq_data['Close'].iloc[idx_60ago]
                
                qqq_current = float(q_curr.iloc[0]) if hasattr(q_curr, 'iloc') else float(q_curr)
                qqq_60ago = float(q_60.iloc[0]) if hasattr(q_60, 'iloc') else float(q_60)
                
                if stock_60ago == 0 or qqq_60ago == 0:
                    rs_val = 0
                else:
                    rs_val = ((stock_current / stock_60ago) / (qqq_current / qqq_60ago)) - 1
        
        # 메타데이터 (Market Cap 등) 
        # Ticker.info 호출은 느릴 수 있음
        try:
            t = yf.Ticker(ticker)
            info = t.info
            market_cap = info.get('marketCap', 0)
            sector = info.get('sector', 'N/A')
            industry = info.get('industry', 'N/A')
            
            # 포메팅
            mc_str = f"{market_cap / 1e9:.2f}B" if market_cap else "N/A"
            
        except Exception:
            market_cap = 0
            mc_str = "N/A"
            sector = "N/A"
            industry = "N/A"

        return {
            "Ticker": ticker,
            "RS": round(rs_val, 4) if rs_val is not None else 0,
            "Market Cap": mc_str,
            "Price": round(float(hist.iloc[-1]), 2),
            "Sector": sector,
            "Industry": industry
        }

    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return None
