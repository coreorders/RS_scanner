
from flask import Flask, render_template, jsonify, request, send_file
import pandas as pd
import os
import yfinance as yf
import concurrent.futures
import io
import time
from utils import get_tickers_from_excel

app = Flask(__name__)

# 전역 캐시는 이제 큰 의미가 없지만(Stateless), 짧은 배치를 위해 남겨둠
# Vercel은 요청 간 메모리 공유를 보장하지 않음.
SOURCE_EXCEL_FILE = "RS분석툴.xlsm"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/tickers')
def get_all_tickers():
    """
    1. 엑셀에서 모든 티커 및 기본 정보 읽어서 반환
    """
    try:
        data = get_tickers_from_excel(SOURCE_EXCEL_FILE)
        
        # [TEST MODE] 100개로 제한
        limit = 100
        if len(data) > limit:
            data = data[:limit]
            
        return jsonify({"status": "success", "data": data, "count": len(data)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/batch_screen', methods=['POST'])
def batch_screen():
    """
    2. 소규모 배치를 받아 RS 계산 후 반환 (Stateless)
    Payload: { "tickers": [ {"Ticker": "AAPL", ...}, ... ] }
    """
    try:
        req_data = request.json
        items = req_data.get('items', [])
        
        if not items:
            return jsonify({"status": "success", "results": []})
            
        print(f"Processing batch of {len(items)} items...")
        results = calculate_batch(items)
        
        return jsonify({"status": "success", "results": results})
        
    except Exception as e:
        print(f"Batch error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/create_excel', methods=['POST'])
def create_excel():
    """
    3. 클라이언트가 완성된 데이터 배열을 보내면 엑셀로 변환해서 다운로드 제공
    Vercel은 파일 저장이 안 되므로, 받아서 바로 변환해 쏴줌.
    Payload: { "data": [ ... ] }
    """
    try:
        req_data = request.json
        data_list = req_data.get('data', [])
        
        if not data_list:
            return "No data provided", 400
            
        df = pd.DataFrame(data_list)
        # 컬럼 순서 보장
        cols = ["Ticker", "MarketCap", "MarketCapRank", "RS", "Sector", "Industry", "Price", "Shares"]
        available_cols = [c for c in cols if c in df.columns]
        df = df[available_cols]
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        
        filename = f"RS_Scanner_Result_{int(time.time())}.xlsx"
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
         return f"Excel creation failed: {str(e)}", 500

# --- Helper Logic (moved from utils to here/inline or import) ---
# utils.py 의 get_market_cap_and_rs 로직을 배치에 맞게 경량화

def get_shares_outstanding(ticker):
    try:
        t = yf.Ticker(ticker)
        return ticker, t.fast_info.get('shares', 0)
    except:
        return ticker, 0

def calculate_batch(items):
    """
    items: list of dict {'Ticker':..., 'Sector':...}
    """
    tickers = [x['Ticker'] for x in items]
    info_map = {x['Ticker']: x for x in items}
    
    # 1. Price Bulk Download
    # 배치 사이즈가 작으므로(20~50개) 금방 끝남
    # QQQ는 매 배치마다 필요하므로 리스트에 추가
    download_list = tickers + ["QQQ"]
    
    # threads=False로 설정하여 오버헤드 줄임 (배치가 작아서)
    data = yf.download(download_list, period="6mo", progress=False, threads=True)
    
    if 'Adj Close' in data.columns:
        closes = data['Adj Close']
    elif 'Close' in data.columns:
        closes = data['Close']
    else:
        closes = data
        
    if len(closes) < 61:
        # 데이터 부족 시 빈 리스트 반환 (혹은 가능한 것만)
        return []
        
    latest = closes.iloc[-1]
    prev_60 = closes.iloc[-61]
    
    # QQQ
    q_cur = latest.get("QQQ", 0)
    q_prev = prev_60.get("QQQ", 0)
    q_chg = (q_cur / q_prev) if q_prev != 0 else 0
    
    # Shares (Parallel)
    shares_map = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(tickers), 10)) as exc:
        future_to_t = {exc.submit(get_shares_outstanding, t): t for t in tickers}
        for f in concurrent.futures.as_completed(future_to_t):
            t, s = f.result()
            shares_map[t] = s
            
    results = []
    for t in tickers:
        try:
            cur = latest.get(t, None)
            prev = prev_60.get(t, None)
            
            if pd.isna(cur) or pd.isna(prev) or prev == 0:
                continue
                
            chg = cur / prev
            rs = (chg / q_chg) - 1 if q_chg != 0 else 0
            
            sh = shares_map.get(t, 0)
            mcap = cur * sh
            
            base = info_map.get(t, {})
            
            results.append({
                "Ticker": t,
                "Price": round(cur, 2),
                "RS": round(rs, 4),
                "MarketCap": round(mcap, 0),
                "Shares": sh,
                "Sector": base.get("Sector", ""),
                "Industry": base.get("Industry", "")
            })
        except:
            continue
            
    return results

if __name__ == '__main__':
    app.run(debug=True, port=8888)
