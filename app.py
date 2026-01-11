
from flask import Flask, render_template, jsonify, send_file
import pandas as pd
import os
import shutil
from openpyxl import load_workbook
from utils import get_tickers_from_excel, get_market_cap_and_rs
import threading
import time
import io

app = Flask(__name__)

CACHE = {
    "data": [],
    "last_updated": None,
    "is_loading": False,
    "progress": 0
}

SOURCE_EXCEL_FILE = "RS분석툴.xlsm"
LIMIT_FOR_TEST = 20 # Vercel 테스트용 제한

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/screen')
def screen_stocks():
    global CACHE
    
    if CACHE['is_loading']:
        return jsonify({"status": "loading", "message": "이미 조회 중입니다..."})

    CACHE['progress'] = 0 # Reset progress
    
    # Vercel 환경에서는 백그라운드 스레드가 불안정할 수 있으나,
    # 20개 테스트 모드에서는 충분히 빠르거나, 사용자가 폴링하는 동안 살아있기를 기대함.
    # 만약 Vercel 타임아웃 문제가 발생하면 동기 처리로 바꿔야 함.
    thread = threading.Thread(target=fetch_data_background)
    thread.start()
    
    return jsonify({"status": "started", "message": "데이터 수집을 시작했습니다."})

@app.route('/api/status')
def check_status():
    global CACHE
    state = {
        "status": "idle",
        "progress": CACHE.get('progress', 0)
    }
    
    if CACHE['is_loading']:
        state['status'] = 'loading'
    elif CACHE['last_updated'] is None:
        state['status'] = 'idle'
    else:
        state['status'] = 'done'
        state['data'] = CACHE['data']
        state['count'] = len(CACHE['data'])
        
    return jsonify(state)

def fetch_data_background():
    global CACHE
    if CACHE['is_loading']:
        return

    CACHE['is_loading'] = True
    try:
        print("Starting background fetch...")
        
        def update_progress(p):
            CACHE['progress'] = p
            print(f"Progress: {p}%")
        
        # 1. Excel에서 티커 및 정보 읽기
        ticker_info_list = get_tickers_from_excel(SOURCE_EXCEL_FILE)
        
        # 2. RS 및 시총 계산 (테스트 제한 적용)
        # 이제 ticker_info_list(list of dict)를 넘김
        results = get_market_cap_and_rs(ticker_info_list, limit=LIMIT_FOR_TEST, progress_callback=update_progress)
        
        CACHE['data'] = results
        CACHE['last_updated'] = time.time()
        print(f"Fetch complete. {len(results)} items.")
        
    except Exception as e:
        print(f"Fetch failed: {e}")
    finally:
        CACHE['is_loading'] = False

@app.route('/api/download/basic')
def download_basic():
    if not CACHE['data']:
        return "데이터가 없습니다. 먼저 조회를 해주세요.", 400
        
    df = pd.DataFrame(CACHE['data'])
    # 컬럼 순서 조정: Ticker, MarketCap, MarketCapRank, RS, Sector, Industry
    cols = ["Ticker", "MarketCap", "MarketCapRank", "RS", "Sector", "Industry", "Price"]
    
    # 존재하지 않는 컬럼 예외처리
    available_cols = [c for c in cols if c in df.columns]
    df = df[available_cols]
    
    # In-memory Excel creation for Vercel (Read-only FS compatibility)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    
    filename = f"RS_Result_{int(time.time())}.xlsx"
    
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

if __name__ == '__main__':
    app.run(debug=True, port=8888)
