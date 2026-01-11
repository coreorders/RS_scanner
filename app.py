
from flask import Flask, render_template, jsonify, request, send_file
import pandas as pd
import io
import time
from utils import get_tickers_from_excel

app = Flask(__name__)

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
        
        # [TEST MODE] 100개로 제한 (클라이언트 사이드 부하 고려)
        limit = 100
        if len(data) > limit:
            data = data[:limit]
            
        return jsonify({"status": "success", "data": data, "count": len(data)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/create_excel', methods=['POST'])
def create_excel():
    """
    2. 클라이언트가 완성된 데이터 배열을 보내면 엑셀로 변환해서 다운로드 제공
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

if __name__ == '__main__':
    app.run(debug=True, port=8888)
