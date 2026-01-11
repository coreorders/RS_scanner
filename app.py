
from flask import Flask, render_template, send_from_directory
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

# Static 폴더의 result.json을 서빙하기 위한 라우트 (혹은 static 기본 동작 활용)
@app.route('/data/<path:filename>')
def serve_data(filename):
    return send_from_directory('static', filename)

if __name__ == '__main__':
    app.run(debug=True, port=8888)
