from flask import Flask, render_template, request, jsonify
import requests
import os

app = Flask(__name__)

# 服务器地址
SERVER_URL = "http://localhost:8084"

# 添加静态文件访问支持
@app.route('/static/graphs/')
@app.route('/static/graphs/<path:filename>')
def graphs_static(filename=''):
    from flask import send_from_directory, abort
    import os
    # 如果没有指定文件名，默认显示索引页面
    if filename == '':
        filename = 'index.html'
    # 检查文件是否存在
    file_path = os.path.join('static/graphs', filename)
    if os.path.exists(file_path):
        return send_from_directory('static/graphs', filename)
    else:
        abort(404)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/import', methods=['POST'])
def import_csv():
    try:
        csv_file = request.form.get('csvFile')
        device_id = request.form.get('deviceId', 'root.example.exampledev')

        # 调用后端导入API
        response = requests.post(f"{SERVER_URL}/import?csvFile={csv_file}&deviceId={device_id}")

        if response.status_code == 200:
            return jsonify({"success": True, "message": response.text})
        else:
            return jsonify({"success": False, "error": response.text}), response.status_code
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/statistic')
def statistic():
    try:
        device_id = request.args.get('deviceId', 'root.example.exampledev')

        # 调用后端统计API
        response = requests.get(f"{SERVER_URL}/statistic?deviceId={device_id}")

        if response.status_code == 200:
            return jsonify({"success": True, "data": response.text})
        else:
            return jsonify({"success": False, "error": response.text}), response.status_code
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/correlation')
def correlation():
    try:
        device_id = request.args.get('deviceId', 'root.example.exampledev')

        # 调用后端相关性API
        response = requests.get(f"{SERVER_URL}/correlation?deviceId={device_id}")

        if response.status_code == 200:
            return jsonify({"success": True, "data": response.text})
        else:
            return jsonify({"success": False, "error": response.text}), response.status_code
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/graph')
def graph():
    try:
        device_id = request.args.get('deviceId', 'root.example.exampledev')

        # 调用后端图表API
        response = requests.get(f"{SERVER_URL}/graph?deviceId={device_id}")

        if response.status_code == 200:
            return jsonify({"success": True, "data": response.text})
        else:
            return jsonify({"success": False, "error": response.text}), response.status_code
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/condition')
def condition():
    try:
        device_id = request.args.get('deviceId', 'root.example.exampledev')

        # 调用后端条件分析API
        response = requests.get(f"{SERVER_URL}/condition?deviceId={device_id}")

        if response.status_code == 200:
            return jsonify({"success": True, "data": response.text})
        else:
            return jsonify({"success": False, "error": response.text}), response.status_code
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
