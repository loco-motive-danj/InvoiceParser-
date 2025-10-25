from dotenv import load_dotenv
import threading
from main import run_parser
from flask import Flask, render_template_string, send_file, redirect, url_for, jsonify
import pandas as pd
import os
from glob import glob
#import subprocess
from flask_cors import CORS

load_dotenv()
app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
    return render_template_string("""
        <h2>📊 Receipt Parser Dashboard</h2>
        <p>Receipts are automatically parsed in the background <em>or</em> you can trigger parsing manually below.</p>
        <form action="/run-parser" method="post">
            <button type="submit">🔁 Parse New Receipts</button>
        </form>
        <br>
        <form action="/merge" method="get">
            <button type="submit">📥 Download Combined Excel</button>
        </form>
    """)

@app.post('/api/run-parser')
def api_run_parser():
    """JSON API for React: trigger parsing workflow."""
    try:
        subprocess.run(["python", "main.py"], check=True)
        return jsonify({"status": "ok", "message": "Parsing completed successfully"})
    except subprocess.CalledProcessError as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/merge')
def merge():
    """Combine all parsed Excel files into one."""
    files = glob("outputs/*_parsed.xlsx")
    if not files:
        return "No parsed files available yet.", 404

    all_data = []
    for f in files:
        df = pd.read_excel(f)
        df["Source"] = os.path.basename(f)
        all_data.append(df)

    merged = pd.concat(all_data, ignore_index=True)
    out_path = "outputs/All_Receipts_Combined.xlsx"
    merged.to_excel(out_path, index=False)

    return send_file(out_path, as_attachment=True)


@app.post('/api/merge')
def api_merge():
    """JSON API for React: merge parsed files and return download URL."""
    files = glob("outputs/*_parsed.xlsx")
    if not files:
        return jsonify({"status": "empty", "message": "No parsed files available yet."}), 404

    all_data = []
    for f in files:
        df = pd.read_excel(f)
        df["Source"] = os.path.basename(f)
        all_data.append(df)

    merged = pd.concat(all_data, ignore_index=True)
    out_path = "outputs/All_Receipts_Combined.xlsx"
    merged.to_excel(out_path, index=False)
    return jsonify({
        "status": "ok",
        "path": out_path,
        "download_url": url_for('download_combined', _external=False)
    })


@app.get('/download')
def download_combined():
    """Download the combined Excel file if it exists."""
    out_path = "outputs/All_Receipts_Combined.xlsx"
    if not os.path.exists(out_path):
        return "No combined file available yet.", 404
    return send_file(out_path, as_attachment=True)

@app.route('/run-parser', methods=['POST'])
def run_parser_route():
    def background_task():
        run_parser()

    threading.Thread(target=background_task).start()
    return render_template_string("""
        <h2>🔁 Parsing started in background</h2>
        <p>Check back in a few minutes for results.</p>
        <a href="/">⬅️ Back to Dashboard</a>
    """)

if __name__ == '__main__':
    # host 0.0.0.0 ensures it works on Render or Replit
    app.run(host='0.0.0.0', port=10000)