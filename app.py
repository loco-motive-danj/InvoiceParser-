from dotenv import load_dotenv
import threading
from main import run_parser
from flask import Flask, render_template_string, send_file, redirect, url_for, jsonify
import pandas as pd
import os
from glob import glob
import logging
from flask_cors import CORS
from main import run_parser

run_parser()

#  Load environment variables
load_dotenv()

#  Configure logging
logging.basicConfig(level=logging.INFO)

#  Flask setup
app = Flask(__name__)
CORS(app)

#  Configurable paths
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "outputs")
MERGED_FILE = os.path.join(OUTPUT_DIR, "All_Receipts_Combined.xlsx")


@app.route('/health')
def health():
    return "OK", 200


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
        run_parser()
        app.logger.info("Parsing completed via API")
        return jsonify({
            "status": "ok",
            "message": "Parsing completed successfully"
        })
    except Exception as e:
        app.logger.error(f"Parsing failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/merge')
def merge():
    """Combine all parsed Excel files into one."""
    files = glob(f"{OUTPUT_DIR}/*_parsed.xlsx")
    if not files:
        return "No parsed files available yet.", 404

    all_data = []
    for f in files:
        df = pd.read_excel(f)
        df["Source"] = os.path.basename(f)
        all_data.append(df)

    merged = pd.concat(all_data, ignore_index=True)
    merged.to_excel(MERGED_FILE, index=False)

    return send_file(MERGED_FILE, as_attachment=True)


@app.post('/api/merge')
def api_merge():
    """JSON API for React: merge parsed files and return download URL."""
    files = glob(f"{OUTPUT_DIR}/*_parsed.xlsx")
    if not files:
        return jsonify({
            "status": "empty",
            "message": "No parsed files available yet."
        }), 404

    all_data = []
    for f in files:
        df = pd.read_excel(f)
        df["Source"] = os.path.basename(f)
        all_data.append(df)

    merged = pd.concat(all_data, ignore_index=True)
    merged.to_excel(MERGED_FILE, index=False)
    return jsonify({
        "status":
        "ok",
        "path":
        MERGED_FILE,
        "download_url":
        url_for('download_combined', _external=False)
    })


@app.get('/download')
def download_combined():
    """Download the combined Excel file if it exists."""
    if not os.path.exists(MERGED_FILE):
        return "No combined file available yet.", 404
    return send_file(MERGED_FILE, as_attachment=True)


@app.route('/run-parser', methods=['POST'])
def run_parser_route():

    def background_task():
        run_parser()
        app.logger.info("Background parsing started")

    threading.Thread(target=background_task).start()
    return render_template_string("""
        <h2>🔁 Parsing started in background</h2>
        <p>Check back in a few minutes for results.</p>
        <a href="/">⬅️ Back to Dashboard</a>
    """)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000, debug=False)
