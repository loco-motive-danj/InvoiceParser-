from dotenv import load_dotenv
import threading
from main import run_parser
from flask import Flask, render_template_string, send_file, redirect, url_for, jsonify, send_from_directory
import pandas as pd
import os
from glob import glob
import logging
from flask_cors import CORS
from main import run_parser
import zipfile
import io

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
@app.route("/")
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
        <br>

        <form action="/outputs" method="get">
            <button type="submit">📂 View Individual Receipt Files</button>
        </form>
        <br>

        <form action="/download-all" method="get">
            <button type="submit">📦 Download All Parsed Receipts (ZIP)</button>
        </form>
        <br>

        <form action="/cleanup" method="post">
            <button type="submit">🧹 Cleanup Parsed Files</button>
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

@app.route('/outputs')
def list_outputs():
    """List all individual parsed Excel files."""
    files = glob(f"{OUTPUT_DIR}/*_parsed.xlsx")
    file_links = [
        f"<li><a href='/download-output/{os.path.basename(f)}'>{os.path.basename(f)}</a></li>"
        for f in files
    ]
    return render_template_string(f"""
        <h2>📁 Individual Parsed Receipts</h2>
        <ul>{''.join(file_links)}</ul>
        <a href="/">⬅️ Back to Dashboard</a>
    """)


@app.route('/download-all')
def download_all_outputs():
    """Download all parsed Excel files as a single ZIP archive."""
    files = glob(f"{OUTPUT_DIR}/*_parsed.xlsx")
    if not files:
        return "No parsed files available yet.", 404

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        for f in files:
            zip_file.write(f, arcname=os.path.basename(f))
    zip_buffer.seek(0)

    return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name="All_Receipts.zip")

@app.route('/cleanup', methods=['POST'])
def cleanup_outputs():
    """Delete all parsed Excel files."""
    files = glob(f"{OUTPUT_DIR}/*_parsed.xlsx")
    deleted = []
    for f in files:
        try:
            os.remove(f)
            deleted.append(os.path.basename(f))
        except Exception as e:
            app.logger.warning(f"Failed to delete {f}: {e}")
    return render_template_string(f"""
        <h2>🧹 Cleanup Complete</h2>
        <p>Deleted {len(deleted)} files:</p>
        <ul>{''.join(f'<li>{name}</li>' for name in deleted)}</ul>
        <a href="/">⬅️ Back to Dashboard</a>
    """)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000, debug=False)
