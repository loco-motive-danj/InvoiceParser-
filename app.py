# app.py

from flask import Flask, render_template_string, send_file, jsonify, request
from flask_cors import CORS
import os, glob, io, zipfile, threading
import pandas as pd
from urllib.parse import quote
from dotenv import load_dotenv
from main import run_parser, OUTPUT_DIR

load_dotenv()
MERGED_FILE = os.path.join(OUTPUT_DIR, "All_Receipts_Combined.xlsx")

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
    return render_template_string("""
        <h2>📊 Receipt Parser Dashboard</h2>
        <form action="/run-parser" method="post"><button>🔁 Parse New Receipts</button></form><br>
        <form action="/merge" method="get"><button>📥 Download Combined Excel</button></form><br>
        <form action="/outputs" method="get"><button>📂 View Individual Receipt Files</button></form><br>
        <form action="/download-all" method="get"><button>📦 Download All Parsed Receipts (ZIP)</button></form><br>
        <form action="/cleanup" method="post"><button>🧹 Cleanup Parsed Files</button></form>
    """)

@app.route('/run-parser', methods=['POST'])
def run_parser_route():
    threading.Thread(target=run_parser).start()
    return render_template_string("""
        <h2>🔁 Parsing started in background</h2>
        <p>Check back in a few minutes for results.</p>
        <a href="/">⬅️ Back to Dashboard</a>
    """)

@app.route('/outputs')
def list_outputs():
    files = glob.glob(f"{OUTPUT_DIR}/*_parsed.xlsx")
    file_links = [
        f"<li><a href='/download/{quote(os.path.basename(f))}'>{os.path.basename(f)}</a></li>"
        for f in files
    ]
    return render_template_string(f"""
        <h2>📁 Individual Parsed Receipts</h2>
        <ul>{''.join(file_links)}</ul>
        <a href="/">⬅️ Back to Dashboard</a>
    """)

@app.route("/download/<filename>")
def download_output_file(filename):
    path = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    else:
        return f"<p>File {filename} not found.</p>", 404

@app.route('/merge')
def merge():
    files = glob.glob(f"{OUTPUT_DIR}/*_parsed.xlsx")
    if not files:
        return "No parsed files available yet.", 404
    all_data = [pd.read_excel(f).assign(Source=os.path.basename(f)) for f in files]
    merged = pd.concat(all_data, ignore_index=True)
    merged.to_excel(MERGED_FILE, index=False)
    return send_file(MERGED_FILE, as_attachment=True)


@app.route('/download-all')
def download_all_outputs():
    files = glob.glob(f"{OUTPUT_DIR}/*_parsed.xlsx")
    if not files:
        return "No parsed files available yet.", 404

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        for f in files:
            zip_file.write(f, arcname=os.path.basename(f))
    zip_buffer.seek(0)

    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name="All_Receipts.zip"
    )
@app.route('/cleanup', methods=['POST'])
def cleanup_outputs():
    files = glob.glob(f"{OUTPUT_DIR}/*_parsed.xlsx")
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000, debug=True)