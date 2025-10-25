import os
import io
import json
import time
import glob
import pickle
import pandas as pd
import requests
from flask import Flask, request, jsonify, send_file
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# Load environment variables
load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/drive"]
AZURE_KEY = os.getenv("AZURE_KEY")
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
FOLDER_ID = os.getenv("FOLDER_ID", "1gBOXAU9b1zSt06c-1YPQcmPiu02zTdXZ")
MODEL = "prebuilt-receipt"

app = Flask(__name__)


# 🔐 OAuth-based Google Drive auth
def get_drive_service():
    creds = None
    if os.path.exists("token.pkl"):
        with open("token.pkl", "rb") as token:
            creds = pickle.load(token)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(
            "client_secret.json", SCOPES)
        auth_url, _ = flow.authorization_url(prompt="consent")
        print("🔗 Visit this URL to authorize:")
        print(auth_url)
        code = input("📥 Paste the authorization code here: ")
        flow.fetch_token(code=code)
        creds = flow.credentials
        with open("token.pkl", "wb") as token:
            pickle.dump(creds, token)

    return build("drive", "v3", credentials=creds)


drive = get_drive_service()


# 📂 List files in a folder
def list_files(folder_id):
    q = f"'{folder_id}' in parents and trashed=false"
    res = drive.files().list(q=q).execute()
    return res.get("files", [])


# 📥 Download a file by ID
def download_file(file_id, name):
    req = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()


# 🧠 Analyze receipt with Azure
def analyze_receipt(file_bytes, max_retries=5):
    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_KEY,
        "Content-Type": "application/octet-stream"
    }
    params = {"api-version": "2023-07-31"}

    for attempt in range(max_retries):
        resp = requests.post(
            f"{AZURE_ENDPOINT}formrecognizer/documentModels/{MODEL}:analyze",
            headers=headers,
            params=params,
            data=file_bytes)

        if resp.status_code in (200, 202):
            break

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            print(f"⏳ Rate limit hit. Retrying in {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            print("Azure POST failed:", resp.status_code, resp.text)
            raise Exception(f"Azure request failed ({resp.status_code})")
    else:
        raise Exception("Exceeded max retries due to rate limiting.")

    op = resp.headers.get("operation-location")
    if not op:
        raise Exception("No operation-location header returned from Azure.")

    while True:
        r = requests.get(op, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY})
        data = r.json()
        if data.get("status") == "succeeded":
            return data
        elif data.get("status") == "failed":
            raise Exception("Azure analysis failed.")
        time.sleep(2)


# 📊 Parse and save to Excel
def parse_and_save(data, name):
    docs = data["analyzeResult"]["documents"]
    if not docs:
        return None
    items = docs[0]["fields"]["Items"]["valueArray"]
    rows = []
    for it in items:
        obj = it["valueObject"]
        rows.append({
            "Description":
            obj.get("Description", {}).get("valueString", ""),
            "Quantity":
            obj.get("Quantity", {}).get("valueNumber", 1),
            "Total":
            obj.get("TotalPrice", {}).get("valueNumber", 0)
        })

    project_name = os.path.splitext(name)[0].split("_")[0]
    df = pd.DataFrame(rows)
    df["Project"] = project_name
    os.makedirs("outputs", exist_ok=True)
    out_path = f"outputs/{os.path.splitext(name)[0]}_parsed.xlsx"
    df.to_excel(out_path, index=False)
    return out_path


# ☁️ Upload to Drive
def upload_to_drive(local_path, folder_id):
    file_metadata = {
        "name":
        os.path.basename(local_path),
        "parents": [folder_id],
        "mimeType":
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }
    media = MediaFileUpload(local_path, mimetype=file_metadata["mimeType"])
    try:
        drive.files().create(body=file_metadata, media_body=media,
                             fields="id").execute()
        print(
            f"✅ Uploaded {os.path.basename(local_path)} to Drive folder {folder_id}"
        )
    except Exception as e:
        print(f"⚠️ Upload failed: {e}")


# 📁 Merge all Excel outputs
def merge_excels(output_dir="outputs"):
    all_files = glob.glob(os.path.join(output_dir, "*_parsed.xlsx"))
    if not all_files:
        print("ℹ️ No parsed files to merge.")
        return
    dfs = [pd.read_excel(f) for f in all_files]
    merged = pd.concat(dfs, ignore_index=True)
    merged.to_excel(os.path.join(output_dir, "All_Receipts_Combined.xlsx"),
                    index=False)
    print("🧾 Combined Excel saved as All_Receipts_Combined.xlsx")


# 🚀 Run parser across Drive folder
def run_parser():
    print("🚀 run_parser() started")
    print("📂 Using folder ID:", FOLDER_ID)
    files = list_files(FOLDER_ID)
    print(f"📁 Found {len(files)} files")
    for f in files:
        name = f["name"]
        if name.endswith(".xlsx") or "_parsed" in name:
            print(f"⏭️ Skipping {name}")
            continue
        print(f"🔍 Processing {name}...")
        content = download_file(f["id"], f["name"])
        parsed = analyze_receipt(content)
        out_path = parse_and_save(parsed, f["name"])
        if out_path:
            print(f"✅ Parsed and saved: {out_path}")
            upload_to_drive(out_path, FOLDER_ID)
        else:
            print(f"⚠️ No data found for {f['name']}")
    merge_excels()


# 🌐 Flask routes
@app.route("/")
def home():
    return "<h2>🧾 Receipt Parser Running — v3.2</h2>"


@app.route("/download")
def download_results():
    path = "outputs/All_Receipts_Combined.xlsx"
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    else:
        return "<p>No results found yet.</p>"


@app.route("/parse", methods=["POST"])
def parse_uploaded_receipt():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file uploaded"}), 400
    content = file.read()
    try:
        parsed = analyze_receipt(content)
        name = file.filename or "receipt"
        out_path = parse_and_save(parsed, name)
        return jsonify({"status": "success", "output": out_path})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# 🏁 Entry point
if __name__ == "__main__":
    if os.getenv("GITHUB_ACTIONS") == "true":
        print("✅ Running parser in CI/CD mode")
        run_parser()
    else:
        print("🌐 Starting Flask server")
        app.run(host="0.0.0.0", port=8080, debug=True)
