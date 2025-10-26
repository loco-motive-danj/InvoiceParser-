import os
import io
import json
import time
import glob
import pickle
import pandas as pd
import requests
import mimetypes
from flask import Flask, request, jsonify, send_file
from dotenv import load_dotenv
from google.auth.transport.requests import Request
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload

load_dotenv()


TEMPLATE_FILE_ID = os.getenv("TEMPLATE_FILE_ID")
client_id = os.getenv("GOOGLE_CLIENT_ID")
token_path = os.getenv("TOKEN_PATH", "./token.pkl")

# Load environment variables

print(f"🔍 GOOGLE_ACCOUNT_KEY loaded: {'GOOGLE_ACCOUNT_KEY' in os.environ}")


SCOPES = ["https://www.googleapis.com/auth/drive"]
AZURE_KEY = os.getenv("AZURE_KEY")
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
FOLDER_ID = os.getenv("FOLDER_ID", "1gBOXAU9b1zSt06c-1YPQcmPiu02zTdXZ")
MODEL = "prebuilt-receipt"

app = Flask(__name__)
print(f"🔍 AZURE_ENDPOINT: {AZURE_ENDPOINT}")
print(f"🔍 AZURE_KEY: {AZURE_KEY}")
print(f"🔍 MODEL: {MODEL}")


# 🔐 OAuth-based Google Drive auth
def get_drive_service():
    # Load service account credentials from environment variable
    service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT"))
    raw_key = os.getenv("GOOGLE_SERVICE_ACCOUNT")
    if not raw_key:
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT is missing from environment")
    creds = service_account.Credentials.from_service_account_info(service_account_info)
    drive = build('drive', 'v3', credentials=creds)
    return drive


drive = get_drive_service()

def upload_via_template(local_path, new_filename, drive_service, folder_id, template_id):
    # Step 1: Copy the template
    file_metadata = {
        "name": new_filename,
        "parents": [folder_id]
    }
   
#drive = build("drive", "v3", credentials=creds)


# 📂 List files in a folder
def list_files(drive_service, folder_id):
    results = drive_service.files().list(
    q=f"'{folder_id}' in parents and trashed = false",
    fields="files(id, name, mimeType)"
).execute()
    return results.get("files", []) 


# 📥 Download a file by ID
def download_file(drive_service, file_id, filename):
    file = drive_service.files().get(fileId=file_id, fields="mimeType").execute()
    mime_type = file["mimeType"]

    if mime_type == "application/vnd.google-apps.spreadsheet":
        request = drive_service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        request = drive_service.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    local_path = os.path.join("downloads", filename)
    os.makedirs("downloads", exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(fh.getbuffer())
    return local_path


# Analyze receipt

def analyze_receipt_dynamic(file_or_bytes, name="receipt"):
    """
    Sends a PDF or image (JPEG/PNG) to Azure Form Recognizer and returns the parsed result.
    Accepts either a file path (str) or raw bytes (bytes).
    Automatically handles rate limits (429), retries, and polling.
    """
    endpoint = os.getenv("AZURE_ENDPOINT")
    key = os.getenv("AZURE_KEY")
    model = os.getenv("MODEL", "prebuilt-receipt")

    if not endpoint or not endpoint.startswith("http"):
        raise ValueError(f"AZURE_ENDPOINT is invalid: {endpoint}")

    # Detect content and MIME type
    if isinstance(file_or_bytes, str):
        mime_type, _ = mimetypes.guess_type(file_or_bytes)
        with open(file_or_bytes, "rb") as f:
            content = f.read()
    else:
        content = file_or_bytes
        mime_type = "application/pdf" if content[:4] == b"%PDF" else "image/jpeg"

    if not mime_type:
        raise ValueError("Unable to determine MIME type for Azure request")

    headers = {
        "Content-Type": mime_type,
        "Ocp-Apim-Subscription-Key": key
    }

    print(f"📤 Sending {len(content)} bytes as {mime_type} to Azure")

    url = f"{endpoint}/formrecognizer/documentModels/{model}:analyze?api-version=2023-07-31"

    # Retry logic for 429 rate limit
    for attempt in range(3):
        resp = requests.post(url, headers=headers, data=content)
        if resp.status_code == 202:
            break
        elif resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            print(f"⏳ Rate limit hit. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            print(f"❌ Azure response: {resp.text}")
            raise Exception(f"Azure request failed ({resp.status_code})")
    else:
        raise Exception("Azure request failed after multiple retries")

    result_url = resp.headers["operation-location"]
    max_wait = 60  # seconds
    interval = 2
    attempts = max_wait // interval

    for i in range(attempts):
        print(f"⏳ Polling Azure... attempt {i+1}")
        result_resp = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": key})
        result_json = result_resp.json()
        status = result_json.get("status")

        if status == "succeeded":
            return result_json
        elif status == "failed":
            print(f"❌ Azure result failed: {result_json}")
            raise Exception("Azure analysis failed")

        time.sleep(interval)

    raise Exception("Azure polling timed out")



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
    all_files = [
    f for f in glob.glob(os.path.join(output_dir, "*.xlsx"))
    if not f.endswith("All_Receipts_Combined.xlsx")
]
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
    files = list_files(drive, FOLDER_ID)
    print(f"📁 Found {len(files)} files")

    for f in files:
        name = f["name"]
        mime = f["mimeType"]

        if name.endswith(".xlsx") or "_parsed" in name:
            print(f"⏭️ Skipping {name} (already parsed or Excel)")
            continue

        if mime == "application/vnd.google-apps.spreadsheet":
            print(f"⏭️ Skipping Google Sheet: {name}")
            continue

        print(f"🔍 Processing {name}...")
        content = download_file(drive, f["id"], name)
        parsed = analyze_receipt_dynamic(content)
        out_path = parse_and_save(parsed, name)

        if out_path:
            print(f"✅ Parsed and saved: {out_path}")
            upload_via_template(
                local_path=out_path,
                new_filename=os.path.basename(out_path),
                drive_service=drive,
                folder_id=FOLDER_ID,
                template_id=TEMPLATE_FILE_ID
            )
        else:
            print(f"⚠️ No data found for {name}")


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
        parsed = analyze_receipt_dynamic(content)
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
    try:
        run_parser()
    except Exception as e:
        import traceback
    print("🔥 Uncaught exception:")
    traceback.print_exc()
    raise
