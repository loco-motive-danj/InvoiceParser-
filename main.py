# main.py

import os, io, json, time, glob, shutil, mimetypes
import pandas as pd
from openpyxl import load_workbook
import requests
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

load_dotenv()
branch = os.getenv("BRANCH", "demo")
env_file = ".env.prod" if branch == "prod" else ".env.demo"
load_dotenv(dotenv_path=env_file)

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "outputs")
AZURE_KEY = os.getenv("AZURE_KEY")
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
MODEL = os.getenv("MODEL", "prebuilt-receipt")
FOLDER_ID = os.getenv("FOLDER_ID")
TEMPLATE_FILE_ID = os.getenv("TEMPLATE_FILE_ID")

def get_drive_service():
    service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT"))
    creds = service_account.Credentials.from_service_account_info(service_account_info)
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

drive = get_drive_service()

def list_files(drive_service, folder_id):
    results = drive_service.files().list(
        q=f"'{folder_id}' in parents and trashed = false",
        fields="files(id, name, mimeType)"
    ).execute()
    return results.get("files", [])

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

def analyze_receipt_dynamic(file_or_bytes, name="receipt"):
    if isinstance(file_or_bytes, str):
        mime_type, _ = mimetypes.guess_type(file_or_bytes)
        with open(file_or_bytes, "rb") as f:
            content = f.read()
    else:
        content = file_or_bytes
        mime_type = "application/pdf" if content[:4] == b"%PDF" else "image/jpeg"
    headers = {
        "Content-Type": mime_type,
        "Ocp-Apim-Subscription-Key": AZURE_KEY
    }
    url = f"{AZURE_ENDPOINT}/formrecognizer/documentModels/{MODEL}:analyze?api-version=2023-07-31"
    for attempt in range(3):
        resp = requests.post(url, headers=headers, data=content)
        if resp.status_code == 202:
            break
        elif resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            time.sleep(retry_after)
        else:
            raise Exception(f"Azure request failed ({resp.status_code})")
    else:
        raise Exception("Azure request failed after multiple retries")
    result_url = resp.headers["operation-location"]
    for i in range(250):
        result_resp = requests.get(result_url, headers={"Ocp-Apim-Subscription-Key": AZURE_KEY})
        result_json = result_resp.json()
        status = result_json.get("status")
        if status == "succeeded":
            return result_json
        elif status == "failed":
            raise Exception("Azure analysis failed")
        time.sleep(2)
    raise Exception("Azure polling timed out")

def parse_and_save(data, name):
    docs = data["analyzeResult"]["documents"]
    if not docs:
        return None
    items = docs[0]["fields"]["Items"]["valueArray"]
    rows = []
    for it in items:
        obj = it["valueObject"]
        rows.append({
            "Description": obj.get("Description", {}).get("valueString", ""),
            "Quantity": obj.get("Quantity", {}).get("valueNumber", 1),
            "Total": obj.get("TotalPrice", {}).get("valueNumber", 0)
        })
    receipt_date = docs[0]["fields"].get("TransactionDate", {}).get("valueDate", "")
    project_name = os.path.splitext(name)[0].split("_")[0]
    df = pd.DataFrame(rows)
    df["Project"] = project_name
    df["Date"] = receipt_date
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{os.path.splitext(name)[0]}_parsed.xlsx")
    df.to_excel(out_path, index=False)
    wb = load_workbook(out_path)
    ws = wb.active
    for col in ws.columns:
        max_length = max(len(str(cell.value)) if cell.value else 0 for cell in col)
        ws.column_dimensions[col[0].column_letter].width = max(max_length + 2, 15)
    wb.save(out_path)
    return out_path

def run_parser():
    files = list_files(drive, FOLDER_ID)
    for f in files:
        name = f["name"]
        mime = f["mimeType"]
        if name.endswith(".xlsx") or "_parsed" in name or mime == "application/vnd.google-apps.spreadsheet":
            continue
        content = download_file(drive, f["id"], name)
        parsed = analyze_receipt_dynamic(content)
        out_path = parse_and_save(parsed, name)
        if out_path:
            print(f"✅ Parsed and saved: {out_path}")
