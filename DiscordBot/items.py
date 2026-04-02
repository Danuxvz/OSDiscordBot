import os
import io
import csv
import json
import asyncio
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from .utils import local_md5

ITEMS_TABLE_FILE = "items_table.json"
IMAGES_DIR = "ENTES"
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "")
SHEET_ID = os.getenv("SHEET_ID", "1dMUMUXjn22L2nYHFHKmDBObD1VskyVruzh-OM9IexLk")
DEFAULT_SHEET_CSV = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=0"

def parse_csv_to_items_table(csv_text):
    reader = csv.DictReader(io.StringIO(csv_text))
    reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]
    table = {}
    for row in reader:
        ID = (row.get("id") or "").strip()
        tier = (row.get("tier") or "").strip().upper() or "E"
        name = (row.get("name") or "").strip()
        rutas = (row.get("rutas") or "").strip()
        clase = (row.get("clase") or "").strip()
        elemento = (row.get("elemento") or "").strip()
        if not rutas:
            continue
        route_key = rutas
        tier_key = tier
        entry = {
            "id": ID,
            "tier": tier_key,
            "name": name,
            "ruta": route_key,
            "clase": clase,
            "elemento": elemento,
        }
        table.setdefault(route_key, {}).setdefault(tier_key, []).append(entry)
    return table

def save_items_table(table):
    with open(ITEMS_TABLE_FILE, "w", encoding="utf-8") as f:
        json.dump(table, f, indent=4, ensure_ascii=False)

def load_items_table():
    if os.path.exists(ITEMS_TABLE_FILE):
        with open(ITEMS_TABLE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

async def refresh_items_table():
    url = SHEET_CSV_URL or DEFAULT_SHEET_CSV
    print(f"Refreshing items table from: {url}")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        if not resp.text.strip():
            print("⚠️ CSV response is empty!")
            return False
        csv_bytes = resp.content
        csv_text = csv_bytes.decode("utf-8-sig")
        table = parse_csv_to_items_table(csv_text)
        save_items_table(table)
        print(f"✅ Items table refreshed: {len(table)} routes loaded.")
        await asyncio.to_thread(refresh_images_from_drive)
        print("✅ Images table refreshed")
        return True
    except Exception as e:
        print("Failed to refresh items table:", e)
        return False

def refresh_images_from_drive():
    SERVICE_ACCOUNT_FILE = "osbotdrive-b0e15f00170f.json"
    DRIVE_FOLDER_ID = "1FBoqcvBle140D-7rV5CPcod-l2ZN0ClJ"
    OUTPUT_DIR = IMAGES_DIR

    print("🔄 Starting Drive image sync...")
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        service = build('drive', 'v3', credentials=creds)
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        def list_all_files(folder_id):
            items = []
            page_token = None
            while True:
                response = service.files().list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    fields="nextPageToken, files(id, name, mimeType, md5Checksum, size)",
                    pageSize=1000,
                    pageToken=page_token
                ).execute()
                items.extend(response.get("files", []))
                page_token = response.get("nextPageToken")
                if not page_token:
                    break
            return items

        def sync_folder(folder_id, local_path):
            os.makedirs(local_path, exist_ok=True)
            items = list_all_files(folder_id)
            for item in items:
                file_id = item["id"]
                name = item["name"]
                mime = item["mimeType"]
                dest_path = os.path.join(local_path, name)
                if mime == "application/vnd.google-apps.folder":
                    sync_folder(file_id, dest_path)
                    continue
                drive_md5 = item.get("md5Checksum")
                local_md5_val = local_md5(dest_path)
                if local_md5_val == drive_md5:
                    print(f"✔ Skipped (unchanged): {dest_path}")
                    continue
                print(f"⬇ Downloading: {dest_path}")
                request = service.files().get_media(fileId=file_id)
                with open(dest_path, "wb") as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
        print("📁 Syncing from Drive folder:", DRIVE_FOLDER_ID)
        sync_folder(DRIVE_FOLDER_ID, OUTPUT_DIR)
        print("✅ Images refreshed from Drive (incremental sync).")
        return True
    except Exception as e:
        print(f"❌ Drive sync failed: {e}")
        return False