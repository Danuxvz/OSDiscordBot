# main.py
import os
import io
import csv
import json
import random
import requests
import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo  # Python 3.9+
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
import re
import difflib
import hashlib
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from supabase import create_client, Client

# -----------------------------
# Configuration
# -----------------------------
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "")
SHEET_ID = os.getenv("SHEET_ID", "1dMUMUXjn22L2nYHFHKmDBObD1VskyVruzh-OM9IexLk")
DEFAULT_SHEET_CSV = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=0"
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
else:
    print("⚠️ Supabase not configured. Some features disabled.")

ITEMS_TABLE_FILE = "items_table.json"
CONFIG_FILE = "config.json"
LOG_DIR = "weekly_logs"
IMAGES_DIR = "ENTES"
EXPORTS_DIR = "exports"

os.makedirs(EXPORTS_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(IMAGES_DIR, exist_ok=True)


# -----------------------------
# Discord bot setup
# -----------------------------
intents = discord.Intents.default()
intents.message_content = True

def get_prefix(bot, message):
    if message.guild is None:
        return ">"
    cfg = get_guild_cfg(message.guild.id)
    return cfg.get("prefix", ">")

bot = commands.Bot(command_prefix=get_prefix, intents=intents, case_insensitive=True, help_command=None)


# -----------------------------
# Helper functions (timezone)
# -----------------------------
LOCAL_TZ = ZoneInfo("America/New_York")

def get_local_now():
    """Return current time in America/New_York timezone."""
    return datetime.now(LOCAL_TZ)

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_dt(value: str):
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


# -----------------------------
# CONFIG SYNC SYSTEM (Supabase + Cache) – one row per guild
# -----------------------------
CONFIG_SYNC_INTERVAL = 300

config_cache = {}          # guild_id -> {"data": {...}, "updated_at": "..."}
config_dirty = set()       # guild_id (to push)


async def load_config_from_db():
    global config_cache
    if not supabase:
        return

    try:
        res = supabase.table("config").select("guild_id,data,updated_at").execute()
        data = res.data or []

        for row in data:
            gid = str(row["guild_id"])
            remote_data = row.get("data") or {}
            remote_updated_at = row.get("updated_at") or utc_now_iso()

            current = config_cache.get(gid)
            if not current or parse_dt(remote_updated_at) > parse_dt(current["updated_at"]):
                config_cache[gid] = {
                    "data": dict(remote_data),
                    "updated_at": remote_updated_at
                }

        print(f"[CONFIG] Loaded {len(data)} guilds from Supabase")

    except Exception as e:
        print("[CONFIG] Failed to load from DB:", e)


def get_guild_cfg(guild_id):
    gid = str(guild_id)

    if gid not in config_cache:
        config_cache[gid] = {
            "data": {},
            "updated_at": utc_now_iso()
        }

    data = config_cache[gid]["data"]

    # Ensure defaults
    if "busquedas_channel" not in data:
        data["busquedas_channel"] = None
    if "operations_channel" not in data:
        data["operations_channel"] = None
    if "last_week" not in data:
        data["last_week"] = None
    if "scan_hour" not in data:
        data["scan_hour"] = 17
    if "prefix" not in data:
        data["prefix"] = ">"

    return data


def set_config(guild_id, key, value):
    gid = str(guild_id)

    if gid not in config_cache:
        config_cache[gid] = {
            "data": {},
            "updated_at": utc_now_iso()
        }

    config_cache[gid]["data"][key] = value
    config_cache[gid]["updated_at"] = utc_now_iso()
    config_dirty.add(gid)


async def push_dirty_to_db():
    global config_dirty

    if not supabase or not config_dirty:
        return

    payload = []
    for gid in list(config_dirty):
        entry = config_cache.get(gid)
        if not entry:
            continue
        payload.append({
            "guild_id": gid,
            "data": entry["data"],
            "updated_at": entry["updated_at"]
        })

    if not payload:
        return

    try:
        supabase.table("config").upsert(payload, on_conflict="guild_id").execute()
        print(f"[CONFIG] Pushed {len(payload)} guild configs")
        config_dirty.clear()
    except Exception as e:
        print("[CONFIG] Push failed:", e)


async def pull_updates_from_db():
    if not supabase:
        return

    try:
        res = supabase.table("config").select("guild_id,data,updated_at").execute()
        data = res.data or []

        for row in data:
            gid = str(row["guild_id"])
            remote_data = row.get("data") or {}
            remote_updated_at = row.get("updated_at") or utc_now_iso()

            local = config_cache.get(gid)
            if not local or parse_dt(remote_updated_at) > parse_dt(local["updated_at"]):
                config_cache[gid] = {
                    "data": dict(remote_data),
                    "updated_at": remote_updated_at
                }

        print("[CONFIG] Pulled updates")

    except Exception as e:
        print("[CONFIG] Pull failed:", e)


@tasks.loop(seconds=CONFIG_SYNC_INTERVAL)
async def config_sync_loop():
    await push_dirty_to_db()
    await pull_updates_from_db()


# -----------------------------
# Helper functions (continued)
# -----------------------------
def get_current_week_range():
    """Return start and end dates of current week (Mon-Sun) in local time."""
    now = get_local_now()
    start = now - timedelta(days=now.weekday())  # Monday
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=6)
    return start, end

def get_current_week_start_str():
    start, _ = get_current_week_range()
    return start.date().isoformat()

def get_weekly_log_path(start, guild_id):
    """Return path for weekly log file for a given week start and guild."""
    os.makedirs(LOG_DIR, exist_ok=True)
    filename = f"{guild_id}_{start.strftime('%Y%m%d')}.json"
    return os.path.join(LOG_DIR, filename)

async def load_weekly_log_from_db(guild_id, week_start_str):
    """Try to load weekly log from Supabase; return dict or None."""
    if not supabase:
        return None

    try:
        res = supabase.table("weekly_logs") \
            .select("data") \
            .eq("guild_id", str(guild_id)) \
            .eq("week_start", week_start_str) \
            .maybe_single() \
            .execute()

        if res.data and res.data.get("data"):
            return res.data["data"]
    except Exception as e:
        print(f"[SUPABASE] Failed to load weekly log: {e}")

    return None

def sanitize_text(text):
    """Remove emojis but preserve newlines and basic formatting."""
    # Remove emoji ranges
    text = re.sub(r'[\U00010000-\U0010FFFF]', '', text)
    text = re.sub(r'[\u2600-\u26FF\u2700-\u27BF]', '', text)

    # Keep newlines; only collapse multiple spaces per line
    lines = text.splitlines()
    cleaned_lines = []
    for line in lines:
        line = re.sub(r'\s+', ' ', line)
        cleaned_lines.append(line.strip())
    return "\n".join(cleaned_lines).strip()

async def upload_characters_csv_to_supabase(csv_path, table_name="characters_export"):
    """
    Reads characters.csv and UPSERTS rows to Supabase.
    """
    try:
        rows = []

        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)

        if not rows:
            print("[SUPABASE] characters.csv has no rows. Skipping.")
            return

        # Perform UPSERT
        (
            supabase
            .table(table_name)
            .upsert(rows, on_conflict=["name"])
            .execute()
        )

        print(f"[SUPABASE] Upserted {len(rows)} rows into '{table_name}'.")
    except Exception as e:
        print("[SUPABASE] Error uploading CSV:", e)


# -----------------------------
# Items table management (download and load)
# -----------------------------
def parse_csv_to_items_table(csv_text):
    reader = csv.DictReader(io.StringIO(csv_text))
    # normalize headers to remove spaces and lowercase
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

async def refresh_items_table(session=None):
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

        def local_md5(path):
            if not os.path.exists(path):
                return None
            md5 = hashlib.md5()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    md5.update(chunk)
            return md5.hexdigest()

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
                local_md5_value = local_md5(dest_path)
                if local_md5_value == drive_md5:
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
    except Exception as e:
        print(f"❌ Drive sync failed: {e}")
        return False
    return True

# -----------------------------
# Routing / fuzzy matching
# -----------------------------
VALID_ROUTES = [
    "Rio Barakawa", "Academia Mofunoakabe", "El Cluster", "Templo Fudakudai", "Bosque de Onigashima", "Crimson Light District",
    "Academia Saint George", "The Fae Parliament", "Everything Hill", "St. Peter Cathedral", "Thames River", "The Botanical Forest",
    "Academia St Peter", "Gehenna Door", "Abandoned Colosseum", "Elysian Garden", "Central Church", "Hopeless River", "Toy Factory"
]

ROUTE_ALIASES = {
    "Rio Barakawa": [
        "barakawa river",
        "rio barakawa",
        "river barakawa",
        "río barakawa"
    ],
    "Academia Mofunoakabe": [
        "mofuno",
        "academia mofuno",
        "mofunoakabe",
        "mofunoakabe academy"
    ],
    "El Cluster": [
        "cluster",
        "el cluster",
        "the cluster"
    ],
    "Templo Fudakudai": [
        "fudakudai temple",
        "templo fudakudai",
        "temple fudakudai"
    ],
    "Bosque de Onigashima": [
        "onigashima forest",
        "bosque onigashima",
        "bosque de onigashima",
        "forest of onigashima",
        "onigashima"
    ],
    "Crimson Light District": [
        "crimson district",
        "crimson light",
        "crimson light district",
        "red light district",
        "red light",
        "distrito luz carmesí",
        "distrito carmesí"
    ],
    "Academia Saint George": [
        "saint george academy",
        "academia san george",
        "san george academy",
        "st. george academy"
    ],
    "The Fae Parliament": [
        "fae parliament",
        "the fae parliament",
        "parlamento feérico",
        "parlamento fae"
    ],
    "Everything Hill": [
        "everything hill",
        "colina del todo",
        "la colina todo"
    ],
    "St. Peter Cathedral": [
        "st peter cathedral",
        "st. peter cathedral",
        "catedral san pedro",
        "cathedral st peter",
        "san pedro cathedral"
    ],
    "Thames River": [
        "river thames",
        "thames",
        "rio thames",
        "río thames"
    ],
    "The Botanical Forest": [
        "botanical forest",
        "forest botanical",
        "bosque botánico",
        "jardin botánico",
        "botanical garden"
    ],
    "Academia St Peter": [
        "st peter academy",
        "st. peter academy",
        "academia st peter",
        "academia san pedro",
        "san pedro academy"
    ],
    "Gehenna Door": [
        "gehenna door",
        "puerta gehenna",
        "la puerta gehenna",
        "door of gehenna"
    ],
    "Abandoned Colosseum": [
        "abandoned colosseum",
        "coliseo abandonado",
        "coliseo viejo",
        "colosseum"
    ],
    "Elysian Garden": [
        "elysian garden",
        "jardín elíseo",
        "jardin elyseo",
        "jardín elíseo"
    ],
    "Central Church": [
        "central church",
        "iglesia central",
        "la iglesia central"
    ],
    "Hopeless River": [
        "hopeless river",
        "río sin esperanza",
        "rio sin esperanza",
        "Hopeless"
    ],
    "Toy Factory": [
        "toy factory",
        "fábrica de juguetes",
        "fabrica de juguetes",
        "la fábrica de juguetes",
        "jugeteria",
        "Santa's little secret stash of the good stuff",
    ]
}

ALIAS_MAP = {}
for canonical, alias_list in ROUTE_ALIASES.items():
    for a in alias_list:
        ALIAS_MAP[a.lower()] = canonical

def match_route(user_route, items_table, cutoff=0.7):
    if not user_route:
        return None

    normalized = user_route.strip().lower()

    # --- 1) Alias exact match ---
    if normalized in ALIAS_MAP:
        return ALIAS_MAP[normalized]

    # --- 2) Fuzzy match on aliases ---
    alias_candidates = list(ALIAS_MAP.keys())
    matches = difflib.get_close_matches(normalized, alias_candidates, n=1, cutoff=cutoff)
    if matches:
        return ALIAS_MAP[matches[0]]

    # --- 3) Exact route name match ---
    if user_route in VALID_ROUTES:
        return user_route

    # --- 4) Fuzzy match on VALID_ROUTES ---
    matches = difflib.get_close_matches(user_route, VALID_ROUTES, n=1, cutoff=cutoff)
    if matches:
        return matches[0]

    # --- 5) Exact key in items_table ---
    if user_route in items_table:
        return user_route

    # --- 6) Fuzzy match on items_table keys ---
    keys = list(items_table.keys())
    matches = difflib.get_close_matches(user_route, keys, n=1, cutoff=cutoff)
    if matches:
        return matches[0]
    return None


# -----------------------------
# Parse user messages
# -----------------------------
def fuzzy_find(label, text, candidates, cutoff=0.6):
    """Return True if any candidate appears fuzzily in text."""
    text_low = text.lower()
    for cand in candidates:
        cand_low = cand.lower()
        if cand_low in text_low:
            return True
        if difflib.SequenceMatcher(None, cand_low, text_low).ratio() >= cutoff:
            return True
    return False


def parse_busqueda_message(content):
    # Clean bold formatting, emojis, and redundant symbols
    content = sanitize_text(content)
    content = re.sub(r'\*\*(.*?)\*\*', r'\1', content)  # remove bold
    content = re.sub(r'[•⭐🕸️🕷️🌟✨💫🔥🌀🌙🌑⚡☄️🧿]', '', content)

    lines = content.split("\n")
    cleaned = [line.strip() for line in lines if line.strip()]
    joined = "\n".join(cleaned).lower()

    # ---------------------------
    # 1) FUZZY MATCH USER CODE
    # ---------------------------
    codigo_labels = [
        "codigo de usuario",
        "codigo usuario",
        "codigo",
        "código",
        "user code",
        "codigo del usuario",
        "codigo:"
    ]

    codigo_line = None
    for line in cleaned:
        if fuzzy_find("codigo", line, codigo_labels):
            codigo_line = line
            break

    if not codigo_line:
        return None  # nothing close to "Código" found

    # extract the number inside [H010], [010], H010, etc.
    m = re.search(r'H?-?\s*(\d{1,4})', codigo_line, re.IGNORECASE)
    if not m:
        return None

    codigo = "H" + m.group(1).zfill(3)

    # ---------------------------
    # 2) FUZZY MATCH ROUTES
    # ---------------------------
    rutas_labels = [
        "rutas a visitar",
        "rutas visitar",
        "rutas",
        "ruta a visitar",
        "ruta"
    ]

    rutas_start_index = None
    for i, line in enumerate(cleaned):
        if fuzzy_find("rutas", line, rutas_labels):
            rutas_start_index = i
            break

    if rutas_start_index is None:
        return {"codigo": codigo, "rutas": []}  # no routes found

    rutas = []
    for line in cleaned[rutas_start_index+1:]:
        # stop if next section starts (just in case)
        if fuzzy_find("codigo", line, codigo_labels):
            break

        # remove common bullet symbols
        line = re.sub(r'^[-•*–]+', '', line).strip()

        if not line:
            continue

        # separated by commas too
        for part in line.split(","):
            p = part.strip()
            if p:
                rutas.append(p)

    return {"codigo": codigo, "rutas": rutas}


# -----------------------------
# Rolling logic
# -----------------------------
def roll_tier():
    r = random.randint(1, 20)
    if 1 <= r <= 14:
        return "E", r
    if 15 <= r <= 19:
        return "D", r
    return "C", r


# -----------------------------
# Scheduled tasks & per-guild scanner
# -----------------------------
scan_locks = {}

async def wait_for_rpforge_file(channel, timeout=120):
    def check(msg):
        return (
            msg.channel == channel and
            msg.attachments
        )

    try:
        msg = await bot.wait_for("message", timeout=timeout, check=check)
        return msg.attachments[0]
    except asyncio.TimeoutError:
        return None

def get_lock(guild_id):
    if guild_id not in scan_locks:
        scan_locks[guild_id] = asyncio.Lock()
    return scan_locks[guild_id]

async def scan_guild(guild_id, force=False):
    """
    Scan a single guild's busquedas thread and process unprocessed messages.
    If force=True, ignore the configured hour and run immediately.
    """
    cfg = get_guild_cfg(guild_id)
    scan_hour = cfg.get("scan_hour", 17)
    now = get_local_now()
    if not force:
        if scan_hour is None:
            # scans disabled for this guild
            return
        if now.hour != int(scan_hour):
            return

    busq_ch_id = cfg.get("busquedas_channel")
    if not busq_ch_id:
        print(f"[SCAN] guild {guild_id} has no busquedas channel set.")
        return

    lock = get_lock(guild_id)
    if lock.locked():
        print(f"[SCAN] guild {guild_id} already scanning; skipping")
        return

    async with lock:
        channel = bot.get_channel(busq_ch_id)
        if not channel:
            print(f"[SCAN] cannot find channel {busq_ch_id} for guild {guild_id}")
            return

        start, end = get_current_week_range()
        week_start_str = start.date().isoformat()
        log_path = get_weekly_log_path(start, guild_id)

        # Try to load from Supabase first, fallback to local file
        weekly_log = await load_weekly_log_from_db(guild_id, week_start_str)
        if not weekly_log and os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8") as f:
                weekly_log = json.load(f)
        elif not weekly_log:
            weekly_log = {}

        thread_name = f"BUSQUEDAS {start.strftime('%d %b')} - {end.strftime('%d %b')}"
        # Active threads are in channel.threads (property)
        threads = list(channel.threads)
        thread = discord.utils.get(threads, name=thread_name)

        if not thread:
            archived = await channel.archived_threads(limit=50).flatten()
            thread = discord.utils.get(archived, name=thread_name)
        if not thread:
            print(f"[SCAN] No thread for guild {guild_id}")
            return

        items_table = load_items_table()

        async for message in thread.history(limit=1000, oldest_first=True):
            if message.author.bot:
                continue
            parsed = parse_busqueda_message(message.content)
            if not parsed:
                continue
            user_id = str(message.author.id)
            codigo = parsed["codigo"]
            log_key = f"{user_id}:{codigo}"

            if log_key in weekly_log:
                # already processed for this week
                print(f"[SCAN] Skipping already-processed entry {log_key} in guild {guild_id}.")
                continue

            # Validate routes and prepare mapped routes
            mapped_routes = []
            invalid_routes = []
            for raw_route in parsed["rutas"]:
                matched = match_route(raw_route, items_table)
                if not matched:
                    print(f"[SCAN] Unrecognized route: {raw_route} (user {message.author})")
                    invalid_routes.append(raw_route)
                else:
                    mapped_routes.append(matched)

            if not mapped_routes:
                try:
                    await message.reply("❌ Ninguna ruta válida encontrada. Pide ayuda a un admin.", mention_author=True)
                except Exception as e:
                    print("Failed to reply (no valid routes):", e)

                weekly_log[log_key] = {
                    "user_id": user_id,
                    "author": message.author.name,
                    "codigo": codigo,
                    "rutas": parsed["rutas"],
                    "results": [],
                    "invalid": invalid_routes,
                    "delivered": False,
                    "timestamp": get_local_now().isoformat()
                }
                with open(log_path, "w", encoding="utf-8") as f:
                    json.dump(weekly_log, f, indent=4, ensure_ascii=False)

                # Still record the failure in processed_users
                if supabase:
                    try:
                        supabase.table("processed_users").upsert({
                            "guild_id": str(guild_id),
                            "week_start": week_start_str,
                            "user_id": user_id,
                            "codigo": codigo,
                            "payload": {"routes": parsed["rutas"], "failed": True}
                        }).execute()
                    except Exception as e:
                        print("[SUPABASE] processed_users error:", e)
                continue

            # Roll items for each route
            results = []
            failed = False
            for route in mapped_routes:
                tier, roll = roll_tier()
                tier_list = items_table.get(route, {}).get(tier, [])
                if not tier_list:
                    try:
                        await thread.send("Invalid Entry, pide ayuda a un admin")
                    except Exception:
                        pass
                    print(f"[SCAN] No items for route {route} tier {tier} (user {message.author})")
                    failed = True
                    break
                item = random.choice(tier_list)
                results.append({"route": route, "tier": tier, "roll": roll, "id": item.get("id"), "name": item.get("name"), "image": item.get("id") + ".png"})

            if failed:
                weekly_log[log_key] = {
                    "user_id": user_id,
                    "author": message.author.name,
                    "codigo": codigo,
                    "rutas": parsed["rutas"],
                    "results": [],
                    "delivered": False,
                    "error": "Unrecognized route(s)",
                    "timestamp": get_local_now().isoformat()
                }
                with open(log_path, "w", encoding="utf-8") as f:
                    json.dump(weekly_log, f, indent=4, ensure_ascii=False)

                if supabase:
                    try:
                        supabase.table("processed_users").upsert({
                            "guild_id": str(guild_id),
                            "week_start": week_start_str,
                            "user_id": user_id,
                            "codigo": codigo,
                            "payload": {"routes": parsed["rutas"], "failed": True}
                        }).execute()
                    except Exception as e:
                        print("[SUPABASE] processed_users error:", e)
                continue

            formatted_names = []
            for r in results:
                name = f"{r['name']}"
                if r['tier'] == "C":
                    formatted_names.append(f"✨**{name}**✨")
                else:
                    formatted_names.append(f"**{name}**")

            if len(formatted_names) == 1:
                descr = f"¡Felicidades! Has hecho un contrato con {formatted_names[0]}!"
            else:
                descr = f"¡Felicidades! Has hecho un contrato con {', '.join(formatted_names[:-1])} y {formatted_names[-1]}!"

            if invalid_routes:
                invalid_str = ", ".join([f"- {r}" for r in invalid_routes])
                descr += f"\n\nRuta inválida: **{invalid_str}**"

            files = []
            for r in results:
                tier_folder = f"RANK {r['tier']}"
                img_path = os.path.join(IMAGES_DIR, tier_folder, r["id"] + ".png")
                if os.path.exists(img_path):
                    try:
                        files.append(discord.File(img_path))
                    except Exception as e:
                        print("Failed to attach image", img_path, e)
                else:
                    print("Image not found:", img_path)

            try:
                if files:
                    await message.reply(descr, files=files, mention_author=True)
                else:
                    await message.reply(descr, mention_author=True)
            except Exception as e:
                print("Failed to send reply:", e)

            # Send rp!giveitem commands to operations channel
            ops_channel = None
            ops_channel_id = cfg.get("operations_channel")
            if ops_channel_id:
                ops_channel = bot.get_channel(ops_channel_id)

            if not ops_channel:
                print("[SCAN] No operations channel set for guild; skipping rp!giveitem commands.")
            else:
                for r in results:
                    if r.get("id"):
                        cmd = f"rp!giveitem {r['id']}x1 {codigo}"
                        try:
                            await ops_channel.send(cmd)
                            await asyncio.sleep(2)
                        except Exception as e:
                            print("Failed to send giveitem cmd:", e)

            weekly_log[log_key] = {
                "user_id": user_id,
                "author": message.author.name,
                "codigo": codigo,
                "rutas": parsed["rutas"],
                "results": results,
                "delivered": True,
                "timestamp": get_local_now().isoformat()
            }

            # periodically save (in case of long runs)
            with open(log_path, "w", encoding="utf-8") as f:
                json.dump(weekly_log, f, indent=4, ensure_ascii=False)

            # Record successful processing in Supabase
            if supabase:
                try:
                    supabase.table("processed_users").upsert({
                        "guild_id": str(guild_id),
                        "week_start": week_start_str,
                        "user_id": user_id,
                        "codigo": codigo,
                        "payload": {"routes": parsed["rutas"], "results": results}
                    }).execute()
                except Exception as e:
                    print("[SUPABASE] processed_users error:", e)

        # final save for this guild
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(weekly_log, f, indent=4, ensure_ascii=False)

        # Upsert aggregated weekly_log into weekly_logs table
        if supabase:
            try:
                supabase.table("weekly_logs").upsert({
                    "guild_id": str(guild_id),
                    "week_start": week_start_str,
                    "data": weekly_log,
                    "updated_at": utc_now_iso()
                }).execute()
                print(f"[SCAN] Upserted weekly log for guild {guild_id} ({len(weekly_log)} entries)")
            except Exception as e:
                print("[SUPABASE] weekly_logs error:", e)

        # ---------------------------------------------------------
        # Request updated RPForge characters.csv
        # ---------------------------------------------------------
        ops_channel_id = cfg.get("operations_channel")
        ops_channel = bot.get_channel(ops_channel_id) if ops_channel_id else None

        if ops_channel:
            try:
                print(f"[RPFORGE] Requesting characters export for guild {guild_id}")
                await ops_channel.send("rp!export characters")

                attachment = await wait_for_rpforge_file(ops_channel, timeout=500)
                if attachment:
                    save_path = os.path.join(EXPORTS_DIR, f"characters_{guild_id}.csv")
                    await attachment.save(save_path)
                    print(f"[RPFORGE] Saved file to {save_path}")
                    await upload_characters_csv_to_supabase(save_path)
                else:
                    print(f"[RPFORGE] No file received for guild {guild_id}")

            except Exception as e:
                print("[RPFORGE] Error during export:", e)
        else:
            print(f"[RPFORGE] No operations_channel for guild {guild_id}; skipping export.")

        print(f"[SCAN] Finished scanning guild {guild_id}")

# runs every hour and checks per-guild scan_hour
@tasks.loop(minutes=60)
async def scan_busquedas_thread():
    now = get_local_now()
    # iterate keys because we may mutate config while iterating
    for gid in list(config_cache.keys()):
        try:
            guild_id = int(gid)
        except:
            continue
        cfg = get_guild_cfg(guild_id)
        scan_hour = cfg.get("scan_hour", 17)
        if scan_hour is None:
            continue
        # only run if hour matches configured hour
        if now.hour == int(scan_hour):
            await scan_guild(guild_id, force=False)

# check weekly thread creation per-guild (runs every hour)
@tasks.loop(minutes=60)
async def check_weekly_thread():
    now = get_local_now()
    start, end = get_current_week_range()
    current_week_str = start.strftime("%Y%m%d")

    for gid in list(config_cache.keys()):
        try:
            guild_id = int(gid)
        except:
            continue

        cfg = get_guild_cfg(guild_id)
        busq_ch_id = cfg.get("busquedas_channel")
        last_week = cfg.get("last_week")

        if not busq_ch_id:
            continue

        # If last_week is not this week, create a new thread
        if last_week != current_week_str:
            channel = bot.get_channel(busq_ch_id)
            if not channel:
                print(f"[WEEK] cannot find channel {busq_ch_id} for guild {guild_id}")
                continue

            title = f"BUSQUEDAS {start.strftime('%d %b')} - {end.strftime('%d %b')}"
            thread = discord.utils.get(channel.threads, name=title)

            if not thread:
                try:
                    thread = await channel.create_thread(
                        name=title,
                        type=discord.ChannelType.public_thread
                    )

                    # ---- Generate dynamic route list (6 per line) ----
                    chunked_routes = [VALID_ROUTES[i:i + 6] for i in range(0, len(VALID_ROUTES), 6)]
                    route_lines = ["- " + ", ".join(chunk) for chunk in chunked_routes]
                    routes_text = "\n".join(route_lines)

                    message = (
                        "# Hilo para Búsquedas Semanales\n\n"
                        "Para hacer sus búsquedas deben dejar un mensaje en el formato:\n"
                        "```Código de Usuario: `H0XX`\n"
                        "Rutas a Visitar:\n"
                        "Ruta 1\nRuta 2\n```"
                        f"**Rutas disponibles esta semana:**\n{routes_text}\n\n"
                        "¡Buena Suerte con sus Búsquedas esta semana! 🍀"
                    )

                    await thread.send(message)
                    print(f"[WEEK] Created thread for guild {guild_id}: {title}")

                except Exception as e:
                    print("[WEEK] failed to create thread:", e)

            set_config(guild_id, "last_week", current_week_str)

    # Refresh items table once after threads are checked
    await refresh_items_table()

# -----------------------------
# Bot events
# -----------------------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}!")
    await load_config_from_db()
    await pull_updates_from_db()  # force config sync at startup

    config_sync_loop.start()
    check_weekly_thread.start()
    scan_busquedas_thread.start()

    await check_weekly_thread()

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply("❌ No tienes permisos para usar este comando.", mention_author=False)
    else:
        raise error


# -----------------------------
# Commands
# -----------------------------
@bot.command()
async def hour(ctx):
    utc = datetime.now(timezone.utc)
    local = get_local_now()
    await ctx.send(
        f"UTC: {utc.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Local: {local.strftime('%Y-%m-%d %H:%M:%S')}"
    )

@bot.command(aliases=["busquedas", "busqueda", "search", "setb", "busquedaschannel", "setsearch", "sb", "setbusquedas"])
@commands.has_permissions(administrator=True)
async def set_busquedas(ctx, *, arg=None):
    cfg = get_guild_cfg(ctx.guild.id)

    if arg is None:
        current_id = cfg.get("busquedas_channel")
        if current_id is None:
            await ctx.send("ℹ️ This server **has no busquedas channel configured**.")
        else:
            ch = ctx.guild.get_channel(current_id)
            if ch:
                await ctx.send(f"ℹ️ Current busquedas channel is {ch.mention}.")
            else:
                await ctx.send("⚠️ A busquedas channel is saved but no longer exists.")
        return

    if arg.lower() in ("none", "off", "remove", "clear"):
        set_config(ctx.guild.id, "busquedas_channel", None)
        await ctx.send("✔️ Busquedas channel **cleared**.")
        return

    channel = None
    if arg.strip("<#>").isdigit():
        channel = ctx.guild.get_channel(int(arg.strip("<#>")))
    else:
        channel = discord.utils.get(ctx.guild.channels, mention=arg)

    if not channel or not isinstance(channel, discord.TextChannel):
        await ctx.send("❌ Invalid channel. Mention a text channel or type `none`.")
        return

    set_config(ctx.guild.id, "busquedas_channel", channel.id)
    await ctx.send(f"✔️ Busquedas channel set to {channel.mention}.")

@bot.command(aliases=["setlogs", "setl", "sl", "logs", "lchannel", "log", "setoperations", "seto"])
@commands.has_permissions(administrator=True)
async def set_operations(ctx, *, arg=None):
    cfg = get_guild_cfg(ctx.guild.id)

    if arg is None:
        current_id = cfg.get("operations_channel")
        if current_id is None:
            await ctx.send("ℹ️ This server **has no operations channel configured**.")
        else:
            ch = ctx.guild.get_channel(current_id)
            if ch:
                await ctx.send(f"ℹ️ Current operations channel is {ch.mention}.")
            else:
                await ctx.send("⚠️ An operations channel is saved but no longer exists.")
        return

    if arg.lower() in ("none", "off", "remove", "clear"):
        set_config(ctx.guild.id, "operations_channel", None)
        await ctx.send("✔️ Operations channel **cleared**.")
        return

    channel = None
    if arg.strip("<#>").isdigit():
        channel = ctx.guild.get_channel(int(arg.strip("<#>")))
    else:
        channel = discord.utils.get(ctx.guild.channels, mention=arg)

    if not channel or not isinstance(channel, discord.TextChannel):
        await ctx.send("❌ Invalid channel.")
        return

    set_config(ctx.guild.id, "operations_channel", channel.id)
    await ctx.send(f"✔️ Operations channel set to {channel.mention}.")

@bot.command(aliases=["scanhour", "setsh", "setscan", "sethour", "sh"])
@commands.has_permissions(administrator=True)
async def set_scan_hour(ctx, hour: str = None):
    cfg = get_guild_cfg(ctx.guild.id)

    if hour is None:
        current = cfg.get("scan_hour", None)
        if current is None:
            await ctx.send("ℹ️ Daily scanning is **disabled**.")
        else:
            await ctx.send(f"ℹ️ Current scan hour: **{current}:00**.")
        return

    if hour.lower() in ("off", "none", "disable"):
        val = None
    else:
        try:
            val = int(hour)
            if not 0 <= val <= 23:
                raise ValueError
        except:
            await ctx.send("❌ Invalid hour (0–23) or 'off'.")
            return

    set_config(ctx.guild.id, "scan_hour", val)

    await ctx.send(
        "🛑 Daily scan turned off."
        if val is None else f"✔️ Daily scan set to **{val}:00**."
    )

@bot.command(aliases=["refresh", "qr", "quick_refresh"])
@commands.has_permissions(administrator=True)
async def refresh_items(ctx=None):
    ok = await refresh_items_table()
    if ctx:
        if ok:
            await ctx.send("✅ Items table refreshed.")
        else:
            await ctx.send("⚠️ Failed to refresh items table; check logs.")
    return ok

@bot.command(aliases=["qc", "create", "weekly", "thread", "new thread"])
@commands.has_permissions(administrator=True)
async def quick_create(ctx):
    cfg = get_guild_cfg(ctx.guild.id)

    if not cfg.get("busquedas_channel"):
        await ctx.send("Busquedas channel is not set. Use >set_busquedas first.")
        return

    start, end = get_current_week_range()
    channel = bot.get_channel(cfg["busquedas_channel"])

    if not channel:
        await ctx.send("Could not find the busquedas channel.")
        return

    title = f"BUSQUEDAS {start.strftime('%d %b')} - {end.strftime('%d %b')}"
    thread = discord.utils.get(channel.threads, name=title)

    if thread:
        await ctx.send(f"A thread for this week already exists: {thread.mention}")
        return

    # Create the thread
    thread = await channel.create_thread(
        name=title,
        type=discord.ChannelType.public_thread
    )

    # ---- Generate dynamic route list (6 per line) ----
    chunked_routes = [
        VALID_ROUTES[i:i + 6] for i in range(0, len(VALID_ROUTES), 6)
    ]

    route_lines = [
        "- " + ", ".join(chunk)
        for chunk in chunked_routes
    ]

    routes_text = "\n".join(route_lines)
    # ----------------------------------------------

    message = (
        "# Hilo para Búsquedas Semanales\n\n"
        "Para hacer sus búsquedas deben dejar un mensaje en el formato:\n"
        "Código de Usuario: `H0XX`\n"
        "Rutas a Visitar:\n"
        "Ruta 1\nRuta 2\n\n"
        "**Rutas disponibles esta semana:**\n"
        f"{routes_text}\n\n"
        "¡Buena Suerte con sus Búsquedas esta semana! 🍀"
    )

    await thread.send(message)

    set_config(ctx.guild.id, "last_week", start.strftime("%Y%m%d"))

    await ctx.send(f"✅ Thread created: {thread.mention}")

    await refresh_items_table()

@bot.command(aliases=["qs","scan"])
@commands.has_permissions(administrator=True)
async def quick_scan(ctx):
    """Trigger an immediate scan for this server only (ignores hour)."""
    cfg = get_guild_cfg(ctx.guild.id)
    if not cfg.get("busquedas_channel"):
        await ctx.send("This server has no busquedas channel set. Use >set_busquedas.")
        return
    await ctx.send("Running quick scan for this server now...")
    await scan_guild(ctx.guild.id, force=True)
    await ctx.send("Quick scan finished.")

@bot.command()
async def ping(ctx):
    await ctx.send("Pong!")

@bot.command(aliases=["help", "h", "commands"])
async def help_command(ctx):
    embed = discord.Embed(
        title="📘 OS Bot — Guía de Comandos",
        description="Estos son los comandos disponibles del bot, agrupados por función.\n"
                    "Úsalos con el prefijo `>`.",
        color=discord.Color.gold()
    )
    embed.add_field(
        name="Comandos de Configuración",
        value=(
            "**setbusquedas** `#canal` — Establece el canal donde se crean los hilos semanales de busquedas.\n"
            "**setlogs** `#canal` — Establece el canal donde se registran las operaciones rápidas.\n"
            "**sethour** `hour|off` — Establece la hora diaria de escaneo (0-23) o 'off'.\n"
            "**prefix** `nuevo_prefijo` — Cambia el prefijo del bot."
        ),
        inline=False
    )
    embed.add_field(
        name="Debugging / Sincronización",
        value=(
            "**ping** — Verifica si el bot está en línea.\n"
            "**refresh** — Forzar la actualización de ítems desde Google Sheets y Drive.\n"
            "**remove** `código` — Elimina entradas procesadas con ese código y sincroniza.\n"
            "**debug_logs** — Muestra cuántos registros hay en Supabase para esta semana.\n"
            "**push_logs** — Sube el archivo local weekly_logs a Supabase."
        ),
        inline=False
    )
    embed.add_field(
        name="Acciones Rápidas",
        value=(
            "**create** — Crear inmediatamente el hilo de busquedas de esta semana.\n"
            "**scan** — Escanear manualmente el hilo semanal de busquedas para este servidor ahora."
        ),
        inline=False
    )
    await ctx.send(embed=embed)

@bot.command()
@commands.has_permissions(administrator=True)
async def prefix(ctx, new_prefix: str = None):
    """Change the bot prefix for this guild."""
    cfg = get_guild_cfg(ctx.guild.id)
    
    if not new_prefix:
        current_prefix = cfg.get("prefix", ">")
        await ctx.send(f"Current prefix is: `{current_prefix}`")
        return

    set_config(ctx.guild.id, "prefix", new_prefix)
    await ctx.send(f"✔️ Bot prefix changed to: `{new_prefix}`")

@bot.command()
@commands.has_permissions(administrator=True)
async def remove(ctx, codigo: str):
    """Remove processed entries by Código and sync changes."""

    if not supabase:
        await ctx.send("❌ Supabase not configured.")
        return

    codigo = codigo.upper().strip()
    if not codigo.startswith("H"):
        codigo = "H" + codigo

    week = get_current_week_start_str()
    gid = str(ctx.guild.id)

    start, _ = get_current_week_range()
    log_path = get_weekly_log_path(start, ctx.guild.id)

    removed_count = 0

    # -----------------------------
    # 1) Remove from Supabase processed_users (only entries with this codigo)
    # -----------------------------
    try:
        supabase.table("processed_users") \
            .delete() \
            .eq("guild_id", gid) \
            .eq("week_start", week) \
            .eq("codigo", codigo) \
            .execute()
        await ctx.send(f"🧹 Removed entries with código `{codigo}` from processed_users.")
    except Exception as e:
        await ctx.send(f"❌ Supabase error: {e}")
        return

    # -----------------------------
    # 2) Remove from local weekly log
    # -----------------------------
    if not os.path.exists(log_path):
        await ctx.send("⚠️ No local weekly log found.")
        return

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        keys_to_delete = []

        for key, entry in data.items():
            if entry.get("codigo") == codigo:
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del data[key]
            removed_count += 1

        # Save updated log
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        await ctx.send(f"🗑 Removed **{removed_count}** entries from local weekly log.")

    except Exception as e:
        await ctx.send(f"❌ Local log error: {e}")
        return

    # -----------------------------
    # 3) Push updated weekly log to Supabase
    # -----------------------------
    try:
        supabase.table("weekly_logs").upsert({
            "guild_id": gid,
            "week_start": week,
            "data": data,
            "updated_at": utc_now_iso()
        }).execute()
        print("☁️ Weekly log synced to Supabase.")
    except Exception as e:
        print(f"❌ Failed to push weekly log: {e}")

@bot.command()
@commands.has_permissions(administrator=True)
async def debug_logs(ctx):
    """Show count of processed entries for this week."""
    if not supabase:
        await ctx.send("❌ Supabase not configured.")
        return

    week = get_current_week_start_str()
    gid = str(ctx.guild.id)

    try:
        res = supabase.table("processed_users") \
            .select("user_id, codigo") \
            .eq("guild_id", gid) \
            .eq("week_start", week) \
            .execute()

        data = res.data or []
        await ctx.send(f"📊 Found **{len(data)}** processed entries for week {week}.")
        if data:
            # Show first 5 as preview
            preview = "\n".join(f"- {row['user_id']} → {row['codigo']}" for row in data[:5])
            if len(data) > 5:
                preview += f"\n... and {len(data)-5} more."
            await ctx.send(f"```\n{preview}\n```")
    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command()
@commands.has_permissions(administrator=True)
async def push_logs(ctx):
    """Force-push the local weekly log file to Supabase."""
    if not supabase:
        await ctx.send("❌ Supabase not configured.")
        return

    start, _ = get_current_week_range()
    log_path = get_weekly_log_path(start, ctx.guild.id)

    if not os.path.exists(log_path):
        await ctx.send("❌ No local log file found for this week.")
        return

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        supabase.table("weekly_logs").upsert({
            "guild_id": str(ctx.guild.id),
            "week_start": start.date().isoformat(),
            "data": data,
            "updated_at": utc_now_iso()
        }).execute()

        await ctx.send("✅ Local logs pushed to Supabase.")
    except Exception as e:
        await ctx.send(f"❌ Error: {e}")

@bot.command(aliases=["iteminfo", "skill", "skillinfo", "i", "ente", "unlocks", "unlock", "ability", "abilityinfo", "card", "cards", "items"])
async def item(ctx):

    def safe_text(value):
        if value is None:
            return ""
        return str(value).encode("utf-8", "ignore").decode("utf-8").strip()

    def normalize_id(value):
        return safe_text(value).upper().replace(" ", "")

    def get_prefix(code: str):
        emoji = discord.utils.get(ctx.guild.emojis, name=code.lower())
        return str(emoji) if emoji else f"{code}:"

    PREFIX_MAP = {
        "AE": get_prefix("ae"),
        "SB": get_prefix("stat"),
        "HE": get_prefix("he"),
        "AC": get_prefix("armor"),
    }

    UNLOCK_MULTIPLIER = {
        "AE": 2,
        "SB": 3,
        "HE": 4,
        "AC": 5,
    }

    # ---------- SHEETS ----------
    UNLOCK_SHEET_URL = "https://docs.google.com/spreadsheets/d/1LWhg-GA_QuFOlic2-oD7lFX2whhq-i5QPljdwCB0fCk/export?format=csv&gid=2073923557"
    ENTE_SHEET_URL = SHEET_CSV_URL or DEFAULT_SHEET_CSV  # <- your rutas sheet

    def load_sheet(url):
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        text = resp.content.decode("utf-8-sig", errors="replace")

        reader = csv.DictReader(io.StringIO(text))
        data = {}

        for row in reader:
            clean = {safe_text(k).lower(): safe_text(v) for k, v in row.items()}
            raw_id = normalize_id(clean.get("id"))

            if raw_id:
                data[raw_id] = clean

        return data

    def find_item(data, query):
        q = normalize_id(query)

        if q in data:
            return q, data[q]

        matches = difflib.get_close_matches(q, data.keys(), n=1, cutoff=0.7)
        if matches:
            return matches[0], data[matches[0]]

        return None, None

    # ---------- IMAGE ----------
    def find_image(base_id):
        for tier in ["E", "D", "C"]:
            folder = os.path.join(IMAGES_DIR, f"RANK {tier}")
            if not os.path.exists(folder):
                continue

            for f in os.listdir(folder):
                if f.lower() == f"{base_id.lower()}.png":
                    return os.path.join(folder, f)

        return None

    def make_file(base_id):
        path = find_image(base_id)
        if not path:
            return None, None

        filename = os.path.basename(path)
        return discord.File(path, filename=filename), f"attachment://{filename}"

    # ---------- PARSE ----------
    parts = ctx.message.content.split(maxsplit=1)
    if len(parts) < 2:
        await ctx.send("❌ Missing item ID.")
        return

    raw_query = parts[1].strip()

    if ":" in raw_query:
        base_id, suffix = raw_query.split(":", 1)
        base_id = normalize_id(base_id)
        suffix = normalize_id(suffix)
        unlock_query = True
    else:
        base_id = normalize_id(raw_query)
        suffix = "AE"
        unlock_query = False

    try:
        unlocks = load_sheet(UNLOCK_SHEET_URL)
        entes = load_sheet(ENTE_SHEET_URL)
    except Exception as e:
        await ctx.send(f"❌ Error loading sheet: `{e}`")
        return

    # ---------- BUILDERS ----------
    def build_unlock(row, full_id, base_id, suffix):
        title = row.get("title") or row.get("name") or "Unknown"
        desc = row.get("description", "")
        typ = row.get("type", "Unknown")

        released = row.get("released", "true").lower() in ("true", "1", "yes")
        mult = UNLOCK_MULTIPLIER.get(suffix, 2)

        if not released:
            embed = discord.Embed(
                title="Item Pending",
                description=f"{full_id}\nAún no ha sido liberado.",
                color=discord.Color.orange()
            )
        else:
            prefix = PREFIX_MAP.get(suffix, f"{suffix}:")
            embed = discord.Embed(
                title=f"{prefix} {title}",
                description=desc,
                color=discord.Color.green()
            )

        embed.add_field(name="ID", value=full_id)
        embed.add_field(name="Type", value=typ)
        embed.add_field(name="Unlocked At", value=f"{base_id} x{mult}")
        return embed

    def build_ente(row, base_id):
        name = row.get("name") or "Unknown"
        element = row.get("elemento") or row.get("element") or "Unknown"

        embed = discord.Embed(
            title=base_id,            # ✅ ID as title
            description=name,         # ✅ Name as description
            color=discord.Color.blurple()
        )
        embed.add_field(name="Element", value=element)
        return embed

    # ---------- VIEW ----------
    class EnteView(discord.ui.View):
        def __init__(self, base_id):
            super().__init__(timeout=180)
            self.base_id = base_id

        async def show(self, interaction, suffix):
            target = f"{self.base_id}:{suffix}"
            _, r = find_item(unlocks, target)

            if not r:
                await interaction.response.send_message(f"❌ {target} not found", ephemeral=True)
                return

            embed = build_unlock(r, target, self.base_id, suffix)
            file, url = make_file(self.base_id)

            if url:
                embed.set_image(url=url)

            await interaction.response.defer()
            if file:
                await interaction.message.edit(embed=embed, attachments=[file], view=self)
            else:
                await interaction.message.edit(embed=embed, view=self)

        @discord.ui.button(label="AE")
        async def ae(self, i, b): await self.show(i, "AE")

        @discord.ui.button(label="SB")
        async def sb(self, i, b): await self.show(i, "SB")

        @discord.ui.button(label="HE")
        async def he(self, i, b): await self.show(i, "HE")

        @discord.ui.button(label="AC")
        async def ac(self, i, b): await self.show(i, "AC")

    # ---------- OUTPUT ----------
    if unlock_query:
        full_id = f"{base_id}:{suffix}"
        _, row = find_item(unlocks, full_id)

        if not row:
            await ctx.send("❌ Item not found.")
            return

        embed = build_unlock(row, full_id, base_id, suffix)
        await ctx.send(embed=embed)
        return

    # ✅ BASE ENTE (correct sheet)
    _, row = find_item(entes, base_id)

    if not row:
        await ctx.send("❌ Item not found.")
        return

    embed = build_ente(row, base_id)

    file, url = make_file(base_id)
    if url:
        embed.set_image(url=url)

    view = EnteView(base_id)

    if file:
        await ctx.send(embed=embed, file=file, view=view)
    else:
        await ctx.send(embed=embed, view=view)
        
# -----------------------------
# Run bot
# -----------------------------
if __name__ == "__main__":
    if not (SHEET_CSV_URL or os.getenv("SHEET_CSV_URL")):
        print("Note: No SHEET_CSV_URL provided in environment. Using default gid=0 URL:", DEFAULT_SHEET_CSV)
    bot.run(DISCORD_TOKEN)