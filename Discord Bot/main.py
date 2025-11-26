# main.py
import os
import io
import csv
import json
import random
import requests
import asyncio
from datetime import datetime, timedelta
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
import re
import difflib
import hashlib
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from supabase import create_client, Client

# -----------------------------
# Configuration
# -----------------------------
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# You can override the sheet CSV URL in .env by adding SHEET_CSV_URL.
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "")
SHEET_ID = os.getenv("SHEET_ID", "1dMUMUXjn22L2nYHFHKmDBObD1VskyVruzh-OM9IexLk")
DEFAULT_SHEET_CSV = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=0"
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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
bot = commands.Bot(command_prefix="?", intents=intents, case_insensitive=True, help_command=None)


# -----------------------------
# Config helpers
# -----------------------------
def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    # default: mapping guild_id -> {busquedas_channel, operations_channel, last_week, scan_hour}
    return {}

def save_config(cfg):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)

config = load_config()

def get_guild_cfg(guild_id):
    gid = str(guild_id)
    if gid not in config:
        # default scan_hour kept at 17 to match previous behaviour
        config[gid] = {"busquedas_channel": None, "operations_channel": None, "last_week": None, "scan_hour": 17}
    return config[gid]

def get_weekly_log_path(start_date, guild_id):
    return os.path.join(LOG_DIR, f"busquedas_g{guild_id}_{start_date.strftime('%Y%m%d')}.json")

async def wait_for_rpforge_file(channel, timeout=300):
    """
    Wait for RPForge to send a file after an rp!export command.
    Returns the discord.Attachment or None.
    """
    def check(msg):
        if not msg.attachments:
            return False
        # RPForge exports always come from a bot
        if not msg.author.bot:
            return False
        # must be in same channel
        if msg.channel.id != channel.id:
            return False
        # must contain a CSV file
        for a in msg.attachments:
            if a.filename.lower().endswith(".csv"):
                return True
        return False

    try:
        msg = await bot.wait_for("message", check=check, timeout=timeout)
        for att in msg.attachments:
            if att.filename.lower().endswith(".csv"):
                return att
        return None
    except asyncio.TimeoutError:
        print("[RPFORGE] Timeout waiting for characters.csv")
        return None

# -----------------------------
# Helper functions
# -----------------------------
def get_current_week_range():
    """Return start and end dates of current week (Mon-Sun) using local time."""
    now = datetime.now()
    start = now - timedelta(days=now.weekday())  # Monday
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=6)
    return start, end

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
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        if not resp.text.strip():
            print("⚠️ CSV response is empty!")
            return False
        csv_bytes = resp.content
        csv_text = csv_bytes.decode("utf-8-sig")
        table = parse_csv_to_items_table(csv_text)
        save_items_table(table)
        print(f"✅ Items table refreshed: {len(table)} routes loaded.")
        refresh_images_from_drive()
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
    "Academia St Peter", "Gehenna Door", "Abandoned Colosseum", "Elysian Garden", "Central Church", "Hopeless River"
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
    now = datetime.now()
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
        log_path = get_weekly_log_path(start, guild_id)
        weekly_log = {}
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8") as f:
                weekly_log = json.load(f)

        thread_name = f"BUSQUEDAS {start.strftime('%d %b')} - {end.strftime('%d %b')}"
        thread = discord.utils.get(channel.threads, name=thread_name)
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
            if user_id in weekly_log:
                # already processed for this week
                print(f"[SCAN] Skipping already-processed user {message.author} ({user_id}) in guild {guild_id}.")
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

                weekly_log[user_id] = {
                    "author": message.author.name,
                    "codigo": parsed["codigo"],
                    "rutas": parsed["rutas"],
                    "results": [],
                    "invalid": invalid_routes,
                    "delivered": False,
                    "timestamp": datetime.now().isoformat()
                }
                with open(log_path, "w", encoding="utf-8") as f:
                    json.dump(weekly_log, f, indent=4, ensure_ascii=False)
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
                weekly_log[user_id] = {
                    "author": message.author.name,
                    "codigo": parsed["codigo"],
                    "rutas": parsed["rutas"],
                    "results": [],
                    "delivered": False,
                    "error": "Unrecognized route(s)",
                    "timestamp": datetime.now().isoformat()
                }
                with open(log_path, "w", encoding="utf-8") as f:
                    json.dump(weekly_log, f, indent=4, ensure_ascii=False)
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
                        cmd = f"rp!giveitem {r['id']}x1 {parsed['codigo']}"
                        try:
                            await ops_channel.send(cmd)
                            await asyncio.sleep(2)
                        except Exception as e:
                            print("Failed to send giveitem cmd:", e)

            weekly_log[user_id] = {
                "author": message.author.name,
                "codigo": parsed["codigo"],
                "rutas": parsed["rutas"],
                "results": results,
                "delivered": True,
                "timestamp": datetime.now().isoformat()
            }

            # periodically save (in case of long runs)
            with open(log_path, "w", encoding="utf-8") as f:
                json.dump(weekly_log, f, indent=4, ensure_ascii=False)

        # final save for this guild
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(weekly_log, f, indent=4, ensure_ascii=False)
        
        # ---------------------------------------------------------
        # Request updated RPForge characters.csv
        # ---------------------------------------------------------
        ops_channel_id = cfg.get("operations_channel")
        ops_channel = bot.get_channel(ops_channel_id) if ops_channel_id else None

        if ops_channel:
            try:
                print(f"[RPFORGE] Requesting characters export for guild {guild_id}")
                await ops_channel.send("rp!export characters")

                attachment = await wait_for_rpforge_file(ops_channel, timeout=30)
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
    now = datetime.now()
    # iterate keys because we may mutate config while iterating
    for gid in list(config.keys()):
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
    now = datetime.now()
    start, end = get_current_week_range()
    # if it's Monday 00:00 create threads where missing (per guild)
    if now.weekday() == 0 and now.hour == 0:
        for gid in list(config.keys()):
            try:
                guild_id = int(gid)
            except:
                continue
            cfg = get_guild_cfg(guild_id)
            busq_ch_id = cfg.get("busquedas_channel")
            last_week = cfg.get("last_week")
            if not busq_ch_id:
                continue
            if last_week == start.strftime("%Y%m%d"):
                continue
            channel = bot.get_channel(busq_ch_id)
            if not channel:
                print(f"[WEEK] cannot find channel {busq_ch_id} for guild {guild_id}")
                continue
            title = f"BUSQUEDAS {start.strftime('%d %b')} - {end.strftime('%d %b')}"
            thread = discord.utils.get(channel.threads, name=title)
            if not thread:
                try:
                    thread = await channel.create_thread(name=title, type=discord.ChannelType.public_thread)
                    await thread.send("Canal de Busquedas semanal creado! Envia tu mensaje en el horario especificado." \
                    "" \
                    "Rutas disponibles: ")
                    print(f"[WEEK] Created thread for guild {guild_id}: {title}")
                except Exception as e:
                    print("[WEEK] failed to create thread:", e)
            cfg["last_week"] = start.strftime("%Y%m%d")
            save_config(config)
        # refresh global items table at week boundary
        await refresh_items_table()

# -----------------------------
# Bot events
# -----------------------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}!")
    check_weekly_thread.start()
    scan_busquedas_thread.start()

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
    now = datetime.now()
    await ctx.send(f"Current server time: {now.strftime('%Y-%m-%d %H:%M:%S')} (Hour = {now.hour})")

@bot.command(aliases=["busquedas", "busqueda", "search", "setb", "busquedaschannel", "setsearch", "sb", "setbusquedas"])
@commands.has_permissions(administrator=True)
async def set_busquedas(ctx, *, arg=None):
    """
    Show or set the busquedas channel for this server.
    Use 'none' to clear the saved channel.
    """
    cfg = get_guild_cfg(ctx.guild.id)

    # Just ">setbusquedas"
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

    # User typed: none/off/remove/clear
    if arg.lower() in ("none", "off", "remove", "clear"):
        cfg["busquedas_channel"] = None
        save_config(config)
        await ctx.send("✔️ Busquedas channel **cleared**.")
        return

    # Try to parse a channel mention
    channel = discord.utils.get(ctx.guild.channels, mention=arg) or ctx.guild.get_channel(int(arg.strip("<#>"))) if arg.strip("<#>").isdigit() else None

    if not channel or not isinstance(channel, discord.TextChannel):
        await ctx.send("❌ Invalid channel. Mention a text channel or type `none`.")
        return

    cfg["busquedas_channel"] = channel.id
    save_config(config)
    await ctx.send(f"✔️ Busquedas channel set to {channel.mention}.")

@bot.command(aliases=["setlogs", "setl", "sl", "logs", "lchannel", "log channel", "log", "setoperations", "seto"])
@commands.has_permissions(administrator=True)
async def set_operations(ctx, *, arg=None):
    """
    Show or set the operations channel for this server.
    Use 'none' to clear the saved channel.
    """
    cfg = get_guild_cfg(ctx.guild.id)

    # Just ">setlogs"
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

    # User typed: none/off/remove/clear
    if arg.lower() in ("none", "off", "remove", "clear"):
        cfg["operations_channel"] = None
        save_config(config)
        await ctx.send("✔️ Operations channel **cleared**.")
        return

    # Channel parsing
    channel = discord.utils.get(ctx.guild.channels, mention=arg) or ctx.guild.get_channel(int(arg.strip("<#>"))) if arg.strip("<#>").isdigit() else None

    if not channel or not isinstance(channel, discord.TextChannel):
        await ctx.send("❌ Invalid channel. Mention a text channel or type `none`.")
        return

    cfg["operations_channel"] = channel.id
    save_config(config)
    await ctx.send(f"✔️ Operations channel set to {channel.mention}.")

@bot.command(aliases=["scanhour", "setsh", "setscan", "sethour", "sh"])
@commands.has_permissions(administrator=True)
async def set_scan_hour(ctx, hour: str = None):
    """
    Set the daily scan hour for this server (0-23) or 'off' to disable daily scans.
    If no hour is given, shows the current configured scan hour.
    """

    cfg = get_guild_cfg(ctx.guild.id)

    # If the user typed just ">scanhour"
    if hour is None:
        current = cfg.get("scan_hour", None)
        if current is None:
            await ctx.send("ℹ️ This server currently has **daily scanning disabled**.")
        else:
            await ctx.send(f"ℹ️ Current scan hour is set to **{current}:00**.")
        return

    # If user provided input -> handle setting hour
    if hour.lower() in ("off", "none", "disable"):
        val = None
    else:
        try:
            val = int(hour)
            if not 0 <= val <= 23:
                raise ValueError("Hour out of range.")
        except Exception:
            await ctx.send("❌ Invalid hour. Enter a number between 0 and 23, or `off` to disable.")
            return

    cfg["scan_hour"] = val
    save_config(config)

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
    thread = await channel.create_thread(name=title, type=discord.ChannelType.public_thread)
    await thread.send("Weekly busquedas thread created! Post your messages in the specified format.")
    cfg["last_week"] = start.strftime("%Y%m%d")
    save_config(config)
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
            "**sethour** `hour|off` — Establece la hora diaria de escaneo (0-23) o 'off'."
        ),
        inline=False
    )
    embed.add_field(
        name="Debugging / Sincronización",
        value=(
            "**ping** — Verifica si el bot está en línea.\n"
            "**refresh** — Forzar la actualización de ítems desde Google Sheets y Drive."
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

# -----------------------------
# Run bot
# -----------------------------
if __name__ == "__main__":
    if not (SHEET_CSV_URL or os.getenv("SHEET_CSV_URL")):
        print("Note: No SHEET_CSV_URL provided in environment. Using default gid=0 URL:", DEFAULT_SHEET_CSV)
    bot.run(DISCORD_TOKEN)
