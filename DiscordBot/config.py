import os
import json
import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
RPFORGE_BOT_ID = 1230402077747056641
supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# In-memory cache
config_cache: Dict[str, Dict[str, Any]] = {}
config_dirty: set = set()

CONFIG_SYNC_INTERVAL = 300
LOCAL_TZ = ZoneInfo("America/New_York")

def get_local_now():
    return datetime.now(LOCAL_TZ)

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_dt(value: str):
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)

def _parse_config_data(raw) -> Dict[str, Any]:
    """
    Safely parse Supabase's `data` column.
    It can be:
      - None / missing
      - a JSON object (dict)
      - a JSON string (text column)
    """
    if raw is None:
        return {}

    if isinstance(raw, dict):
        return dict(raw)

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    return {}

async def load_config_from_db():
    """Load all guild configs into memory, overwriting only newer records."""
    if not supabase:
        print("[CONFIG] Supabase client not configured. Skipping config load.")
        return

    try:
        res = supabase.table("config").select("guild_id,data,updated_at").execute()
        data = res.data or []
        for row in data:
            gid = str(row["guild_id"])
            remote_data = _parse_config_data(row.get("data"))
            remote_updated_at = row.get("updated_at") or utc_now_iso()
            current = config_cache.get(gid)

            if not current or parse_dt(remote_updated_at) > parse_dt(current["updated_at"]):
                config_cache[gid] = {
                    "data": remote_data,
                    "updated_at": remote_updated_at,
                }
        print(f"[CONFIG] Loaded {len(data)} guilds from Supabase")
    except Exception as e:
        print("[CONFIG] Failed to load from DB:", e)

def get_guild_cfg(guild_id) -> Dict[str, Any]:
    """
    Return a mutable dictionary for the guild's config, ensuring all
    required keys exist with safe defaults.
    """
    gid = str(guild_id)
    if gid not in config_cache:
        config_cache[gid] = {
            "data": {},
            "updated_at": utc_now_iso(),
        }

    data = config_cache[gid]["data"]

    defaults = {
        "busquedas_channel": None,
        "operations_channel": None,
        "daruma_channel": None,
        "rankup_channel_id": None,
        "last_week": None,
        "last_faction_week": None,
        "scan_hour": 17,
        "prefix": ">",
    }

    for key, value in defaults.items():
        data.setdefault(key, value)

    return data

def set_config(guild_id, key, value):
    """Set a config key in memory and mark the guild dirty for DB sync."""
    gid = str(guild_id)
    if gid not in config_cache:
        config_cache[gid] = {
            "data": {},
            "updated_at": utc_now_iso(),
        }
    config_cache[gid]["data"][key] = value
    config_cache[gid]["updated_at"] = utc_now_iso()
    config_dirty.add(gid)

async def push_dirty_to_db():
    """Push dirty configs back to Supabase (upsert by guild_id)."""
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
            "updated_at": entry["updated_at"],
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
    """Pull newer configs from Supabase, merging into memory."""
    if not supabase:
        return

    try:
        res = supabase.table("config").select("guild_id,data,updated_at").execute()
        data = res.data or []
        for row in data:
            gid = str(row["guild_id"])
            remote_data = _parse_config_data(row.get("data"))
            remote_updated_at = row.get("updated_at") or utc_now_iso()
            local = config_cache.get(gid)

            if not local or parse_dt(remote_updated_at) > parse_dt(local["updated_at"]):
                config_cache[gid] = {
                    "data": remote_data,
                    "updated_at": remote_updated_at,
                }
        print("[CONFIG] Pulled updates")
    except Exception as e:
        print("[CONFIG] Pull failed:", e)

async def config_sync_loop():
    """Periodically push dirty configs and pull newer remote configs."""
    while True:
        await asyncio.sleep(CONFIG_SYNC_INTERVAL)
        await push_dirty_to_db()
        await pull_updates_from_db()