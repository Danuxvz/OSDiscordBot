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
supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# In-memory cache
config_cache: Dict[str, Dict[str, Any]] = {}          # guild_id -> {"data": {...}, "updated_at": "..."}
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

async def load_config_from_db():
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

async def config_sync_loop():
    while True:
        await asyncio.sleep(CONFIG_SYNC_INTERVAL)
        await push_dirty_to_db()
        await pull_updates_from_db()