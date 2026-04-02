import os
import json
import hashlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/New_York")

def get_local_now():
    return datetime.now(LOCAL_TZ)

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def get_current_week_range():
    now = get_local_now()
    start = now - timedelta(days=now.weekday())
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=6)
    return start, end

def get_current_week_start_str():
    start, _ = get_current_week_range()
    return start.date().isoformat()

def get_weekly_log_path(start, guild_id):
    os.makedirs("weekly_logs", exist_ok=True)
    filename = f"{guild_id}_{start.strftime('%Y%m%d')}.json"
    return os.path.join("weekly_logs", filename)

def local_md5(path):
    if not os.path.exists(path):
        return None
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
    return md5.hexdigest()