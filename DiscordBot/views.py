import asyncio
import time
import discord
import os
import difflib
import requests
import io
import csv
from .items import load_items_table
from .utils import local_md5

# Sheet URLs
UNLOCK_SHEET_URL = "https://docs.google.com/spreadsheets/d/1LWhg-GA_QuFOlic2-oD7lFX2whhq-i5QPljdwCB0fCk/export?format=csv&gid=2073923557"
ENTE_SHEET_URL = os.getenv("SHEET_CSV_URL") or f"https://docs.google.com/spreadsheets/d/{os.getenv('SHEET_ID', '1dMUMUXjn22L2nYHFHKmDBObD1VskyVruzh-OM9IexLk')}/export?format=csv&gid=0"
IMAGES_DIR = "ENTES"

PREFIX_MAP = {
    "AE": "ae",
    "SB": "stat",
    "HE": "he",
    "AC": "armor"
}

# ---------------------------------------------------------------------------
# Sheet caching (TTL = 5 minutes)
# ---------------------------------------------------------------------------
_unlocks_cache = None
_unlocks_cache_time = 0
_entes_cache = None
_entes_cache_time = 0
_CACHE_TTL = 300

async def get_cached_unlocks_async():
    global _unlocks_cache, _unlocks_cache_time
    now = time.time()
    if _unlocks_cache is None or (now - _unlocks_cache_time) > _CACHE_TTL:
        _unlocks_cache = await asyncio.to_thread(load_sheet, UNLOCK_SHEET_URL)
        _unlocks_cache_time = now
    return _unlocks_cache

async def get_cached_entes_async():
    global _entes_cache, _entes_cache_time
    now = time.time()
    if _entes_cache is None or (now - _entes_cache_time) > _CACHE_TTL:
        _entes_cache = await asyncio.to_thread(load_sheet, ENTE_SHEET_URL)
        _entes_cache_time = now
    return _entes_cache

# ---------------------------------------------------------------------------
# Image index (built once, invalidated manually after refresh)
# ---------------------------------------------------------------------------
_image_index = None

def build_image_index():
    global _image_index
    _image_index = {}
    for tier in ["E", "D", "C"]:
        folder = os.path.join(IMAGES_DIR, f"RANK {tier}")
        if not os.path.exists(folder):
            continue
        for f in os.listdir(folder):
            if f.lower().endswith('.png'):
                item_id = f[:-4].upper()
                _image_index[item_id] = os.path.join(folder, f)

def find_image_cached(base_id):
    global _image_index
    if _image_index is None:
        build_image_index()
    return _image_index.get(base_id.upper())

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def safe_text(value):
    if value is None:
        return ""
    return str(value).encode("utf-8", "ignore").decode("utf-8").strip()

def normalize_id(value):
    return safe_text(value).upper().replace(" ", "")

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

# ---------------------------------------------------------------------------
# EnteView with caching
# ---------------------------------------------------------------------------
class EnteView(discord.ui.View):
    def __init__(self, base_id):
        super().__init__(timeout=180)
        self.base_id = base_id

    async def show(self, interaction, suffix):
        target = f"{self.base_id}:{suffix}"
        unlocks = await get_cached_unlocks_async()
        _, row = find_item(unlocks, target)
        if not row:
            await interaction.response.send_message(f"❌ {target} not found", ephemeral=True)
            return

        title = row.get("title") or row.get("name") or "Unknown"
        desc = row.get("description", "")
        typ = row.get("type", "Unknown")
        released = row.get("released", "true").lower() in ("true", "1", "yes")
        mult = {"AE":2, "SB":3, "HE":4, "AC":5}.get(suffix, 2)

        if not released:
            embed = discord.Embed(
                title="Item Pending",
                description=f"{target}\nAún no ha sido liberado.",
                color=discord.Color.orange()
            )
        else:
            emoji_name = PREFIX_MAP.get(suffix, suffix.lower())
            prefix = discord.utils.get(interaction.guild.emojis, name=emoji_name)
            prefix_str = str(prefix) if prefix else f"{suffix}:"
            embed = discord.Embed(
                title=f"{prefix_str} {title}",
                description=desc,
                color=discord.Color.green()
            )

        embed.add_field(name="ID", value=target)
        embed.add_field(name="Type", value=typ)
        embed.add_field(name="Unlocked At", value=f"{self.base_id} x{mult}")

        img_path = find_image_cached(self.base_id)
        file = None
        if img_path:
            file = discord.File(img_path, filename=os.path.basename(img_path))
            embed.set_image(url=f"attachment://{os.path.basename(img_path)}")

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