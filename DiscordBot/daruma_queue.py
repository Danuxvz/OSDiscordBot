import asyncio
import re
import io
import discord
from PIL import Image, ImageDraw
from .config import supabase, get_guild_cfg, utc_now_iso 

PENDING = "pending"
PROCESSING = "processing"
COMPLETED = "completed"
FAILED = "failed"
RPFORGE_BOT_ID = 1230402077747056641

# Daruma color mapping (ente code → color name)
DARUMA_COLORS = {
    "E123A": "Black",
    "E123B": "Red",
    "E123C": "Green",
    "E123D": "Yellow",
    "E123E": "Purple",
    "E123F": "White",
    "E123G": "Pink",
    "E123H": "Copper",
    "E123I": "Silver",
    "E123J": "Gold"
}

async def resolve_ops_channel(bot, guild_id=None):
    if guild_id:
        guild = bot.get_guild(int(guild_id))
        if guild:
            cfg = get_guild_cfg(guild.id)
            channel_id = cfg.get("operations_channel")
            if channel_id:
                ch = guild.get_channel(channel_id)
                if ch:
                    return guild, ch

    for guild in bot.guilds:
        cfg = get_guild_cfg(guild.id)
        channel_id = cfg.get("operations_channel")
        if channel_id:
            ch = guild.get_channel(channel_id)
            if ch:
                return guild, ch
    return None, None

def extract_text(msg):
    parts = [msg.content or ""]
    for embed in msg.embeds:
        if embed.title: parts.append(embed.title)
        if embed.description: parts.append(embed.description)
        if embed.footer and embed.footer.text: parts.append(embed.footer.text)
        if embed.author and embed.author.name: parts.append(embed.author.name)
        for field in embed.fields:
            parts.append(field.name)
            parts.append(field.value)
    return "\n".join(parts)

async def run_rpforge_command(bot, channel, cmd, success_phrase, timeout=120):
    def is_rpforge(msg):
        return (msg.channel == channel and msg.author.bot
                and msg.author.id == RPFORGE_BOT_ID)

    await channel.send(cmd)
    start = asyncio.get_event_loop().time()
    seen = []
    transaction_id = None

    while True:
        remaining = timeout - (asyncio.get_event_loop().time() - start)
        if remaining <= 0:
            return {"success": False, "error_message": "Timeout waiting for RPForge response",
                    "transaction_id": transaction_id, "raw_response": "\n\n".join(seen)}

        try:
            msg = await bot.wait_for("message", timeout=remaining, check=is_rpforge)
        except asyncio.TimeoutError:
            return {"success": False, "error_message": "Timeout waiting for RPForge response",
                    "transaction_id": transaction_id, "raw_response": "\n\n".join(seen)}

        text = extract_text(msg)
        seen.append(text)
        if transaction_id is None:
            m = re.search(r"Transaction Record\s+(\S+)", text)
            if m:
                transaction_id = m.group(1)
        if success_phrase in text:
            return {"success": True, "error_message": None,
                    "transaction_id": transaction_id, "raw_response": "\n\n".join(seen)}

async def count_inventory(bot, channel, ente, character_code, timeout=30):
    """Return the quantity of ente in character's inventory, or 0 if not found."""
    cmd = f"rp!search countinventory {ente} {character_code}"
    await channel.send(cmd)

    def is_rpforge(msg):
        return (msg.channel == channel and msg.author.bot
                and msg.author.id == RPFORGE_BOT_ID)

    start = asyncio.get_event_loop().time()
    while True:
        remaining = timeout - (asyncio.get_event_loop().time() - start)
        if remaining <= 0:
            return 0

        try:
            msg = await bot.wait_for("message", timeout=remaining, check=is_rpforge)
        except asyncio.TimeoutError:
            return 0

        text = extract_text(msg)
        if "There is no item with that name" in text:
            return 0
        m = re.search(r"Target has\s+(\d+)\s*x\s*(\S+)", text)
        if m:
            return int(m.group(1))
        if "Target has 0x" in text:
            return 0

def create_daruma_swap_image(source_ente: str, target_ente: str) -> discord.File | None:
    """
    Create a side‑by‑side image of the two daruma entes with a white pixel art arrow.
    Cards keep their original aspect ratio (260x360) and are scaled to a height of 200px.
    Returns a discord.File ready to be sent, or None if images are missing.
    """
    from .views import find_image

    source_path = find_image(source_ente)
    target_path = find_image(target_ente)

    if not source_path or not target_path:
        return None

    try:
        img_left = Image.open(source_path).convert("RGBA")
        img_right = Image.open(target_path).convert("RGBA")
    except Exception:
        return None

    # Scale to 200px height, preserving aspect ratio (260x360 -> ~144x200)
    target_height = 200
    for img in (img_left, img_right):
        w, h = img.size
        ratio = target_height / h
        new_w = int(w * ratio)
        img = img.resize((new_w, target_height), Image.LANCZOS)

    # Re‑assign after resize (PIL resize returns a new image)
    img_left = img_left.resize((int(img_left.width * target_height / img_left.height), target_height), Image.LANCZOS)
    img_right = img_right.resize((int(img_right.width * target_height / img_right.height), target_height), Image.LANCZOS)

    arrow_width = 30
    total_width = img_left.width + arrow_width + img_right.width
    canvas = Image.new("RGBA", (total_width, target_height), (255, 255, 255, 0))
    canvas.paste(img_left, (0, 0))
    canvas.paste(img_right, (img_left.width + arrow_width, 0))

    # White pixel‑art arrow (small, centered in the arrow zone)
    draw = ImageDraw.Draw(canvas)
    cx = img_left.width + arrow_width // 2   # centre x of the arrow area
    cy = target_height // 2

    # Arrow shaft (vertical rectangle)
    shaft_w = 4
    shaft_h = 20
    draw.rectangle([cx - shaft_w//2, cy - shaft_h//2,
                    cx + shaft_w//2, cy + shaft_h//2], fill="white", outline="black")

    # Arrowhead (triangle pointing right)
    arrow_head = [
        (cx + shaft_w//2, cy - 8),   # top tip
        (cx + shaft_w//2 + 10, cy),   # right point
        (cx + shaft_w//2, cy + 8)     # bottom tip
    ]
    draw.polygon(arrow_head, fill="white", outline="black")

    buf = io.BytesIO()
    canvas.save(buf, format="PNG")
    buf.seek(0)
    return discord.File(buf, filename="daruma_swap.png")

async def process_daruma_transaction(bot, tx):
    guild_id = tx.get("guild_id")
    guild, ops_channel = await resolve_ops_channel(bot, guild_id)
    if not ops_channel:
        supabase.table("daruma_transactions").update({
            "status": FAILED,
            "error_message": "No operations channel configured",
            "updated_at": utc_now_iso(),
        }).eq("id", tx["id"]).execute()
        return

    source_ente = tx["source_ente"]
    target_ente = tx["target_ente"]
    requested_source = int(tx.get("source_amount") or 0)
    requested_target = int(tx.get("target_amount") or 0)
    character_code = tx["character_code"]

    async def fail(msg):
        supabase.table("daruma_transactions").update({
            "status": FAILED,
            "error_message": msg[:1000],
            "updated_at": utc_now_iso(),
        }).eq("id", tx["id"]).execute()

    # Mark as processing
    supabase.table("daruma_transactions").update({
        "status": PROCESSING,
        "updated_at": utc_now_iso(),
    }).eq("id", tx["id"]).eq("status", PENDING).execute()

    # ---- Inventory check and adjustment ----
    source_inv = 0
    target_inv = 0
    if requested_source > 0:
        source_inv = await count_inventory(bot, ops_channel, source_ente, character_code)
    if requested_target > 0:
        target_inv = await count_inventory(bot, ops_channel, target_ente, character_code)

    actual_source = min(requested_source, source_inv) if requested_source > 0 else 0
    actual_target = min(requested_target, target_inv) if requested_target > 0 else 0

    if actual_source == 0 and actual_target == 0:
        await fail("Character has none of the required entes for this swap.")
        return

    # ---- Perform the swap ----
    if actual_source > 0:
        res = await run_rpforge_command(bot, ops_channel,
            f"rp!takeitem {source_ente}x{actual_source} {character_code}",
            "Successfully took items")
        if not res["success"]:
            await fail(res["error_message"] or "Failed taking source ente")
            return

    if actual_target > 0:
        res = await run_rpforge_command(bot, ops_channel,
            f"rp!takeitem {target_ente}x{actual_target} {character_code}",
            "Successfully took items")
        if not res["success"]:
            await fail(res["error_message"] or "Failed taking target ente")
            return

    if actual_target > 0:
        res = await run_rpforge_command(bot, ops_channel,
            f"rp!giveitem {source_ente}x{actual_target} {character_code}",
            "Successfully gave items")
        if not res["success"]:
            await fail(res["error_message"] or "Failed giving source ente back")
            return

    if actual_source > 0:
        res = await run_rpforge_command(bot, ops_channel,
            f"rp!giveitem {target_ente}x{actual_source} {character_code}",
            "Successfully gave items")
        if not res["success"]:
            await fail(res["error_message"] or "Failed giving target ente back")
            return

    supabase.table("daruma_transactions").update({
        "status": COMPLETED,
        "processed_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
    }).eq("id", tx["id"]).execute()

    # ---- Announce the swap in the daruma channel ----
    if guild:
        cfg = get_guild_cfg(guild.id)
        daruma_channel_id = cfg.get("daruma_channel")
        if daruma_channel_id:
            channel = guild.get_channel(daruma_channel_id)
            if channel:
                source_color = DARUMA_COLORS.get(source_ente, source_ente)
                target_color = DARUMA_COLORS.get(target_ente, target_ente)
                text = f"**{character_code}** cambió el color de su Daruma de **{source_color}** a **{target_color}**!"
                file = create_daruma_swap_image(source_ente, target_ente)
                embed = discord.Embed(description=text, color=0x00ff00)
                if file:
                    embed.set_image(url=f"attachment://{file.filename}")
                    await channel.send(embed=embed, file=file)
                else:
                    await channel.send(embed=embed)


async def process_daruma_queue(bot, interval=30):
    while True:
        try:
            if not supabase:
                await asyncio.sleep(interval)
                continue

            res = supabase.table("daruma_transactions") \
                .select("*") \
                .eq("status", PENDING) \
                .order("created_at") \
                .limit(10) \
                .execute()

            rows = res.data or []
            for tx in rows:
                await process_daruma_transaction(bot, tx)

        except Exception as e:
            print("[DARUMA] queue worker error:", e)

        await asyncio.sleep(interval)