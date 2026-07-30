import asyncio
import re
import difflib
import discord
from discord.ext import commands
from .config import supabase as _supabase, get_guild_cfg, set_config, utc_now_iso, RPFORGE_BOT_ID, SUPABASE_URL, SUPABASE_KEY
from .views import FactionView, get_cached_faction_async
from supabase import create_client

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
THRESHOLDS = [5, 10, 20, 30, 45]
PERKS      = ["A", "B", "C", "D", "E"]
RANK_NAMES = ["Notario", "Estenógrafo", "Jurado", "Fiscal", "Magistrado"]
MAX_BOONS  = 5
FACTION_ITEM_MAP = {
    "hexen":    "Hexen",
    "yuugen":   "Yuugen",
    "carnival": "Carnival",
}

# ---------------------------------------------------------------------------
# RP‑forge helper
# ---------------------------------------------------------------------------
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
            return {"success": False, "error_message": "Timeout",
                    "transaction_id": transaction_id, "raw_response": "\n\n".join(seen)}

        try:
            msg = await bot.wait_for("message", timeout=remaining, check=is_rpforge)
        except asyncio.TimeoutError:
            return {"success": False, "error_message": "Timeout",
                    "transaction_id": transaction_id, "raw_response": "\n\n".join(seen)}

        text = "".join([msg.content or ""] +
                       [embed.title or "" for embed in msg.embeds] +
                       [embed.description or "" for embed in msg.embeds])
        seen.append(text)
        if transaction_id is None:
            m = re.search(r"Transaction Record\s+(\S+)", text)
            if m:
                transaction_id = m.group(1)
        if success_phrase in text:
            return {"success": True, "error_message": None,
                    "transaction_id": transaction_id, "raw_response": "\n\n".join(seen)}

# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------
def is_admin_or_bot_admin(ctx):
    if ctx.author.guild_permissions.administrator:
        return True
    return any(r.name.lower() == 'bot admin' for r in ctx.author.roles)

# ---------------------------------------------------------------------------
# Helper: always get a usable supabase client
# ---------------------------------------------------------------------------
def get_supabase():
    if _supabase is not None:
        return _supabase
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            return create_client(SUPABASE_URL, SUPABASE_KEY)
        except Exception as e:
            print(f"[FactionProgression] Failed to create Supabase client: {e}")
            raise
    raise RuntimeError("Supabase client is not available – check your .env file.")

# ---------------------------------------------------------------------------
# Table existence check
# ---------------------------------------------------------------------------
async def ensure_tables():
    client = get_supabase()
    required = ["faction_progress", "character_boons", "pending_rankups"]
    for table in required:
        try:
            res = client.table(table).select("id").limit(1).execute()
        except Exception as e:
            print(f"[FactionProgression] Table `{table}` check failed: {e}")

# ---------------------------------------------------------------------------
# Character resolver
# ---------------------------------------------------------------------------
async def resolve_character(code):
    code = code.upper().strip()
    try:
        client = get_supabase()
        all_rows = client.table("characters_export") \
            .select("name,owner_id") \
            .execute()
        if not all_rows or not all_rows.data:
            return None

        for row in all_rows.data:
            if row["name"].upper().strip() == code:
                return row["name"].upper(), row["owner_id"], row["name"].upper()

        names = [row["name"].upper().strip() for row in all_rows.data]
        matches = difflib.get_close_matches(code, names, n=1, cutoff=0.9)
        if matches:
            match_name = matches[0]
            row = client.table("characters_export") \
                .select("name,owner_id") \
                .eq("name", match_name.lower()) \
                .maybe_single() \
                .execute()
            if row and row.data:
                return row.data["name"].upper(), row.data["owner_id"], row.data["name"].upper()
    except Exception as e:
        print(f"[FactionProgression] resolve_character error: {e}")
    return None

# ---------------------------------------------------------------------------
# Faction info embed + buttons
# ---------------------------------------------------------------------------
async def build_faction_info_embed_and_view(ctx, faction_name):
    try:
        factions_cog = ctx.bot.get_cog("Factions")
        if factions_cog is None:
            return discord.Embed(title="Error", description="Factions module not loaded.", color=discord.Color.red()), None

        info = await factions_cog._get_faction_info(ctx.guild.id, faction_name)
        if not info:
            return discord.Embed(title="Not Found", description=f"No faction named **{faction_name}** exists in this server.", color=discord.Color.red()), None

        color_hex = info.get('color', '#FFFFFF').lstrip('#')
        try:
            color = int(color_hex, 16)
        except Exception:
            color = discord.Color.blue()

        embed = discord.Embed(title=info['name'], description=info.get('description', ''), color=color)
        if info.get('image_url'):
            embed.set_thumbnail(url=info['image_url'])

        channels = await factions_cog._get_faction_channels(ctx.guild.id, info['name'])
        if channels:
            lines = []
            for ch in channels:
                ch_obj = ctx.guild.get_channel(int(ch['channel_id']))
                ch_name = ch_obj.mention if ch_obj else f"<#{ch['channel_id']}>"
                lines.append(f"{ch_name}: ({ch['pct']:.1f}%) – {ch['status']}")
            embed.add_field(name='📍 Territories', value='\n'.join(lines[:20]), inline=False)
        else:
            embed.add_field(name='📍 Territories', value='No influence anywhere.', inline=False)

        view = None
        try:
            faction_sheet = await get_cached_faction_async()
            if faction_sheet:
                base_id = info['name'].strip().upper()
                entries = []
                for k, v in faction_sheet.items():
                    if k.startswith(base_id + ":"):
                        suffix = k.split(":", 1)[1]
                        entries.append({
                            "suffix": suffix,
                            "title": v.get("title") or v.get("name") or "Unknown",
                            "type": v.get("type", "Unknown"),
                            "description": v.get("description", ""),
                            "released": v.get("released", "true")
                        })
                if entries:
                    entries.sort(key=lambda e: e["suffix"])
                    view = FactionView(base_id, entries)
        except Exception as e:
            print(f"[FactionProgression] FactionView build error: {e}")

        return embed, view
    except Exception as e:
        print(f"[FactionProgression] build_faction_info_embed error: {e}")
        return discord.Embed(title="Error", description=f"Failed to load faction info: {e}", color=discord.Color.red()), None

# ---------------------------------------------------------------------------
# Main cog
# ---------------------------------------------------------------------------
class FactionProgression(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        asyncio.create_task(ensure_tables())

    @commands.command(name="checkfactiondb")
    @commands.check(is_admin_or_bot_admin)
    async def check_db(self, ctx):
        client = get_supabase()
        tables = ["faction_progress", "character_boons", "pending_rankups"]
        msgs = []
        for t in tables:
            try:
                res = client.table(t).select("id").limit(1).execute()
                msgs.append(f"✅ `{t}` accessible.")
            except Exception as e:
                msgs.append(f"❌ `{t}`: {e}")
        await ctx.send("\n".join(msgs))

    async def _get_progress(self, character_code, faction_id):
        client = get_supabase()
        try:
            res = client.table("faction_progress") \
                .select("*") \
                .eq("character_code", character_code) \
                .eq("faction_id", faction_id) \
                .maybe_single() \
                .execute()
        except Exception as e:
            print(f"[FactionProgression] _get_progress query error: {e}")
            raise Exception(f"Database error: {e}")

        if res and res.data:
            return res.data

        try:
            client.table("faction_progress").insert({
                "character_code": character_code,
                "faction_id": faction_id,
                "tokens": 0,
                "perks_unlocked": [],
                "perks_rejected": [],
                "updated_at": utc_now_iso()
            }).execute()
        except Exception as e:
            print(f"[FactionProgression] _get_progress insert error: {e}")
        return {"tokens": 0, "perks_unlocked": [], "perks_rejected": []}

    async def _give_tokens(self, ctx, faction_id, amount, character_code):
        item = FACTION_ITEM_MAP.get(faction_id.lower(), faction_id)
        cfg = get_guild_cfg(ctx.guild.id)
        ops_channel_id = cfg.get("operations_channel")
        if not ops_channel_id:
            raise Exception("No operations channel configured.")
        channel = self.bot.get_channel(ops_channel_id)
        if not channel:
            raise Exception("Operations channel not found.")
        cmd = f"rp!giveitem {item}x{amount} {character_code}"
        res = await run_rpforge_command(self.bot, channel, cmd, "Successfully gave items")
        if not res["success"]:
            raise Exception(res.get("error_message") or "RP‑forge command failed")
        return res

    async def _take_tokens(self, ctx, faction_id, amount, character_code):
        item = FACTION_ITEM_MAP.get(faction_id.lower(), faction_id)
        cfg = get_guild_cfg(ctx.guild.id)
        ops_channel_id = cfg.get("operations_channel")
        if not ops_channel_id:
            raise Exception("No operations channel configured.")
        channel = self.bot.get_channel(ops_channel_id)
        if not channel:
            raise Exception("Operations channel not found.")
        cmd = f"rp!takeitem {item}x{amount} {character_code}"
        res = await run_rpforge_command(self.bot, channel, cmd, "Successfully took items")
        if not res["success"]:
            raise Exception(res.get("error_message") or "RP‑forge command failed")
        return res

    # -----------------------------------------------------------------
    # Rank name helper (hardcoded)
    # -----------------------------------------------------------------
    def _rank_name(self, letter: str) -> str:
        if letter in PERKS:
            return RANK_NAMES[PERKS.index(letter)]
        return letter

    # -----------------------------------------------------------------
    # Boon info from sheet (used only after acceptance)
    # -----------------------------------------------------------------
    async def _get_boon_info(self, faction_id: str, letter: str) -> dict:
        try:
            sheet = await get_cached_faction_async()
            if sheet:
                key = f"{faction_id.upper()}:{letter.upper()}"
                row = sheet.get(key)
                if row:
                    return {
                        "title": row.get("title") or row.get("name") or f"Rank {letter}",
                        "description": row.get("description", ""),
                        "type": row.get("type", "Unknown"),
                        "released": row.get("released", "true")
                    }
        except Exception as e:
            print(f"[FactionProgression] _get_boon_info error: {e}")
        return {"title": f"Rank {letter}", "description": "", "type": "Unknown", "released": "true"}

    # =================================================================
    # Boon removal when tokens fall below threshold
    # =================================================================
    async def _check_boon_removal(self, ctx, character_code, faction_id, new_tokens):
        client = get_supabase()
        boons = client.table("character_boons") \
            .select("id,boon_key") \
            .eq("character_code", character_code) \
            .eq("faction_id", faction_id) \
            .execute()
        if not boons.data:
            return

        removed = []
        for b in boons.data:
            perk = b["boon_key"]
            if perk in PERKS:
                idx = PERKS.index(perk)
                threshold = THRESHOLDS[idx]
                if new_tokens < threshold:
                    client.table("character_boons").delete().eq("id", b["id"]).execute()
                    removed.append(perk)

        if removed:
            names = [self._rank_name(p) for p in removed]
            await ctx.send(f"⚠️ {character_code} ha perdido el/los rango(s): {', '.join(names)} por falta de experiencia.")

    # =================================================================
    # >rank <code>
    # =================================================================
    @commands.command(name="rank")
    async def rank_command(self, ctx, code: str):
        try:
            code = code.upper().strip()
            char_info = await resolve_character(code)
            if not char_info:
                await ctx.send("❌ Personaje no encontrado.")
                return
            char_code, _, _ = char_info

            faction_ids = ["Hexen", "Yuugen", "Carnival"]
            embed = discord.Embed(title=f"Progreso de Facción de {char_code}", color=discord.Color.blue())

            for i, fid in enumerate(faction_ids):
                if i > 0:
                    embed.add_field(name="\u200b", value="\u200b", inline=False)

                try:
                    prog = await self._get_progress(char_code, fid)
                except Exception:
                    prog = {"tokens": 0}
                client = get_supabase()
                boons_res = client.table("character_boons") \
                    .select("boon_key") \
                    .eq("character_code", char_code) \
                    .eq("faction_id", fid) \
                    .execute()
                unlocked_keys = [b["boon_key"] for b in (boons_res.data or [])]

                rank_title = "Ninguno"
                for perk in reversed(PERKS):
                    if perk in unlocked_keys:
                        rank_title = self._rank_name(perk)
                        break

                boon_lines = []
                for key in unlocked_keys:
                    name = self._rank_name(key)
                    boon_lines.append(f"• {name}")
                boons_text = "\n".join(boon_lines) if boon_lines else "Ninguno"

                field_value = (
                    f"**Rango:** {rank_title}\n"
                    f"**Experiencia:** {prog['tokens']} XP\n"
                    f"**Boons Desbloqueados:**\n{boons_text}"
                )
                embed.add_field(name=f"**{fid}**", value=field_value, inline=False)

            await ctx.send(embed=embed)

        except Exception as e:
            print(f"[FactionProgression] rank command error: {e}")
            await ctx.send(f"❌ Error: {e}")

    # =================================================================
    # >setrankupchannel
    # =================================================================
    @commands.command(name="setrankupchannel", aliases=["rankupchannel", "setrankup"])
    @commands.check(is_admin_or_bot_admin)
    async def set_rankup_channel(self, ctx, channel: discord.TextChannel = None):
        try:
            if channel is None:
                await ctx.send("❌ Debes mencionar un canal de texto.")
                return
            set_config(ctx.guild.id, "rankup_channel_id", channel.id)
            await ctx.send(f"✅ Canal de rank‑up configurado a {channel.mention}.")
        except Exception as e:
            print(f"[FactionProgression] setrankupchannel error: {e}")
            await ctx.send(f"❌ Error: {e}")

    # =================================================================
    # >factionboons
    # =================================================================
    @commands.command(name="factionboons", aliases=["boons"])
    async def faction_boons(self, ctx, code: str):
        try:
            char_info = await resolve_character(code)
            if not char_info:
                await ctx.send("❌ Personaje no encontrado.")
                return
            char_code, _, _ = char_info
            client = get_supabase()
            res = client.table("character_boons") \
                .select("faction_id,boon_key") \
                .eq("character_code", char_code) \
                .execute()
            boons = res.data if res else []
            if not boons:
                await ctx.send(f"ℹ️ **{char_code}** no tiene boons activos.")
                return

            lines = []
            for b in boons:
                fid = b['faction_id']
                key = b['boon_key']
                info = await self._get_boon_info(fid, key)
                lines.append(f"• **{fid}** – {self._rank_name(key)}\n  {info['description']}")
            embed = discord.Embed(
                title=f"Boons de {char_code}",
                description="\n".join(lines),
                color=discord.Color.gold()
            )
            embed.set_footer(text=f"Total: {len(boons)} / {MAX_BOONS}")
            await ctx.send(embed=embed)
        except Exception as e:
            print(f"[FactionProgression] factionboons error: {e}")
            await ctx.send(f"❌ Error: {e}")

    # =================================================================
    # Unified faction command handler
    # =================================================================
    async def _faction_command(self, ctx, faction_id: str, args: str = ""):
        try:
            parts = args.strip().split()
            if len(parts) == 0:
                embed, view = await build_faction_info_embed_and_view(ctx, faction_id)
                if view:
                    await ctx.send(embed=embed, view=view)
                else:
                    await ctx.send(embed=embed)
                return

            code = parts[0].upper().strip()

            if len(parts) == 1:
                char_info = await resolve_character(code)
                if not char_info:
                    await ctx.send("❌ Personaje no encontrado.")
                    return
                char_code, _, _ = char_info

                factions_cog = self.bot.get_cog("Factions")
                faction_info = None
                if factions_cog:
                    faction_info = await factions_cog._get_faction_info(ctx.guild.id, faction_id)

                color = discord.Color.blue()
                if faction_info and faction_info.get('color'):
                    try:
                        color = int(faction_info['color'].lstrip('#'), 16)
                    except Exception:
                        pass

                try:
                    prog = await self._get_progress(char_code, faction_id)
                except Exception as e:
                    await ctx.send(f"❌ Error de base de datos: {e}")
                    return
                client = get_supabase()
                boons_res = client.table("character_boons") \
                    .select("boon_key") \
                    .eq("character_code", char_code) \
                    .eq("faction_id", faction_id) \
                    .execute()
                unlocked_keys = [b["boon_key"] for b in (boons_res.data or [])]

                rank_title = "Ninguno"
                for perk in reversed(PERKS):
                    if perk in unlocked_keys:
                        rank_title = self._rank_name(perk)
                        break

                tokens = prog["tokens"]

                boon_lines = []
                for key in unlocked_keys:
                    name = self._rank_name(key)
                    boon_lines.append(f"• {name}")
                boons_text = "\n".join(boon_lines) if boon_lines else "Ninguno"

                embed = discord.Embed(
                    title=f"{char_code} – {faction_id}",
                    color=color
                )
                embed.add_field(name="Rango", value=rank_title, inline=True)
                embed.add_field(name="Experiencia", value=f"{tokens} XP", inline=True)
                embed.add_field(name="Boons Desbloqueados", value=boons_text, inline=False)

                await ctx.send(embed=embed)
                return

            if len(parts) >= 2:
                if not is_admin_or_bot_admin(ctx):
                    await ctx.send("❌ Solo los administradores pueden modificar tokens.")
                    return
                try:
                    delta = int(parts[1])
                except ValueError:
                    await ctx.send("❌ El segundo argumento debe ser un número con signo (ej. +5 o -3).")
                    return

                char_info = await resolve_character(code)
                if not char_info:
                    await ctx.send("❌ Personaje no encontrado.")
                    return
                char_code, discord_id, _ = char_info

                if delta > 0:
                    await self._give_faction_tokens(ctx, faction_id, delta, char_code, discord_id)
                elif delta < 0:
                    await self._remove_faction_tokens(ctx, faction_id, abs(delta), char_code)
                else:
                    await ctx.send("ℹ️ Delta es 0, no se realizaron cambios.")
                return

        except Exception as e:
            print(f"[FactionProgression] _faction_command error: {e}")
            await ctx.send(f"❌ Error inesperado: {e}")

    # =================================================================
    # Token giving – stops n-1 before next rank
    # =================================================================
    async def _give_faction_tokens(self, ctx, faction_id: str, amount: int, character_code: str, discord_id: str = None):
        if amount <= 0:
            await ctx.send("❌ La cantidad debe ser positiva.")
            return

        try:
            prog = await self._get_progress(character_code, faction_id)
        except Exception as e:
            await ctx.send(f"❌ Error de base de datos: {e}")
            return

        current_tokens = prog["tokens"]
        unlocked = prog.get("perks_unlocked", []) or []
        rejected = prog.get("perks_rejected", []) or []

        # ---- Remove any stale pending rank‑ups for this character/faction ----
        client = get_supabase()
        client.table("pending_rankups") \
            .delete() \
            .eq("character_code", character_code) \
            .eq("faction_id", faction_id) \
            .execute()

        max_possible = 45 - current_tokens
        if amount > max_possible:
            await ctx.send(f"❌ {character_code} ya tiene {current_tokens} tokens (máx 45). Solo se pueden añadir {max_possible}.")
            amount = max_possible
            if amount <= 0:
                return

        immediate = amount
        held = 0
        target_perk = None

        for i, t in enumerate(THRESHOLDS):
            perk = PERKS[i]
            if current_tokens < t and perk not in unlocked and perk not in rejected:
                max_safe = t - 1 - current_tokens
                if amount > max_safe:
                    immediate = max(0, max_safe)
                    held = amount - immediate
                    target_perk = perk
                break

        if immediate > 0:
            try:
                await self._give_tokens(ctx, faction_id, immediate, character_code)
            except Exception as e:
                await ctx.send(f"❌ Error al dar tokens: {e}")
                return

        new_tokens = current_tokens + immediate
        client.table("faction_progress").update({
            "tokens": new_tokens,
            "updated_at": utc_now_iso()
        }).eq("character_code", character_code).eq("faction_id", faction_id).execute()

        await ctx.send(f"✅ {character_code} ahora tiene {new_tokens} tokens de {faction_id}.")

        if held > 0 and target_perk:
            client.table("pending_rankups").insert({
                "character_code": character_code,
                "faction_id": faction_id,
                "new_perk": target_perk,
                "tokens_held": held,
                "status": "pending",
                "created_at": utc_now_iso()
            }).execute()

            title = self._rank_name(target_perk)
            await self._announce_rankup(ctx.guild.id, character_code, faction_id, title, discord_id)

        await self._check_boon_removal(ctx, character_code, faction_id, new_tokens)

    # =================================================================
    # Remove tokens
    # =================================================================
    async def _remove_faction_tokens(self, ctx, faction_id: str, amount: int, character_code: str):
        try:
            prog = await self._get_progress(character_code, faction_id)
        except Exception as e:
            await ctx.send(f"❌ Error de base de datos: {e}")
            return

        current = prog["tokens"]
        if amount > current:
            await ctx.send(f"❌ {character_code} solo tiene {current} tokens, no se pueden remover {amount}.")
            return

        try:
            await self._take_tokens(ctx, faction_id, amount, character_code)
        except Exception as e:
            await ctx.send(f"❌ Error al quitar tokens: {e}")
            return

        new_tokens = current - amount
        client = get_supabase()
        client.table("faction_progress").update({
            "tokens": new_tokens,
            "updated_at": utc_now_iso()
        }).eq("character_code", character_code).eq("faction_id", faction_id).execute()

        await ctx.send(f"✅ {character_code} ahora tiene {new_tokens} tokens de {faction_id} (removidos {amount}).")

        await self._check_boon_removal(ctx, character_code, faction_id, new_tokens)

    # =================================================================
    # Rank‑up announcement – title only, no spoilers
    # =================================================================
    async def _announce_rankup(self, guild_id, character_code, faction_id, perk_title, discord_id):
        cfg = get_guild_cfg(guild_id)
        channel_id = cfg.get("rankup_channel_id")
        if not channel_id:
            print(f"[FactionProgression] No rankup_channel_id configurado para el gremio {guild_id}.")
            return
        ch = self.bot.get_channel(channel_id)
        if not ch:
            print(f"[FactionProgression] Canal de rankup {channel_id} no encontrado en el gremio {guild_id}.")
            return

        mention = f"<@{discord_id}>" if discord_id else ""
        embed = discord.Embed(
            title="¡Rank Up Disponible!",
            description=(
                f"**{character_code}** ha alcanzado el rango **{perk_title}** en **{faction_id}**.\n"
                f"Usa `>rankup {character_code}` para aceptarlo o rechazarlo."
            ),
            color=discord.Color.green()
        )
        await ch.send(content=mention, embed=embed)

    # -------------------------------------------------------------------
    # Individual faction commands
    # -------------------------------------------------------------------
    @commands.command(name="hexen")
    async def hexen(self, ctx, *, args: str = ""):
        await self._faction_command(ctx, "Hexen", args)

    @commands.command(name="yuugen")
    async def yuugen(self, ctx, *, args: str = ""):
        await self._faction_command(ctx, "Yuugen", args)

    @commands.command(name="carnival")
    async def carnival(self, ctx, *, args: str = ""):
        await self._faction_command(ctx, "Carnival", args)

    # -------------------------------------------------------------------
    # >rankup
    # -------------------------------------------------------------------
    @commands.command(name="rankup")
    async def rankup(self, ctx, code: str):
        try:
            code = code.upper().strip()
            char_info = await resolve_character(code)
            if not char_info:
                await ctx.send("❌ Personaje no encontrado.")
                return
            character_code, discord_id, _ = char_info

            client = get_supabase()
            pending = client.table("pending_rankups") \
                .select("*") \
                .eq("character_code", character_code) \
                .eq("status", "pending") \
                .order("created_at") \
                .execute()
            if not pending.data:
                await ctx.send("ℹ️ No hay rank‑ups pendientes.")
                return

            row = pending.data[0]
            rank_name = self._rank_name(row["new_perk"])

            boons_res = client.table("character_boons") \
                .select("id") \
                .eq("character_code", character_code) \
                .execute()
            boon_count = len(boons_res.data or [])

            view = RankupView(self, ctx, row, boon_count, discord_id)
            embed = discord.Embed(
                title=f"Rank Up – {row['faction_id']} {rank_name}",
                description=(
                    f"**{character_code}** ha alcanzado el rango **{rank_name}** en **{row['faction_id']}**.\n"
                    f"Tokens pendientes: {row['tokens_held']}\n"
                    f"Boons usados: {boon_count}/{MAX_BOONS}"
                ),
                color=discord.Color.blue()
            )
            await ctx.send(embed=embed, view=view)
        except Exception as e:
            print(f"[FactionProgression] rankup error: {e}")
            await ctx.send(f"❌ Error: {e}")


# ---------------------------------------------------------------------------
# RankUp View
# ---------------------------------------------------------------------------
class RankupView(discord.ui.View):
    def __init__(self, cog, ctx, pending_row, boon_count, discord_id=None):
        super().__init__(timeout=300)
        self.cog = cog
        self.ctx = ctx
        self.row = pending_row
        self.boon_count = boon_count
        self.discord_id = discord_id

        accept = discord.ui.Button(label="Aceptar", style=discord.ButtonStyle.green)
        accept.callback = self.accept_callback
        self.add_item(accept)

        reject = discord.ui.Button(label="Rechazar", style=discord.ButtonStyle.red)
        reject.callback = self.reject_callback
        self.add_item(reject)

        if self.boon_count >= MAX_BOONS:
            client = get_supabase()
            res = client.table("character_boons") \
                .select("*") \
                .eq("character_code", self.row["character_code"]) \
                .execute()
            for b in (res.data or []):
                btn = discord.ui.Button(
                    label=f"Abandonar {b['faction_id']} {b['boon_key']}",
                    style=discord.ButtonStyle.secondary,
                    row=1
                )
                btn.callback = self.make_abandon_callback(b["id"])
                self.add_item(btn)

    def make_abandon_callback(self, boon_id):
        async def callback(interaction: discord.Interaction):
            await self.abandon_and_accept(interaction, boon_id)
        return callback

    async def accept_callback(self, interaction: discord.Interaction):
        if interaction.user != self.ctx.author:
            await interaction.response.send_message("Este no es tu rank‑up.", ephemeral=True)
            return

        await interaction.response.defer()
        client = get_supabase()

        # Add the new boon
        client.table("character_boons").insert({
            "character_code": self.row["character_code"],
            "faction_id": self.row["faction_id"],
            "boon_key": self.row["new_perk"],
            "unlocked_at": utc_now_iso()
        }).execute()

        # Update perks_unlocked in faction_progress
        prog = client.table("faction_progress") \
            .select("tokens,perks_unlocked,perks_rejected") \
            .eq("character_code", self.row["character_code"]) \
            .eq("faction_id", self.row["faction_id"]) \
            .maybe_single().execute()
        current_tokens = prog.data["tokens"] if prog.data else 0
        held = self.row["tokens_held"]
        unlocked = (prog.data.get("perks_unlocked") or []) if prog.data else []
        rejected = (prog.data.get("perks_rejected") or []) if prog.data else []

        if self.row["new_perk"] not in unlocked:
            unlocked.append(self.row["new_perk"])

        # Save updated perks_unlocked immediately
        client.table("faction_progress").update({
            "perks_unlocked": unlocked,
            "updated_at": utc_now_iso()
        }).eq("character_code", self.row["character_code"]).eq("faction_id", self.row["faction_id"]).execute()

        # Find the next pending threshold (for re‑holding tokens)
        next_perk = None
        next_threshold = None
        for i, t in enumerate(THRESHOLDS):
            perk = PERKS[i]
            if perk not in unlocked and perk not in rejected:
                next_perk = perk
                next_threshold = t
                break

        if next_threshold is not None:
            max_give = max(0, next_threshold - 1 - current_tokens)
            give_now = min(held, max_give)
            remaining_held = held - give_now
        else:
            give_now = held
            remaining_held = 0

        if give_now > 0:
            try:
                await self.cog._give_tokens(
                    self.ctx,
                    self.row["faction_id"],
                    give_now,
                    self.row["character_code"]
                )
            except Exception as e:
                await self.ctx.send(f"❌ Error al dar tokens: {e}")
                client.table("character_boons").delete().eq(
                    "character_code", self.row["character_code"]
                ).eq("faction_id", self.row["faction_id"]).eq("boon_key", self.row["new_perk"]).execute()
                return

        new_tokens = current_tokens + give_now
        client.table("faction_progress").update({
            "tokens": new_tokens,
            "updated_at": utc_now_iso()
        }).eq("character_code", self.row["character_code"]).eq("faction_id", self.row["faction_id"]).execute()

        # Mark this pending as accepted
        client.table("pending_rankups").update({
            "status": "accepted",
            "resolved_at": utc_now_iso()
        }).eq("id", self.row["id"]).execute()

        # ----- Display boon info in an embed (like >item) -----
        info = await self.cog._get_boon_info(self.row["faction_id"], self.row["new_perk"])
        rank_name = self.cog._rank_name(self.row["new_perk"])

        embed = discord.Embed(
            title=f"{self.row['faction_id']}:{self.row['new_perk']} – {info['title']}",
            description=info['description'],
            color=discord.Color.green()
        )
        embed.add_field(name="Tipo", value=info['type'], inline=True)
        embed.set_footer(text=f"¡{self.row['character_code']} ha aceptado el rango {rank_name}!")

        await self.ctx.send(embed=embed)

        # ----- Create next pending rank‑up if tokens remain or threshold crossed -----
        if remaining_held > 0 and next_perk:
            client.table("pending_rankups").insert({
                "character_code": self.row["character_code"],
                "faction_id": self.row["faction_id"],
                "new_perk": next_perk,
                "tokens_held": remaining_held,
                "status": "pending",
                "created_at": utc_now_iso()
            }).execute()
            title = self.cog._rank_name(next_perk)
            await self.cog._announce_rankup(self.ctx.guild.id, self.row["character_code"], self.row["faction_id"], title, self.discord_id)
        elif next_threshold and new_tokens >= next_threshold and next_perk:
            existing = client.table("pending_rankups") \
                .select("id") \
                .eq("character_code", self.row["character_code"]) \
                .eq("faction_id", self.row["faction_id"]) \
                .eq("new_perk", next_perk) \
                .eq("status", "pending") \
                .execute()
            if not existing.data:
                client.table("pending_rankups").insert({
                    "character_code": self.row["character_code"],
                    "faction_id": self.row["faction_id"],
                    "new_perk": next_perk,
                    "tokens_held": 0,
                    "status": "pending",
                    "created_at": utc_now_iso()
                }).execute()
                title = self.cog._rank_name(next_perk)
                await self.cog._announce_rankup(self.ctx.guild.id, self.row["character_code"], self.row["faction_id"], title, self.discord_id)

        self.stop()
        await interaction.message.edit(view=None)

        next_pending = client.table("pending_rankups") \
            .select("id") \
            .eq("character_code", self.row["character_code"]) \
            .eq("status", "pending") \
            .execute()
        if next_pending.data:
            await self.ctx.send("ℹ️ Tienes más rank‑ups pendientes. Usa `>rankup` de nuevo.")

    async def reject_callback(self, interaction: discord.Interaction):
        if interaction.user != self.ctx.author:
            await interaction.response.send_message("Este no es tu rank‑up.", ephemeral=True)
            return
        await interaction.response.defer()
        client = get_supabase()

        client.table("pending_rankups").update({
            "status": "rejected",
            "resolved_at": utc_now_iso()
        }).eq("id", self.row["id"]).execute()

        prog = client.table("faction_progress") \
            .select("perks_rejected") \
            .eq("character_code", self.row["character_code"]) \
            .eq("faction_id", self.row["faction_id"]) \
            .maybe_single().execute()
        rejected = prog.data.get("perks_rejected", []) if prog.data else []
        if self.row["new_perk"] not in rejected:
            rejected.append(self.row["new_perk"])
        client.table("faction_progress").update({
            "perks_rejected": rejected,
            "updated_at": utc_now_iso()
        }).eq("character_code", self.row["character_code"]).eq("faction_id", self.row["faction_id"]).execute()

        await self.ctx.send("❌ Rank rechazado. Tokens no entregados.")
        self.stop()
        await interaction.message.edit(view=None)

    async def abandon_and_accept(self, interaction: discord.Interaction, boon_id: int):
        if interaction.user != self.ctx.author:
            await interaction.response.send_message("Este no es tu rank‑up.", ephemeral=True)
            return
        await interaction.response.defer()

        get_supabase().table("character_boons").delete().eq("id", boon_id).execute()
        await self.accept_callback(interaction)