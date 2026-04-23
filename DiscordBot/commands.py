import datetime
import os
import json
import difflib
import re
import csv
import io
from collections import defaultdict

import discord
from discord.ext import commands

from .config import supabase, get_guild_cfg, set_config
from .utils import get_current_week_start_str, get_current_week_range, get_weekly_log_path, utc_now_iso
from .items import refresh_items_table, load_items_table
from .scanning import scan_guild, check_weekly_thread
from .routes import VALID_ROUTES
from .views import EnteView, find_item, load_sheet, find_image, UNLOCK_SHEET_URL, ENTE_SHEET_URL, normalize_id

# Constants for card display
CARD_LABELS = {
    "Basic_Attack": "Ataque básico",
    "AE_Card": "AE",
    "Ethrielle": "Ethrielle",
    "Negociar": "Negociar",
    "Persuadir": "Persuadir",
    "Engañar": "Engañar",
    "Halagar": "Halagar",
    "Intimidar": "Intimidar",
    "Interpretar": "Interpretar",
    "Rogar": "Rogar",
    "Sobornar": "Sobornar",
    "Seducir": "Seducir",
}

CARD_EMOJIS = {
    "Basic_Attack": "<:basicatk:1279227206157078569>",
    "AE_Card": "<:ae:1279228009039138836>",
    "Ethrielle": "<:ethrielle:1279227114213871718>",
    "Negociar": "<:diplomaticact:1279228077691637760>",
    "Persuadir": "<:diplomaticact:1279228077691637760>",
    "Engañar": "<:diplomaticact:1279228077691637760>",
    "Halagar": "<:diplomaticact:1279228077691637760>",
    "Intimidar": "<:diplomaticact:1279228077691637760>",
    "Interpretar": "<:diplomaticact:1279228077691637760>",
    "Rogar": "<:diplomaticact:1279228077691637760>",
    "Sobornar": "<:diplomaticact:1279228077691637760>",
    "Seducir": "<:diplomaticact:1279228077691637760>",
}

class BotCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._he_metadata = None   # lazy‑loaded cache for HE metadata

    # -----------------------------------------------------------------
    # Helper methods for safe parsing and formatting
    # -----------------------------------------------------------------
    def _safe_json(self, value, default):
        if value is None:
            return default
        if isinstance(value, (dict, list, int, float)):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return default
            try:
                return json.loads(text)
            except Exception:
                return default
        return default

    def _norm(self, text):
        return re.sub(r"\s+", " ", str(text).casefold()).strip()

    def _pretty_card_name(self, card_id: str) -> str:
        return CARD_LABELS.get(card_id, card_id.replace("_", " ").strip().title())

    def _get_he_map(self):
        """Load all ente metadata from the unlock sheet and cache it (so any ID can be resolved)."""
        if self._he_metadata is not None:
            return self._he_metadata
        try:
            entes = load_sheet(UNLOCK_SHEET_URL)
            he_map = {}
            for ente_id, row in entes.items():
                name = row.get("title") or ente_id
                desc = row.get("description") or "No description"
                he_map[normalize_id(ente_id)] = {"name": name, "description": desc}
                he_map[ente_id] = {"name": name, "description": desc}
            self._he_metadata = he_map
        except Exception as e:
            print(f"[HE] Failed to load HE metadata: {e}")
            self._he_metadata = {}
        return self._he_metadata

    def _format_weapon(self, weapon):
        if isinstance(weapon, str):
            weapon = self._safe_json(weapon, {})
        if not isinstance(weapon, dict):
            return str(weapon) if weapon else "Ninguna"

        name = weapon.get("name", "Ninguna")
        wtype = weapon.get("type")
        size = weapon.get("size")
        dmg = weapon.get("damageBonus")

        parts = [name]
        if wtype:
            parts.append(str(wtype))
        if size:
            if dmg is not None:
                parts.append(f"{size}({dmg})")
            else:
                parts.append(str(size))
        elif dmg is not None:
            parts.append(f"({dmg})")

        return ", ".join(parts)

    def _format_passives(self, habilidades):
        """Format passive abilities, resolving IDs to names and descriptions."""
        habilidades = self._safe_json(habilidades, [])
        if not habilidades:
            return "Ninguna"

        he_map = self._get_he_map()
        lines = []
        if isinstance(habilidades, list):
            for h in habilidades:
                if isinstance(h, dict):
                    name = h.get("name") or h.get("title") or "Habilidad"
                    text = (h.get("text") or h.get("description") or "").strip()
                else:
                    # h is likely an ID (string)
                    he_id_raw = str(h).strip()
                    he_id_norm = normalize_id(he_id_raw)+":HE"
                    row = he_map.get(he_id_norm) or he_map.get(he_id_raw)
                    if row:
                        name = row["name"]
                        text = row["description"]
                    else:
                        name = "Habilidad"
                        text = he_id_raw
                lines.append(f"- *__{name}__*\n{text}")
        else:
            lines.append(str(habilidades))

        return "\n".join(lines)

    def _format_armor(self, armor):
        armor = self._safe_json(armor, {})
        if not armor:
            return "Ninguna"
        if isinstance(armor, dict):
            bonus = armor.get("bonus", 0)
            name = armor.get("name", "")
            text = (armor.get("text") or "").strip()
            header = f"[+{bonus}] {name}".strip()
            return f"{header}\n{text}".strip()
        return str(armor)

    def _format_hp(self, hp):
        hp = self._safe_json(hp, {})
        if not isinstance(hp, dict):
            return str(hp)

        current = hp.get("baseCurrent", hp.get("current", hp.get("base", hp.get("value", 0))))

        base_max = hp.get("baseMax", 0)
        sources = hp.get("sources", [])
        enabled_bonus = 0
        if isinstance(sources, list):
            for src in sources:
                if isinstance(src, dict) and src.get("enabled", True):
                    enabled_bonus += src.get("bonus", 0) or 0
        temp_bonus = hp.get("tempBonus", 0) or 0
        char_temp_bonus = hp.get("characterTempBonus", 0) or 0
        total_max = base_max + enabled_bonus + temp_bonus + char_temp_bonus

        return f"{current}/{total_max}"
    
    def _format_barriers(self, hp):
        hp = self._safe_json(hp, {})
        if not isinstance(hp, dict):
            return ""

        barriers = hp.get("barriers", [])
        if not isinstance(barriers, list) or not barriers:
            return ""

        lines = []
        for i, b in enumerate(barriers, start=1):
            if not isinstance(b, dict):
                continue
            amount = b.get("amount", 0)
            if amount is None:
                continue

            lines.append(f"**Barrera {i} :** {amount}")

        return "\n".join(lines)

    def _format_atk(self, atk):
        atk = self._safe_json(atk, {})
        if not isinstance(atk, dict):
            return str(atk)

        if "baseCurrent" in atk or "baseMax" in atk:
            current = atk.get("baseCurrent", atk.get("current", atk.get("base", 0)))
            maximum = atk.get("baseMax", atk.get("max", current))
            return f"{current}/{maximum}"

        base = atk.get("base", 0)
        temp_bonus = atk.get("tempBonus", 0) or 0
        char_temp_bonus = atk.get("characterTempBonus", 0) or 0
        sources = atk.get("sources", [])

        bonus_total = 0
        if isinstance(sources, list):
            for src in sources:
                if isinstance(src, dict) and src.get("enabled", True):
                    bonus_total += src.get("bonus", 0) or 0

        total = base + temp_bonus + char_temp_bonus + bonus_total
        return str(total)

    def _format_stamina(self, slots):
        """Return used/total slots, including card bonuses."""
        slots = self._safe_json(slots, {})
        if isinstance(slots, dict):
            cards = slots.get("cards", [])
            if isinstance(cards, list) and cards:
                used_total = 0
                max_total = 0
                for c in cards:
                    if not isinstance(c, dict):
                        continue
                    used = c.get("used")
                    if used is None:
                        used = len(c.get("usedIndices", []) or [])
                    qty = c.get("quantity", 0) or 0

                    # Add sources/bonuses to max
                    bonus = 0
                    sources = c.get("sources", [])
                    if isinstance(sources, list):
                        for src in sources:
                            if isinstance(src, dict) and src.get("enabled", True):
                                bonus += src.get("bonus", 0) or 0

                    used_total += used
                    max_total += qty + bonus

                if max_total:
                    return f"{max_total-used_total}/{max_total}"
        return "0/0"

    def _format_cards(self, slots):
        slots = self._safe_json(slots, {})
        cards = slots.get("cards", [])
        if not isinstance(cards, list) or not cards:
            return "Ninguna"

        lines = []
        for c in cards:
            if not isinstance(c, dict):
                continue
            card_id = c.get("cardId") or c.get("name") or "Unknown"
            card_name = self._pretty_card_name(card_id)
            emoji = CARD_EMOJIS.get(card_id, "")

            total = c.get("quantity", 0) or 0
            used = c.get("used")
            if used is None:
                used = len(c.get("usedIndices", []) or [])
            current = max(total - used, 0)

            bonus = 0
            sources = c.get("sources", [])
            if isinstance(sources, list):
                for src in sources:
                    if isinstance(src, dict) and src.get("enabled", True):
                        bonus += src.get("bonus", 0) or 0
            current += bonus
            total += bonus

            prefix = f"{emoji} " if emoji else ""
            lines.append(f"{prefix}{card_name} {current}/{total}")

        return "\n".join(lines) if lines else "Ninguna"

    def _build_loadout_description(self, row):
        hp = self._format_hp(row.get("hp"))
        barriers = self._format_barriers(row.get("hp"))
        atk = self._format_atk(row.get("atk"))
        weapon = self._format_weapon(row.get("weapon"))
        passives = self._format_passives(row.get("habilidades_pasivas"))
        armor = self._format_armor(row.get("armor_class"))
        stamina = self._format_stamina(row.get("slots"))
        cards = self._format_cards(row.get("slots"))
        notes = row.get("notes")
        if isinstance(notes, str):
            notes_text = notes.strip()
        elif notes is None:
            notes_text = ""
        else:
            notes_text = json.dumps(notes, ensure_ascii=False)

        desc = (
            f"**HP :** {hp}\n"
        )
        if barriers:
            desc += f"{barriers}\n"
        desc += (
            f"**Ataque :** {atk}\n"
            f"**Arma :** {weapon}\n\n"
            f"**Habilidades pasivas:**\n{passives}\n\n"
            f"**Armor Class**\n{armor}\n\n"
            f"**Stamina :** {stamina}\n"
            f"**Cartas :**\n{cards}"
        )

        if notes_text:
            desc += f"\n\n**Notas :**\n{notes_text}"

        return desc[:3900]

    def _get_owned_character_rows(self, discord_id: int):
        user_res = supabase.table("users").select("id,discord_id").eq("discord_id", discord_id).execute()
        user_rows = user_res.data or []

        if not user_rows:
            return []

        user_ids = [row["id"] for row in user_rows]

        char_res = (
            supabase.table("characters")
            .select("id,user_id,char_name")
            .in_("user_id", user_ids)
            .execute()
        )
        chars = char_res.data or []

        return chars

    def _get_owned_loadouts(self, character_ids):
        if not character_ids:
            return []

        res = (
            supabase.table("loadouts")
            .select("*")
            .in_("character_id", character_ids)
            .execute()
        )
        return res.data or []

    def _group_loadouts_by_character(self, characters, loadouts):
        char_map = {c["id"]: c for c in characters}
        grouped = defaultdict(list)
        for row in loadouts:
            grouped[row["character_id"]].append(row)

        result = []
        for char_id, char in char_map.items():
            result.append({
                "character_id": char_id,
                "character_name": char.get("char_name") or "Unknown",
                "loadouts": sorted(grouped.get(char_id, []), key=lambda r: (r.get("name") or "").casefold())
            })
        return sorted(result, key=lambda x: x["character_name"].casefold())

    # -----------------------------------------------------------------
    # Commands
    # -----------------------------------------------------------------
    @commands.command()
    async def hour(self, ctx):
        from .utils import get_local_now
        utc = datetime.datetime.now(datetime.timezone.utc)
        local = get_local_now()
        await ctx.send(
            f"UTC: {utc.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Local: {local.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    @commands.command(aliases=["busquedas", "busqueda", "search", "setb", "busquedaschannel", "setsearch", "sb", "setbusquedas"])
    @commands.has_permissions(administrator=True)
    async def set_busquedas(self, ctx, *, arg=None):
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

    @commands.command(aliases=["setlogs", "setl", "sl", "logs", "lchannel", "log", "setoperations", "seto"])
    @commands.has_permissions(administrator=True)
    async def set_operations(self, ctx, *, arg=None):
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

    @commands.command(aliases=["scanhour", "setsh", "setscan", "sethour", "sh"])
    @commands.has_permissions(administrator=True)
    async def set_scan_hour(self, ctx, hour: str = None):
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
            "🛑 Daily scan turned off." if val is None else f"✔️ Daily scan set to **{val}:00**."
        )

    @commands.command(aliases=["refresh", "qr", "quick_refresh"])
    @commands.has_permissions(administrator=True)
    async def refresh_items(self, ctx):
        ok = await refresh_items_table()
        if ok:
            await ctx.send("✅ Items table refreshed.")
        else:
            await ctx.send("⚠️ Failed to refresh items table; check logs.")

    @commands.command(aliases=["qc", "create", "weekly", "thread", "new thread"])
    @commands.has_permissions(administrator=True)
    async def quick_create(self, ctx):
        cfg = get_guild_cfg(ctx.guild.id)
        if not cfg.get("busquedas_channel"):
            await ctx.send("Busquedas channel is not set. Use >set_busquedas first.")
            return
        start, end = get_current_week_range()
        channel = self.bot.get_channel(cfg["busquedas_channel"])
        if not channel:
            await ctx.send("Could not find the busquedas channel.")
            return
        title = f"BUSQUEDAS {start.strftime('%d %b')} - {end.strftime('%d %b')}"
        thread = discord.utils.get(channel.threads, name=title)
        if thread:
            await ctx.send(f"A thread for this week already exists: {thread.mention}")
            return
        thread = await channel.create_thread(
            name=title,
            type=discord.ChannelType.public_thread
        )
        chunked_routes = [VALID_ROUTES[i:i+6] for i in range(0, len(VALID_ROUTES), 6)]
        route_lines = ["- " + ", ".join(chunk) for chunk in chunked_routes]
        routes_text = "\n".join(route_lines)
        message = (
            "# Hilo para Búsquedas Semanales\n\n"
            "Para hacer sus búsquedas deben dejar un mensaje en el formato:\n"
            "Código de Usuario: `H0XX`\n"
            "Rutas a Visitar:\n"
            "Ruta 1\nRuta 2\n\n"
            f"**Rutas disponibles esta semana:**\n{routes_text}\n\n"
            "¡Buena Suerte con sus Búsquedas esta semana! 🍀"
        )
        await thread.send(message)
        set_config(ctx.guild.id, "last_week", start.strftime("%Y%m%d"))
        await ctx.send(f"✅ Thread created: {thread.mention}")
        await refresh_items_table()

    @commands.command(aliases=["qs","scan"])
    @commands.has_permissions(administrator=True)
    async def quick_scan(self, ctx):
        cfg = get_guild_cfg(ctx.guild.id)
        if not cfg.get("busquedas_channel"):
            await ctx.send("This server has no busquedas channel set. Use >set_busquedas.")
            return
        await ctx.send("Running quick scan for this server now...")
        await scan_guild(self.bot, ctx.guild.id, force=True)
        await ctx.send("Quick scan finished.")

    @commands.command()
    async def ping(self, ctx):
        await ctx.send("Pong!")

    @commands.command(aliases=["ficha", "personajes", "admisitrador", "web", "app"])
    async def webapp(self, ctx):
        await ctx.send("**Accede a la ficha web atravez de este sospechoso enlace:** https://osinventory-c3a0cbd8ekbzfne8.chilecentral-01.azurewebsites.net")

    @commands.command(aliases=["help", "h", "commands"])
    async def help_command(self, ctx):
        embed = discord.Embed(
            title="📘 OS Bot — Guía de Comandos",
            description="Estos son los comandos disponibles del bot, agrupados por función.\n"
                        "Úsalos con el prefijo `>`.",
            color=discord.Color.gold()
        )
        embed.add_field(
            name="Comandos para Jugador",
            value=(
                "**webapp** — Muestra un vinculo a la pagina al administrador web de fichas de personajes.\n"
                "**item** `código` — Muestra información sobre un ente específico y sus habilidades.\n"
                "**item** `código:AE /:SB/:HE/:AC` — Muestra información sobre una habilidad específica de un ente (ej. `E001:AC`).\n"
                "**loadout**  — Muestra la lista de equipaciones de todos los personajes que el usuario tenga guardados la ficha web.\n"
                "**loadout** `nombre` — Muestra la configuración de la equipacion con el nombre especificado.\n"
            ),
            inline=False
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
            name="Debugging / Moderación",
            value=(
                "**create** — Crea inmediatamente el hilo de busquedas de esta semana.\n"
                "**scan** — Escanear manualmente el hilo semanal de busquedas para este servidor ahora. \n"
                "**ping** — Verifica si el bot está en línea.\n"
                "**refresh** — Forzar la actualización de ítems desde Google Sheets y Drive.\n"
                "**remove** `código` — Elimina entradas procesadas con ese código y sincroniza.\n"
                "**push_logs** — Sube el archivo local weekly_logs a la base de datos."
            ),
            inline=False
        )
        await ctx.send(embed=embed)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def prefix(self, ctx, new_prefix: str = None):
        cfg = get_guild_cfg(ctx.guild.id)
        if not new_prefix:
            current_prefix = cfg.get("prefix", ">")
            await ctx.send(f"Current prefix is: `{current_prefix}`")
            return
        set_config(ctx.guild.id, "prefix", new_prefix)
        await ctx.send(f"✔️ Bot prefix changed to: `{new_prefix}`")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def remove(self, ctx, codigo: str):
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        # Normalize input
        codigo = codigo.upper().strip()
        if not re.match(r'^[A-Z]{1,4}\d{1,4}$', codigo):
            await ctx.send("❌ Formato inválido. Ejemplo: A123 o XYZ789.")
            return

        week = get_current_week_start_str()
        gid = str(ctx.guild.id)
        start, _ = get_current_week_range()
        log_path = get_weekly_log_path(start, ctx.guild.id)

        removed_local = 0
        removed_supabase = 0

        # 1. Clean local log file
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                keys_to_delete = [k for k, v in data.items() if v.get("codigo") == codigo]
                removed_local = len(keys_to_delete)
                for k in keys_to_delete:
                    del data[k]
                if removed_local:
                    with open(log_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4, ensure_ascii=False)
            except Exception as e:
                await ctx.send(f"❌ Local log error: {e}")
                return

        # 2. Update Supabase weekly_logs
        try:
            res = supabase.table("weekly_logs") \
                .select("data") \
                .eq("guild_id", gid) \
                .eq("week_start", week) \
                .maybe_single() \
                .execute()
            if res.data and res.data.get("data"):
                supabase_data = res.data["data"]
                keys_to_delete = [k for k, v in supabase_data.items() if v.get("codigo") == codigo]
                removed_supabase = len(keys_to_delete)
                if removed_supabase:
                    for k in keys_to_delete:
                        del supabase_data[k]
                    supabase.table("weekly_logs").upsert({
                        "guild_id": gid,
                        "week_start": week,
                        "data": supabase_data,
                        "updated_at": utc_now_iso()
                    }).execute()
        except Exception as e:
            await ctx.send(f"❌ Supabase error: {e}")
            return

        await ctx.send(
            f"🧹 Removed entries with código `{codigo}`.\n"
            f"Local log: {removed_local} removed.\n"
            f"Supabase weekly log: {removed_supabase} removed."
        )

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def debug_logs(self, ctx):
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
                preview = "\n".join(f"- {row['user_id']} → {row['codigo']}" for row in data[:5])
                if len(data) > 5:
                    preview += f"\n... and {len(data)-5} more."
                await ctx.send(f"```\n{preview}\n```")
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def push_logs(self, ctx):
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

    @commands.command(aliases=["iteminfo", "items", "skill", "skillinfo", "i", "ente", "entes", "unlocks", "unlock", "ability", "abilityinfo", "card", "cards"])
    async def item(self, ctx):
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

        # Load sheets
        try:
            unlocks = load_sheet(UNLOCK_SHEET_URL)
            entes = load_sheet(ENTE_SHEET_URL)
        except Exception as e:
            await ctx.send(f"❌ Error loading sheet: `{e}`")
            return

        if unlock_query:
            full_id = f"{base_id}:{suffix}"
            # Direct lookup in unlock sheet by full ID
            row = unlocks.get(full_id)
            if not row:
                await ctx.send("❌ Item not found in unlocks.")
                return

            # Build embed
            title = row.get("title") or row.get("name") or "Unknown"
            desc = row.get("description", "")
            typ = row.get("type", "Unknown")
            released = row.get("released", "true").lower() in ("true", "1", "yes")
            mult = {"AE": 2, "SB": 3, "HE": 4, "AC": 5}.get(suffix, 2)

            if not released:
                embed = discord.Embed(
                    title="Item Pending",
                    description=f"{full_id}\nAún no ha sido liberado.",
                    color=discord.Color.orange()
                )
            else:
                PREFIX_MAP = {
                    "AE": "ae",
                    "SB": "stat",
                    "HE": "he",
                    "AC": "armor"
                }

                emoji_name = PREFIX_MAP.get(suffix, suffix.lower())
                prefix = discord.utils.get(ctx.guild.emojis, name=emoji_name)
                prefix_str = str(prefix) if prefix else f"{suffix}:"

                embed = discord.Embed(
                    title=f"{prefix_str} {title}",
                    description=desc,
                    color=discord.Color.green()
                )

            embed.add_field(name="ID", value=full_id)
            embed.add_field(name="Type", value=typ)
            embed.add_field(name="Unlocked At", value=f"{base_id} x{mult}")

            file, url = None, None
            img_path = find_image(base_id)
            if img_path:
                file = discord.File(img_path, filename=os.path.basename(img_path))
                url = f"attachment://{os.path.basename(img_path)}"
                embed.set_image(url=url)

            if file:
                await ctx.send(embed=embed, file=file)
            else:
                await ctx.send(embed=embed)

            return

        # Base ente
        _, row = find_item(entes, base_id)
        if not row:
            await ctx.send("❌ Item not found.")
            return
        name = row.get("name") or "Unknown"
        element = row.get("elemento") or row.get("element") or "Unknown"
        embed = discord.Embed(
            title=base_id,
            description=name,
            color=discord.Color.blurple()
        )
        embed.add_field(name="Element", value=element)
        file, url = None, None
        img_path = find_image(base_id)
        if img_path:
            file = discord.File(img_path, filename=os.path.basename(img_path))
            url = f"attachment://{os.path.basename(img_path)}"
            embed.set_image(url=url)
        view = EnteView(base_id)
        if file:
            await ctx.send(embed=embed, file=file, view=view)
        else:
            await ctx.send(embed=embed, view=view)

    # -----------------------------------------------------------------
    # Loadout command
    # -----------------------------------------------------------------
    @commands.command(aliases=["loadouts", "lo", "build", "builds", "equip", "equipos", "equipo"])
    async def loadout(self, ctx, *, name: str = None):
        """
        Display a saved loadout.
        - Without argument: lists all your loadouts per character.
        - With argument: fuzzy match a loadout name (or character/loadout) and show it.
        """
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        discord_id = str(ctx.author.id)

        try:
            # Get all characters owned by this Discord user
            characters = self._get_owned_character_rows(discord_id)
            if not characters:
                await ctx.send("No characters found for your Discord account.")
                return

            # Get all loadouts for those characters
            loadouts = self._get_owned_loadouts([c["id"] for c in characters])
            grouped = self._group_loadouts_by_character(characters, loadouts)

            # ----- No argument: list all loadouts -----
            if name is None:
                if len(grouped) == 1:
                    only_char = grouped[0]
                    only_loadouts = only_char["loadouts"]

                    if not only_loadouts:
                        await ctx.send(f"❌ {only_char['character_name']} has no loadouts.")
                        return

                    if len(only_loadouts) == 1:
                        # Single loadout -> show it
                        row = only_loadouts[0]
                        embed = discord.Embed(
                            title=row["name"],
                            description=self._build_loadout_description(row),
                            color=discord.Color.blurple()
                        )
                        embed.set_footer(text=f"Character: {only_char['character_name']}")
                        await ctx.send(embed=embed)
                        return

                    # Multiple loadouts -> list them
                    embed = discord.Embed(
                        title=f"Loadouts de {only_char['character_name']}",
                        color=discord.Color.gold()
                    )
                    embed.description = "\n".join(f"• {row['name']}" for row in only_loadouts)
                    await ctx.send(embed=embed)
                    return

                # Multiple characters -> show overview
                embed = discord.Embed(
                    title="Tus personajes",
                    description="Escribe `>loadout <nombre>` para buscar uno específico.\n"
                                "También puedes usar `>loadout <personaje> / <loadout>` si hay nombres parecidos.",
                    color=discord.Color.gold()
                )
                for entry in grouped:
                    names = entry["loadouts"]
                    if names:
                        value = "\n".join(f"• {r['name']}" for r in names[:10])
                        if len(names) > 10:
                            value += f"\n... y {len(names) - 10} más."
                    else:
                        value = "Sin loadouts."
                    embed.add_field(
                        name=entry["character_name"],
                        value=value[:1024],
                        inline=False
                    )
                await ctx.send(embed=embed)
                return

            # ----- With argument: fuzzy search -----
            query = self._norm(name)

            # Try to parse "character / loadout"
            character_query = None
            loadout_query = query
            for sep in [" / ", " | ", " :: ", " - "]:
                if sep in query:
                    left, right = query.split(sep, 1)
                    character_query = left.strip()
                    loadout_query = right.strip()
                    break

            # If user gave a character name, try to match that character first
            if character_query:
                char_matches = difflib.get_close_matches(
                    character_query,
                    [self._norm(c["character_name"]) for c in grouped],
                    n=1,
                    cutoff=0.55
                )
                if char_matches:
                    chosen_char = next(c for c in grouped if self._norm(c["character_name"]) == char_matches[0])
                    if not chosen_char["loadouts"]:
                        await ctx.send(f"❌ {chosen_char['character_name']} has no loadouts.")
                        return

                    if len(chosen_char["loadouts"]) == 1:
                        row = chosen_char["loadouts"][0]
                        embed = discord.Embed(
                            title=row["name"],
                            description=self._build_loadout_description(row),
                            color=discord.Color.blurple()
                        )
                        embed.set_footer(text=f"Character: {chosen_char['character_name']}")
                        await ctx.send(embed=embed)
                        return

                    embed = discord.Embed(
                        title=f"Loadouts de {chosen_char['character_name']}",
                        color=discord.Color.gold()
                    )
                    embed.description = "\n".join(f"• {r['name']}" for r in chosen_char["loadouts"])
                    await ctx.send(embed=embed)
                    return

            # Otherwise fuzzy search all loadouts
            candidates = []
            for entry in grouped:
                for row in entry["loadouts"]:
                    candidates.append({
                        "character_name": entry["character_name"],
                        "row": row,
                        "key": self._norm(f"{entry['character_name']} / {row['name']}")
                    })

            # Try direct match on loadout name first
            direct_matches = difflib.get_close_matches(
                loadout_query,
                [self._norm(c["row"]["name"]) for c in candidates],
                n=1,
                cutoff=0.45
            )
            chosen = None
            if direct_matches:
                for c in candidates:
                    if self._norm(c["row"]["name"]) == direct_matches[0]:
                        chosen = c
                        break

            # If not found, try combined key
            if chosen is None:
                combined_matches = difflib.get_close_matches(
                    query,
                    [c["key"] for c in candidates],
                    n=1,
                    cutoff=0.35
                )
                if combined_matches:
                    chosen = next(c for c in candidates if c["key"] == combined_matches[0])

            if not chosen:
                await ctx.send("No matching loadout found.")
                return

            row = chosen["row"]
            embed = discord.Embed(
                title=row["name"],
                description=self._build_loadout_description(row),
                color=discord.Color.blurple()
            )
            embed.set_footer(text=f"Character: {chosen['character_name']}")
            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"❌ Error retrieving loadout: {e}")
            raise

    # -----------------------------------------------------------------
    # Item Deliveries Admin Command
    # -----------------------------------------------------------------
    @commands.command(aliases=["deliveries", "itemlog", "deliverylog", "entregas"])
    @commands.has_permissions(administrator=True)
    async def item_deliveries(self, ctx, limit: int = 50):
        """
        Exporta los registros de entrega de ítems (item_deliveries) a un archivo CSV.
        Uso: >item_deliveries [límite] (por defecto 50, máximo 500)
        """
        if not supabase:
            await ctx.send("❌ Supabase no está configurado.")
            return

        if limit > 500:
            limit = 500
        elif limit < 1:
            limit = 50

        try:
            res = supabase.table("item_deliveries") \
                .select("*") \
                .eq("guild_id", str(ctx.guild.id)) \
                .order("created_at", desc=True) \
                .limit(limit) \
                .execute()
            data = res.data or []
        except Exception as e:
            await ctx.send(f"❌ Error al obtener datos: {e}")
            return

        if not data:
            await ctx.send("📭 No hay registros de entregas para este servidor.")
            return

        # Crear CSV en memoria
        output = io.StringIO()
        writer = csv.writer(output)

        headers = ["ID", "Guild", "Message ID", "Target Code", "Item ID", "Route",
                   "Command", "Transaction ID", "Success", "Error", "Created At"]
        writer.writerow(headers)

        for row in data:
            writer.writerow([
                row.get("id"),
                row.get("guild_id"),
                row.get("message_id"),
                row.get("target_code"),
                row.get("item_id"),
                row.get("route"),
                row.get("command_sent"),
                row.get("transaction_id"),
                row.get("success"),
                (row.get("error_message") or "")[:100],
                row.get("created_at")
            ])

        output.seek(0)
        file = discord.File(
            io.BytesIO(output.getvalue().encode('utf-8')),
            filename=f"entregas_{ctx.guild.id}.csv"
        )

        # Estadísticas para el embed
        success_count = sum(1 for r in data if r.get("success"))
        fail_count = len(data) - success_count

        embed = discord.Embed(
            title="📦 Registros de Entrega de Ítems",
            description=f"Últimas {len(data)} entregas para este servidor.",
            color=discord.Color.blue()
        )
        embed.add_field(name="✅ Exitosas", value=str(success_count))
        embed.add_field(name="❌ Fallidas", value=str(fail_count))
        embed.set_footer(text="Adjunto CSV con el detalle completo.")

        await ctx.send(embed=embed, file=file)