import json
import difflib
from collections import defaultdict
import re

import discord
from discord.ext import commands

from .config import supabase
from .views import load_sheet, UNLOCK_SHEET_URL, normalize_id

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

REFRESH_EMOJI = "🔄"

_SOURCE_BONUS_KEYS = ("bonus", "amount", "value", "bonusValue")


class LoadoutCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._he_metadata = None
        self._loadout_messages = {}

    async def cog_load(self):
        await self._load_refresh_mappings()

    async def _load_refresh_mappings(self):
        if not supabase:
            return
        try:
            res = supabase.table("loadout_messages").select("*").execute()
            for row in res.data or []:
                try:
                    msg_id = int(row["message_id"])
                except (KeyError, ValueError, TypeError):
                    continue
                self._loadout_messages[msg_id] = {
                    "channel_id": int(row["channel_id"]),
                    "loadout_id": row["loadout_id"],
                    "character_id": row["character_id"],
                    "is_npc": row["is_npc"],
                }
        except Exception:
            pass

    # -----------------------------------------------------------------
    # Helpers
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
        except Exception:
            self._he_metadata = {}
        return self._he_metadata

    # -----------------------------------------------------------------
    # Live character bonus_log access
    # -----------------------------------------------------------------
    async def _get_character_bonus_log(self, character_id):
        """Fetch the live `bonus_log` for a character (None on failure)."""
        if not supabase or not character_id:
            return None
        try:
            res = supabase.table("characters") \
                .select("bonus_log") \
                .eq("id", str(character_id)) \
                .maybe_single() \
                .execute()
            if res is not None and res.data:
                parsed = self._safe_json(res.data.get("bonus_log"), None)
                if isinstance(parsed, dict):
                    return parsed
        except Exception:
            pass
        return None

    # -----------------------------------------------------------------
    # Bonus extraction helpers (used for NPC slot → HP conversion)
    # -----------------------------------------------------------------
    def _extract_bonus_from_sources(self, sources) -> float:
        """Sum the enabled bonuses of a sources list, trying several key names."""
        if not isinstance(sources, list):
            return 0
        total = 0
        for src in sources:
            if not isinstance(src, dict):
                continue
            if not src.get("enabled", True):
                continue
            for key in _SOURCE_BONUS_KEYS:
                v = src.get(key)
                if isinstance(v, (int, float)):
                    total += v
                    break
        return total

    def _sum_hp_bonus_for_npc(self, hp, character_bonus_log) -> int:
        """
        Reproduce the web app's mergeLiveWithSaved logic for HP sources
        """
        hp = self._safe_json(hp, {})
        if not isinstance(hp, dict):
            return 0

        saved_sources = hp.get("sources", [])
        if not isinstance(saved_sources, list):
            saved_sources = []

        saved_map: dict = {}
        for s in saved_sources:
            if isinstance(s, dict):
                eid = s.get("enteId")
                if eid:
                    saved_map[eid] = s

        live_hp: dict = {}
        if isinstance(character_bonus_log, dict):
            raw = character_bonus_log.get("hp", {})
            if isinstance(raw, dict):
                live_hp = raw

        if live_hp:
            total = 0
            for eid, live_bonus in live_hp.items():
                saved = saved_map.get(eid)
                enabled = bool(saved.get("enabled", False)) if isinstance(saved, dict) else False
                if enabled:
                    try:
                        total += int(live_bonus or 0)
                    except (TypeError, ValueError):
                        pass
            return total

        # Fallback: no live data, trust the snapshot.
        total = 0
        for s in saved_sources:
            if isinstance(s, dict) and s.get("enabled", False):
                try:
                    total += int(s.get("bonus", 0) or 0)
                except (TypeError, ValueError):
                    pass
        return total

    # -----------------------------------------------------------------
    # Formatting helpers
    # -----------------------------------------------------------------
    def _format_weapon(self, weapon):
        if isinstance(weapon, str):
            weapon = self._safe_json(weapon, {})
        if not isinstance(weapon, dict):
            return str(weapon) if weapon else "Ninguna"

        name = weapon.get("name", "Ninguna")
        wtype = weapon.get("type")
        element = weapon.get("element")
        size = weapon.get("size")
        dmg = weapon.get("damageBonus")

        type_element = []
        if wtype:
            type_element.append(str(wtype))
        if element:
            type_element.append(f"({element})")
        type_element_str = " ".join(type_element)

        parts = [name]
        if type_element_str:
            parts.append(type_element_str)

        if size:
            if dmg is not None:
                parts.append(f"{size}({dmg})")
            else:
                parts.append(str(size))
        elif dmg is not None:
            parts.append(f"({dmg})")

        return ", ".join(parts)

    def _format_passives(self, habilidades, custom_he=None):
        selected_ids = []
        if isinstance(habilidades, dict):
            selected_ids = habilidades.get("selectedIds", [])
        elif isinstance(habilidades, list):
            selected_ids = habilidades

        custom_map = {c["id"]: c for c in (custom_he or []) if isinstance(c, dict)}
        he_map = self._get_he_map()
        lines = []

        for h in selected_ids:
            h = str(h).strip()

            if h in custom_map:
                c = custom_map[h]
                name = c.get("name") or "Habilidad"
                text = c.get("text") or ""
                lines.append(f"- *__{name}__*\n{text}")
                continue

            he_id_norm = normalize_id(h)
            row = he_map.get(he_id_norm + ":HE") or he_map.get(he_id_norm) or he_map.get(h)

            if row:
                name = row["name"]
                text = row["description"]
            else:
                name = "Habilidad"
                text = h

            lines.append(f"- *__{name}__*\n{text}")

        return "\n".join(lines) if lines else "Ninguna"

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
        """HP for main (non-NPC) characters."""
        hp = self._safe_json(hp, {})
        if not isinstance(hp, dict):
            return str(hp)

        current = hp.get(
            "baseCurrent",
            hp.get("current", hp.get("base", hp.get("value", 0))),
        )

        base_max = hp.get("baseMax", 0) or 0
        hp_temp = hp.get("tempBonus", 0) or 0
        hp_char_temp = hp.get("characterTempBonus", 0) or 0
        hp_source_bonus = self._extract_bonus_from_sources(hp.get("sources", []))

        total_max = base_max + hp_temp + hp_char_temp + hp_source_bonus
        return f"{current}/{total_max}"

    def _format_hp_npc(self, hp, character_bonus_log):
        """HP for NPCs: same shape as the web app's LoadoutCard totalHP."""
        hp = self._safe_json(hp, {})
        if not isinstance(hp, dict):
            return str(hp)

        current = hp.get(
            "baseCurrent",
            hp.get("current", hp.get("base", hp.get("value", 0))),
        )

        base_max = hp.get("baseMax", 0) or 0
        hp_temp = hp.get("tempBonus", 0) or 0
        hp_char_temp = hp.get("characterTempBonus", 0) or 0
        enabled_hp_bonus = self._sum_hp_bonus_for_npc(hp, character_bonus_log)

        total_max = base_max + hp_temp + hp_char_temp + enabled_hp_bonus
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
        slots = self._safe_json(slots, {})
        if not isinstance(slots, dict):
            return "0/0"

        base_slots = slots.get("base", 0) or 0
        temp_bonus = slots.get("tempBonus", 0) or 0
        char_temp_bonus = slots.get("characterTempBonus", 0) or 0

        source_bonus = 0
        sources = slots.get("sources", [])
        if isinstance(sources, list):
            for src in sources:
                if isinstance(src, dict) and src.get("enabled", True):
                    source_bonus += src.get("bonus", 0) or 0

        total_slots = base_slots + temp_bonus + char_temp_bonus + source_bonus

        used_total = 0
        cards = slots.get("cards", [])
        if isinstance(cards, list):
            for c in cards:
                if not isinstance(c, dict):
                    continue
                used = c.get("used")
                if used is None:
                    used = len(c.get("usedIndices", []) or [])
                used_total += used

        available = max(total_slots - used_total, 0)
        return f"{available}/{total_slots}"

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

    def _format_activas(self, row):
        custom_activas = self._safe_json(row.get("habilidades_activas"), [])
        selected_activa_ids = self._safe_json(row.get("selected_activa_ids"), [])
        active_ae_ids = self._safe_json(row.get("active_ae_ids"), [])

        if not selected_activa_ids:
            selected_activa_ids = [
                a.get("id") for a in custom_activas
                if isinstance(a, dict) and a.get("id")
            ]

        lines = []

        selected_custom = [
            a for a in custom_activas
            if isinstance(a, dict) and a.get("id") in selected_activa_ids
        ]
        for a in selected_custom:
            name = a.get("name") or "Activa"
            text = a.get("text") or ""
            lines.append(f"- *__{name}__*\n{text}")

        he_map = self._get_he_map()
        for ae_id in active_ae_ids:
            ae_id_str = str(ae_id).strip()
            key = f"{ae_id_str}:AE"
            row_data = he_map.get(key) or he_map.get(normalize_id(key))
            if row_data:
                name = row_data["name"]
                desc = row_data["description"]
            else:
                name = ae_id_str
                desc = ""
            lines.append(f"- *__{name}__*\n{desc}")

        return "\n".join(lines) if lines else "Ninguna"

    def _build_loadout_description(self, row, is_npc=False, character_bonus_log=None):
        if is_npc:
            hp = self._format_hp_npc(row.get("hp"), character_bonus_log)
        else:
            hp = self._format_hp(row.get("hp"))

        barriers = self._format_barriers(row.get("hp"))
        atk = self._format_atk(row.get("atk"))
        weapon = self._format_weapon(row.get("weapon"))

        custom_he = self._safe_json(row.get("custom_he"), [])
        passives = self._format_passives(row.get("habilidades_pasivas"), custom_he)

        armor = self._format_armor(row.get("armor_class"))

        if is_npc:
            activas = self._format_activas(row)
            desc = f"**HP :** {hp}\n"
            if barriers:
                desc += f"{barriers}\n"
            desc += (
                f"**Ataque :** {atk}\n"
                f"**Anrima :** {weapon}\n\n"
                f"**Habilidades pasivas:**\n{passives}\n\n"
                f"**Armor Class**\n{armor}\n\n"
                f"**Habilidades Activas:**\n{activas}"
            )
        else:
            stamina = self._format_stamina(row.get("slots"))
            cards = self._format_cards(row.get("slots"))
            desc = f"**HP :** {hp}\n"
            if barriers:
                desc += f"{barriers}\n"
            desc += (
                f"**Ataque :** {atk}\n"
                f"**Anrima :** {weapon}\n\n"
                f"**Habilidades pasivas:**\n{passives}\n\n"
                f"**Armor Class**\n{armor}\n\n"
                f"**Stamina :** {stamina}\n"
                f"**Cartas :**\n{cards}"
            )

        notes = row.get("notes")
        if isinstance(notes, str):
            notes_text = notes.strip()
        elif notes is None:
            notes_text = ""
        else:
            notes_text = json.dumps(notes, ensure_ascii=False)

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
            .select("id,user_id,char_name,external_id")
            .in_("user_id", user_ids)
            .execute()
        )
        return char_res.data or []

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
                "is_npc": not char.get("external_id"),
                "loadouts": sorted(grouped.get(char_id, []), key=lambda r: (r.get("name") or "").casefold())
            })
        return sorted(result, key=lambda x: x["character_name"].casefold())

    # -----------------------------------------------------------------
    # Helper to send a single loadout and attach refresh reaction
    # -----------------------------------------------------------------
    async def _send_single_loadout(self, ctx, row, is_npc, character_name):
        character_bonus_log = None
        if is_npc:
            character_bonus_log = await self._get_character_bonus_log(row.get("character_id"))

        embed = discord.Embed(
            title=row["name"],
            description=self._build_loadout_description(
                row, is_npc=is_npc, character_bonus_log=character_bonus_log
            ),
            color=discord.Color.blurple()
        )
        embed.set_footer(text=f"Character: {character_name}")
        msg = await ctx.send(embed=embed)

        try:
            await msg.add_reaction(REFRESH_EMOJI)
        except discord.HTTPException:
            pass

        if supabase:
            try:
                supabase.table("loadout_messages").upsert({
                    "message_id": msg.id,
                    "channel_id": ctx.channel.id,
                    "guild_id": ctx.guild.id if ctx.guild else 0,
                    "user_id": ctx.author.id,
                    "character_id": row.get("character_id"),
                    "loadout_id": row["id"],
                    "is_npc": is_npc,
                }).execute()
                self._loadout_messages[msg.id] = {
                    "channel_id": ctx.channel.id,
                    "loadout_id": row["id"],
                    "character_id": row.get("character_id"),
                    "is_npc": is_npc,
                }
            except Exception:
                pass

    # -----------------------------------------------------------------
    # Raw reaction listener (works for uncached messages)
    # -----------------------------------------------------------------
    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload):
        if payload.user_id == self.bot.user.id:
            return
        if str(payload.emoji) != REFRESH_EMOJI:
            return

        msg_id = payload.message_id
        if msg_id not in self._loadout_messages:
            return

        mapping = self._loadout_messages[msg_id]
        channel = self.bot.get_channel(payload.channel_id)
        if not channel:
            return

        try:
            message = await channel.fetch_message(msg_id)
        except discord.NotFound:
            self._loadout_messages.pop(msg_id, None)
            if supabase:
                try:
                    supabase.table("loadout_messages").delete().eq("message_id", msg_id).execute()
                except Exception:
                    pass
            return
        except (discord.Forbidden, discord.HTTPException):
            return

        user = self.bot.get_user(payload.user_id) or await self.bot.fetch_user(payload.user_id)
        if user:
            try:
                await message.remove_reaction(payload.emoji, user)
            except (discord.Forbidden, discord.HTTPException):
                pass

        loadout_id = mapping["loadout_id"]
        try:
            res = supabase.table("loadouts").select("*").eq("id", loadout_id).maybe_single().execute()
            row = res.data
        except Exception:
            await message.edit(content="❌ Error refreshing loadout.", embed=None)
            return

        if not row:
            await message.edit(content="❌ This loadout no longer exists.", embed=None)
            self._loadout_messages.pop(msg_id, None)
            if supabase:
                try:
                    supabase.table("loadout_messages").delete().eq("message_id", msg_id).execute()
                except Exception:
                    pass
            return

        character_id = mapping.get("character_id")
        character_name = "Unknown"
        if character_id:
            try:
                char_res = supabase.table("characters").select("char_name, external_id").eq("id", character_id).maybe_single().execute()
                if char_res.data:
                    character_name = char_res.data["char_name"] or "Unknown"
                    mapping["is_npc"] = not char_res.data.get("external_id")
            except Exception:
                pass

        character_bonus_log = None
        if mapping["is_npc"]:
            character_bonus_log = await self._get_character_bonus_log(character_id)

        embed = discord.Embed(
            title=row["name"],
            description=self._build_loadout_description(
                row, is_npc=mapping["is_npc"], character_bonus_log=character_bonus_log
            ),
            color=discord.Color.blurple()
        )
        embed.set_footer(text=f"Character: {character_name}")

        try:
            await message.edit(embed=embed)
        except discord.HTTPException:
            pass

    # -----------------------------------------------------------------
    # Admin command to prune the listener list
    # -----------------------------------------------------------------
    @commands.command(name="pruneloadoutlisteners", aliases=["pruneloadouts", "clearloadoutlisteners", "clearloadouts"])
    @commands.has_permissions(administrator=True)
    async def prune_loadout_listeners(self, ctx, keep: int = 50):
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return
        if keep < 1:
            await ctx.send("❌ Number to keep must be positive.")
            return

        sorted_ids = sorted(self._loadout_messages.keys(), reverse=True)
        ids_to_remove = sorted_ids[keep:]

        if not ids_to_remove:
            await ctx.send(f"ℹ️ Only {len(sorted_ids)} entries; nothing to prune.")
            return

        for mid in ids_to_remove:
            self._loadout_messages.pop(mid, None)

        try:
            supabase.table("loadout_messages").delete().in_("message_id", ids_to_remove).execute()
        except Exception as e:
            await ctx.send(f"❌ Failed to delete from Supabase: {e}")
            return

        await ctx.send(f"✅ Pruned {len(ids_to_remove)} entries. Kept {len(self._loadout_messages)} most recent.")

    # -----------------------------------------------------------------
    # Loadout command
    # -----------------------------------------------------------------
    @commands.command(aliases=["loadouts", "lo", "build", "builds", "equip", "equipos", "equipo"])
    async def loadout(self, ctx, *, name: str = None):
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        discord_id = str(ctx.author.id)

        try:
            characters = self._get_owned_character_rows(discord_id)
            if not characters:
                await ctx.send("No characters found for your Discord account.")
                return

            loadouts = self._get_owned_loadouts([c["id"] for c in characters])
            grouped = self._group_loadouts_by_character(characters, loadouts)

            if name is None:
                if len(grouped) == 1:
                    only_char = grouped[0]
                    only_loadouts = only_char["loadouts"]

                    if not only_loadouts:
                        await ctx.send(f"❌ {only_char['character_name']} has no loadouts.")
                        return

                    if len(only_loadouts) == 1:
                        row = only_loadouts[0]
                        row["character_id"] = only_char["character_id"]
                        await self._send_single_loadout(
                            ctx, row, is_npc=only_char["is_npc"],
                            character_name=only_char["character_name"]
                        )
                        return

                    embed = discord.Embed(
                        title=f"Loadouts de {only_char['character_name']}",
                        color=discord.Color.gold()
                    )
                    embed.description = "\n".join(f"• {row['name']}" for row in only_loadouts)
                    await ctx.send(embed=embed)
                    return

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

            query = self._norm(name)
            character_query = None
            loadout_query = query
            for sep in [" / ", " | ", " :: ", " - "]:
                if sep in query:
                    left, right = query.split(sep, 1)
                    character_query = left.strip()
                    loadout_query = right.strip()
                    break

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
                        row["character_id"] = chosen_char["character_id"]
                        await self._send_single_loadout(
                            ctx, row, is_npc=chosen_char["is_npc"],
                            character_name=chosen_char["character_name"]
                        )
                        return

                    embed = discord.Embed(
                        title=f"Loadouts de {chosen_char['character_name']}",
                        color=discord.Color.gold()
                    )
                    embed.description = "\n".join(f"• {r['name']}" for r in chosen_char["loadouts"])
                    await ctx.send(embed=embed)
                    return

            candidates = []
            for entry in grouped:
                for row in entry["loadouts"]:
                    candidates.append({
                        "character_name": entry["character_name"],
                        "is_npc": entry["is_npc"],
                        "character_id": entry["character_id"],
                        "row": row,
                        "key": self._norm(f"{entry['character_name']} / {row['name']}")
                    })

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
            row["character_id"] = chosen["character_id"]
            await self._send_single_loadout(
                ctx, row, is_npc=chosen["is_npc"],
                character_name=chosen["character_name"]
            )

        except Exception as e:
            await ctx.send(f"❌ Error retrieving loadout: {e}")
            raise