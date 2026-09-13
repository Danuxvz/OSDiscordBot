import datetime
import os
import json
import re
import csv
import io

import discord
from discord.ext import commands

from .config import supabase, get_guild_cfg, set_config
from .utils import get_current_week_start_str, get_current_week_range, get_weekly_log_path, utc_now_iso
from .items import refresh_items_table, load_items_table
from .scanning import scan_guild, check_weekly_thread
from .routes import VALID_ROUTES, match_route
from .views import (
    EnteView, FactionView, find_item, get_cached_faction_async, find_image,
    get_cached_unlocks_async, get_cached_entes_async, normalize_id
)
from .routes import load_guild_aliases, get_alias_map


# ----- Pagination View for Route Entes -----
class RouteEntesView(discord.ui.View):
    def __init__(self, items, route_name, timeout=120):
        super().__init__(timeout=timeout)
        self.items = items
        self.route_name = route_name
        self.page = 0
        self.items_per_page = 10

    def get_page_data(self):
        start = self.page * self.items_per_page
        end = start + self.items_per_page
        page_items = self.items[start:end]
        embeds = []
        files = []
        for item in page_items:
            embed = discord.Embed(
                title=f"{item['id']} – {item['name']}",
                color=discord.Color.blurple()
            )
            embed.add_field(name="Tier", value=item['tier'], inline=True)
            embed.add_field(name="Elemento", value=item.get('elemento', '?'), inline=True)
            embed.add_field(name="Clase", value=item.get('clase', '?'), inline=True)

            img_path = find_image(item['id'])
            if img_path:
                file = discord.File(img_path, filename=os.path.basename(img_path))
                embed.set_thumbnail(url=f"attachment://{os.path.basename(img_path)}")
                files.append(file)
            embeds.append(embed)
        return embeds, files

    async def update_message(self, interaction):
        embeds, files = self.get_page_data()
        await interaction.response.edit_message(embeds=embeds, attachments=files, view=self)
        self.children[0].disabled = (self.page == 0)
        self.children[1].disabled = (self.page >= (len(self.items)-1) // self.items_per_page)
        await interaction.followup.edit_message(interaction.message.id, view=self)

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.primary)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
            await self.update_message(interaction)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if (self.page + 1) * self.items_per_page < len(self.items):
            self.page += 1
            await self.update_message(interaction)

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True


class BotCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

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

    @commands.command(name="mercycheck", aliases=["mcheck"])
    @commands.has_permissions(administrator=True)
    async def mercy_check(self, ctx, codigo: str):
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        codigo = codigo.upper().strip()
        try:
            res = supabase.table("character_mercy") \
                .select("streak_d, streak_c, updated_at") \
                .eq("guild_id", str(ctx.guild.id)) \
                .eq("codigo", codigo) \
                .maybe_single() \
                .execute()
            if res and res.data:
                data = res.data
                await ctx.send(
                    f"📊 **{codigo}**\n"
                    f"Streak sin D: **{data['streak_d']}**\n"
                    f"Streak sin C: **{data['streak_c']}**\n"
                    f"Última actualización: {data.get('updated_at', '?')}"
                )
            else:
                await ctx.send(f"ℹ️ No hay datos para `{codigo}` (aún no ha participado en búsquedas).")
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")

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

    @commands.command(aliases=["setdaruma", "darumachannel"])
    @commands.has_permissions(administrator=True)
    async def set_daruma_channel(self, ctx, *, arg=None):
        cfg = get_guild_cfg(ctx.guild.id)
        if arg is None:
            current_id = cfg.get("daruma_channel")
            if current_id is None:
                await ctx.send("ℹ️ This server **has no daruma announcements channel configured**.")
            else:
                ch = ctx.guild.get_channel(current_id)
                if ch:
                    await ctx.send(f"ℹ️ Current daruma channel is {ch.mention}.")
                else:
                    await ctx.send("⚠️ A daruma channel is saved but no longer exists.")
            return
        if arg.lower() in ("none", "off", "remove", "clear"):
            set_config(ctx.guild.id, "daruma_channel", None)
            await ctx.send("✔️ Daruma channel **cleared**.")
            return
        channel = None
        if arg.strip("<#>").isdigit():
            channel = ctx.guild.get_channel(int(arg.strip("<#>")))
        else:
            channel = discord.utils.get(ctx.guild.channels, mention=arg)
        if not channel or not isinstance(channel, discord.TextChannel):
            await ctx.send("❌ Invalid channel. Mention a text channel or type `none`.")
            return
        set_config(ctx.guild.id, "daruma_channel", channel.id)
        await ctx.send(f"✔️ Daruma announcements channel set to {channel.mention}.")

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

        tables_cog = self.bot.get_cog("Tables")
        if tables_cog:
            await tables_cog.reload_tables()
            await ctx.send("✅ Custom tables reloaded.")

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

    @commands.command(aliases=["qs", "scan"])
    @commands.has_permissions(administrator=True)
    async def quick_scan(self, ctx):
        cfg = get_guild_cfg(ctx.guild.id)
        if not cfg.get("busquedas_channel"):
            await ctx.send("This server has no busquedas channel set. Use >set_busquedas.")
            return
        await ctx.send("Running quick scan for this server now...")
        await scan_guild(self.bot, ctx.guild.id, force=True)
        await ctx.send("Quick scan finished.")

    @commands.command(aliases=["rutas", "availableroutes", "showroutes", "listroutes"])
    async def routes(self, ctx):
        items_table = load_items_table()
        all_routes = sorted(items_table.keys())

        if not all_routes:
            await ctx.send("No routes found. Run `>refresh_items` first.")
            return

        guild_aliases = await load_guild_aliases(str(ctx.guild.id))
        alias_map = get_alias_map(guild_aliases)
        reverse_aliases = {}
        for alias, canon in alias_map.items():
            reverse_aliases.setdefault(canon, []).append(alias)

        embed = discord.Embed(
            title="📍 Rutas disponibles",
            description=f"Total: {len(all_routes)} rutas",
            color=discord.Color.blue()
        )
        chunk_size = 15
        for i in range(0, len(all_routes), chunk_size):
            chunk = all_routes[i:i+chunk_size]
            value = "\n".join(f"• **{r}**" for r in chunk)
            embed.add_field(name="\u200b", value=value, inline=True)

        if reverse_aliases:
            alias_text = "\n".join(f"**{c}** → {', '.join(a[:3])}" for c, a in list(reverse_aliases.items())[:10])
            embed.add_field(name="Alias conocidos", value=alias_text[:1024], inline=False)

        await ctx.send(embed=embed)

    @commands.command(aliases=["addalias", "routealias", "aliasroute", "aroute", "aalias"])
    @commands.has_permissions(administrator=True)
    async def add_route_alias(self, ctx, canonical: str, *, alias: str):
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        items_table = load_items_table()
        if canonical not in items_table:
            await ctx.send(f"❌ Route `{canonical}` not found in the items sheet.")
            return

        alias_lower = alias.strip().lower()
        guild_id = str(ctx.guild.id)

        try:
            supabase.table("route_aliases").upsert({
                "guild_id": guild_id,
                "canonical": canonical,
                "alias": alias_lower
            }, on_conflict="guild_id,alias").execute()
            await ctx.send(f"✅ Alias `{alias}` → `{canonical}` added.")
        except Exception as e:
            await ctx.send(f"❌ Failed to add alias: {e}")

    @commands.command(aliases=["removealias", "delalias", "ralias"])
    @commands.has_permissions(administrator=True)
    async def remove_route_alias(self, ctx, *, alias: str):
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        guild_id = str(ctx.guild.id)
        alias_lower = alias.strip().lower()
        try:
            res = supabase.table("route_aliases") \
                .delete() \
                .eq("guild_id", guild_id) \
                .eq("alias", alias_lower) \
                .execute()
            if res.data:
                await ctx.send(f"✅ Alias `{alias}` removed.")
            else:
                await ctx.send(f"❌ Alias `{alias}` not found.")
        except Exception as e:
            await ctx.send(f"❌ Failed to remove alias: {e}")

    @commands.command(aliases=["route_items", "route_entelist", "r_entes", "rentes", "rutae", "rutaentes", "entesruta", "routeitems"])
    @commands.has_permissions(administrator=True)
    async def route_entes(self, ctx, *, route_name: str = None):
        if route_name is None:
            await ctx.send("❌ Uso: `>route_entes <nombre_de_ruta>`\nEjemplo: `>route_entes \"Rio Barakawa\"`")
            return

        try:
            items_table = load_items_table()
            if not items_table:
                await ctx.send("❌ No items loaded. Run `>refresh_items` first.")
                return

            canonical_route = await match_route(route_name, items_table, guild_id=ctx.guild.id)
            if not canonical_route:
                await ctx.send(f"❌ Ruta `{route_name}` no reconocida. Usa `>routes` para ver las rutas disponibles.")
                return

            route_data = items_table.get(canonical_route, {})
            items_by_id = {}
            for tier, item_list in route_data.items():
                for item in item_list:
                    item_id = item.get("id")
                    if item_id and item_id not in items_by_id:
                        items_by_id[item_id] = {
                            "id": item_id,
                            "name": item.get("name", "Unknown"),
                            "tier": tier,
                            "elemento": item.get("elemento", "?"),
                            "clase": item.get("clase", "?")
                        }

            if not items_by_id:
                await ctx.send(f"ℹ️ No se encontraron entes en la ruta **{canonical_route}**.")
                return

            tier_order = {"C": 0, "D": 1, "E": 2}
            sorted_items = sorted(
                items_by_id.values(),
                key=lambda x: (tier_order.get(x["tier"], 3), x["id"])
            )

            view = RouteEntesView(sorted_items, canonical_route)
            embeds, files = view.get_page_data()
            await ctx.send(embeds=embeds, files=files, view=view)

        except Exception as e:
            import traceback
            traceback.print_exc()
            await ctx.send(f"❌ Error interno: `{e}`\nRevisa la consola para más detalles.")

    @commands.command()
    async def ping(self, ctx):
        await ctx.send("Pong!")

    @commands.command(aliases=["elements", "ventajas", "tipos", "afinidades", "afinity", "elementos", "el"])
    async def element_chart(self, ctx):
        """Muestra las tablas de afinidades y elementos."""
        from .views import IMAGES_DIR

        afinidades_path = os.path.join(IMAGES_DIR, "AFINIDADES.png")
        elementos_path = os.path.join(IMAGES_DIR, "ELEMENTOS_DISCORD.png")

        files = []
        for path in (afinidades_path, elementos_path):
            if os.path.exists(path):
                files.append(discord.File(path, filename=os.path.basename(path)))
            else:
                print(f"[ELEMENTS] Missing image: {path}")

        if not files:
            await ctx.send("❌ No se encontraron las imágenes de afinidades/elementos.")
            return

        await ctx.send(files=files)    

    @commands.command(aliases=["ficha", "personajes", "admisitrador", "web", "app"])
    async def webapp(self, ctx):
        await ctx.send("**Accede a la ficha web atravez de este sospechoso enlace:** https://osinventory-c3a0cbd8ekbzfne8.chilecentral-01.azurewebsites.net")

    @commands.command(aliases=["help", "h", "commands"])
    async def help_command(self, ctx):
        embed = discord.Embed(
            title="📘 OS Bot — Guía de Comandos",
            description="Estos son los comandos disponibles del bot, agrupados por función.\n"
                        "Úsalos con el prefijo `>`. Los comandos con ⚙️ requieren permisos de administrador.",
            color=discord.Color.gold()
        )
        embed.add_field(
            name="Comandos para Jugador",
            value=(
                "**webapp** — Muestra un vínculo a la página web de gestión de fichas.\n"
                "**item** `código` — Muestra información sobre un ente.\n"
                "**item** `código:AE/:SB/:HE/:AC` — Muestra una habilidad específica (ej. `E001:AC`).\n"
                "**loadout** — Lista tus equipaciones.\n"
                "**loadout** `nombre` — Muestra una equipación concreta.\n"
                "**element_chart** — Muestra las tablas de afinidades y elementos.\n"
            ),
            inline=False
        )
        embed.add_field(
            name="📊 Tables & Rolls",
            value=(
                "**table** — Lista todas las tablas disponibles.\n"
                "**table show** `nombre` — Muestra todas las entradas de una tabla (paginado).\n"
                "**ritual** — Tira un efecto aleatorio de la tabla de rituales.\n"
                "**ritual show** — Muestra la tabla de rituales.\n"
                "**roll** `nombre` — Tira en cualquier tabla.\n"
                "*(Cada tabla creada se convierte en un comando: `>nombre` para tirar, `>nombre show` para verla.)*\n"
                "**⚙️ table create** `nombre` — Crea una nueva tabla (admin).\n"
                "**⚙️ table add** `nombre` `descripción` — Añade una entrada (admin).\n"
                "**⚙️ table remove** `nombre` `#` — Elimina una entrada (admin).\n"
                "**⚙️ table sort** `nombre` — Ordena alfabéticamente una tabla (admin)."
            ),
            inline=False
        )
        embed.add_field(
            name="Comandos de Configuración ⚙️",
            value=(
                "**setbusquedas** `#canal` — Canal donde se crean los hilos semanales de búsquedas.\n"
                "**setlogs** `#canal` — Canal de registro de operaciones.\n"
                "**sethour** `hour|off` — Hora diaria de escaneo (0‑23) o 'off'.\n"
                "**prefix** `nuevo_prefijo` — Cambia el prefijo del bot.\n"
                "**addalias** `ruta_canonica` `alias` — Añade un alias personalizado para una ruta.\n"
                "**removealias** `alias` — Elimina un alias personalizado.\n"
                "**set_daruma_channel** `#canal` — Canal de anuncios de intercambios de Daruma."
            ),
            inline=False
        )
        embed.add_field(
            name="Debugging / Moderación ⚙️",
            value=(
                "**create** — Crea el hilo de búsquedas de la semana actual.\n"
                "**scan** — Escanea manualmente el hilo semanal.\n"
                "**ping** — Verifica que el bot está online.\n"
                "**refresh** — Actualiza la tabla de ítems desde Google Sheets y Drive.\n"
                "**remove** `código` — Elimina entradas procesadas con ese código y sincroniza.\n"
                "**push_logs** — Sube el archivo local `weekly_logs` a Supabase.\n"
                "**routes** — Muestra todas las rutas disponibles y sus alias conocidos.\n"
                "**route_entes** `ruta` — Lista todos los entes de una ruta (con imágenes y paginación)."
            ),
            inline=False
        )
        embed.add_field(
            name="⚔️ Sistema de Facciones",
            value=(
                "**factions** — Muestra influencia de facciones en este canal.\n"
                "**factions** `[#canal]` — Muestra influencia en otro canal.\n"
                "**factions show** `nombre` — Muestra información de una facción.\n"
                "**⚙️ factions create** `nombre` — Crea una facción (admin).\n"
                "**⚙️ factions edit** `nombre` — Edita una facción (admin).\n"
                "**⚙️ factions set** `[#canal]` `Facción1 10, Facción2 20` — Establece puntos (admin).\n"
                "**⚙️ factions points** `[#canal] Facción +/-n` — Modifica puntos (admin).\n"
                "**⚙️ factions location** `[#canal]` — Edita ubicación (admin).\n"
                "**⚙️ factions modifiers** `[#canal]` — Config. modificadores semanales (admin)."
            ),
            inline=False
        )
        embed.add_field(
            name="📈 Progresión de Facciones",
            value=(
                "**`facción`** `código` — Muestra tu progreso en la facción.\n"
                "**⚙️ `facción`** `código +x/-y` — Añade o quita experiencia en la `facción` (admin).\n"
                "**rank** `código` — Muestra el progreso de todas las facciones.\n"
                "**rankup** `código` — Acepta o rechaza una subida de rango.\n"
                "**factionboons** `código` — Lista los boons activos de un personaje.\n"
                "**⚙️ setrankupchannel** `#canal` — Establece el canal de anuncios de rank‑up (admin)."
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

    # -----------------------------------------------------------------
    # Item command (optimised with caching)
    # -----------------------------------------------------------------
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

        try:
            unlocks = await get_cached_unlocks_async()
            entes = await get_cached_entes_async()
            faction_data = await get_cached_faction_async()
        except Exception as e:
            await ctx.send(f"❌ Error loading sheet: `{e}`")
            return

        def build_faction_embed(row, full_id):
            title = row.get("title") or row.get("name") or "Unknown"
            desc = row.get("description", "")
            typ = row.get("type", "Unknown")
            released = row.get("released", "true").lower() in ("true", "1", "yes")
            if not released:
                embed = discord.Embed(
                    title="Item Pending",
                    description=f"{full_id}\nAún no ha sido liberado.",
                    color=discord.Color.orange()
                )
            else:
                embed = discord.Embed(title=title, description=desc, color=discord.Color.blurple())
            embed.add_field(name="ID", value=full_id)
            embed.add_field(name="Type", value=typ)
            img_path = find_image(base_id)
            if img_path:
                file = discord.File(img_path, filename=os.path.basename(img_path))
                embed.set_image(url=f"attachment://{os.path.basename(img_path)}")
                return embed, file
            return embed, None

        async def send_ente_card(ente_id, row):
            name = row.get("name") or "Unknown"
            element = row.get("elemento") or row.get("element") or "Unknown"
            embed = discord.Embed(title=ente_id, description=name, color=discord.Color.blurple())
            embed.add_field(name="Element", value=element)
            img_path = find_image(ente_id)
            file = None
            if img_path:
                file = discord.File(img_path, filename=os.path.basename(img_path))
                embed.set_image(url=f"attachment://{os.path.basename(img_path)}")
            view = EnteView(ente_id)
            if file:
                await ctx.send(embed=embed, file=file, view=view)
            else:
                await ctx.send(embed=embed, view=view)

        if unlock_query:
            full_id = f"{base_id}:{suffix}"
            row = unlocks.get(full_id)
            if row:
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
                    PREFIX_MAP = {"AE": "ae", "SB": "stat", "HE": "he", "AC": "armor"}
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
                img_path = find_image(base_id)
                file = None
                if img_path:
                    file = discord.File(img_path, filename=os.path.basename(img_path))
                    embed.set_image(url=f"attachment://{os.path.basename(img_path)}")
                if file:
                    await ctx.send(embed=embed, file=file)
                else:
                    await ctx.send(embed=embed)
                return

            if faction_data:
                faction_row = faction_data.get(full_id)
                if faction_row:
                    embed, file = build_faction_embed(faction_row, full_id)
                    if file:
                        await ctx.send(embed=embed, file=file)
                    else:
                        await ctx.send(embed=embed)
                    return

            if base_id not in entes:
                error_row = entes.get("F404")
                if error_row:
                    await send_ente_card("F404", error_row)
                    return

            await ctx.send("❌ Item not found in unlocks.")
            return

        row = entes.get(base_id)
        if row:
            await send_ente_card(base_id, row)
            return

        if faction_data:
            faction_entries = []
            for k, v in faction_data.items():
                if k.startswith(base_id + ":"):
                    suffix = k.split(":", 1)[1]
                    faction_entries.append({
                        "suffix": suffix,
                        "title": v.get("title") or v.get("name") or "Unknown",
                        "type": v.get("type", "Unknown"),
                        "description": v.get("description", ""),
                        "released": v.get("released", "true")
                    })
            if faction_entries:
                faction_entries.sort(key=lambda e: e["suffix"])
                embed = discord.Embed(title=base_id, description="Hexen faction item", color=discord.Color.blurple())
                img_path = find_image(base_id)
                file = None
                if img_path:
                    file = discord.File(img_path, filename=os.path.basename(img_path))
                    embed.set_image(url=f"attachment://{os.path.basename(img_path)}")
                view = FactionView(base_id, faction_entries)
                if file:
                    await ctx.send(embed=embed, file=file, view=view)
                else:
                    await ctx.send(embed=embed, view=view)
                return

        error_row = entes.get("F404")
        if error_row:
            await send_ente_card("F404", error_row)
            return

        await ctx.send("❌ Item not found.")

    # -----------------------------------------------------------------
    # Item Deliveries Admin Command
    # -----------------------------------------------------------------
    @commands.command(aliases=["deliveries", "itemlog", "deliverylog", "entregas"])
    @commands.has_permissions(administrator=True)
    async def item_deliveries(self, ctx, limit: int = 50):
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