import asyncio
import random
import re
import unicodedata
import io
from collections import defaultdict
import discord
from discord.ext import commands, tasks
from PIL import Image, ImageDraw
from .config import set_config, supabase, get_guild_cfg, utc_now_iso
from .faction_views import (
    FactionCreateModal, LocationModal, ModifiersModal,
    CreateFactionButton, EditFactionButton, LocationButton, ModifiersButton
)

# ---------------------------------------------------------------------------
# Permission check
# ---------------------------------------------------------------------------
def is_admin_or_bot_admin(ctx: commands.Context) -> bool:
    if ctx.author.guild_permissions.administrator:
        return True
    return any(r.name.lower() == 'bot admin' for r in ctx.author.roles)

# ---------------------------------------------------------------------------
# Invisible‑character remover
# ---------------------------------------------------------------------------
def _remove_invisible(text: str) -> str:
    return ''.join(
        ch for ch in text
        if unicodedata.category(ch) not in ('Cf', 'Cc', 'Co', 'Cs')
    )

# ---------------------------------------------------------------------------
# Image‑based faction bar
# ---------------------------------------------------------------------------
def _build_bar_image(factions: list[dict], total_points: int,
                     width: int = 600, height: int = 30) -> discord.File:
    img = Image.new('RGBA', (width, height), (255, 255, 255, 0))
    draw = ImageDraw.Draw(img)
    if total_points == 0:
        draw.rectangle([0, 0, width, height], fill='#CCCCCC')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        return discord.File(buf, filename='bar.png')
    x = 0
    for f in factions:
        pct = f['points'] / total_points
        segment_w = max(1, round(pct * width))
        color = f.get('color', '#FFFFFF')
        draw.rectangle([x, 0, x + segment_w, height], fill=color)
        x += segment_w
        if x >= width:
            break
    if x < width:
        draw.rectangle([x, 0, width, height], fill='#CCCCCC')
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return discord.File(buf, filename='bar.png')

# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------
def get_status(percentage: float) -> str | None:
    if percentage <= 0:
        return None
    if percentage <= 15:
        return 'Frail'
    if percentage <= 35:
        return 'Shared'
    if percentage <= 49:
        return 'Influential'
    if percentage <= 74:
        return 'Dominant'
    if percentage <= 90:
        return 'Overwhelming'
    return 'Total control'

def faction_sort_key(f):
    return (-f['points'], f['name'])


class Factions(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._status_cache = {}          # (guild_id, channel_id, faction_lower) -> status

    # -------------------------------------------------------------------
    # Cache initialisation (prevents false announcements on restart)
    # -------------------------------------------------------------------
    async def init_status_cache(self):
        if not supabase:
            return
        self._status_cache.clear()
        try:
            all_points = supabase.table('faction_points') \
                .select('guild_id,channel_id,faction_name,points').execute()
            if not all_points or not all_points.data:
                return
            grouped = defaultdict(list)
            for row in all_points.data:
                key = (row['guild_id'], row['channel_id'])
                grouped[key].append((row['faction_name'], row['points']))
            for (guild_id, channel_id), factions in grouped.items():
                total = sum(p for _, p in factions)
                for fname, points in factions:
                    pct = (points / total * 100) if total > 0 else 0
                    status = get_status(pct)
                    if status:
                        self._status_cache[(int(guild_id), int(channel_id), fname.lower())] = status
            print(f'[FACTIONS] Status cache initialised with {len(self._status_cache)} entries.')
        except Exception as e:
            print(f'[FACTIONS] Error initialising status cache: {e}')

    # -------------------------------------------------------------------
    # Weekly faction modifiers – check and apply once per week
    # -------------------------------------------------------------------
    @tasks.loop(minutes=60)
    async def faction_weekly_check(self):
        if not supabase:
            return
        try:
            await self._apply_weekly_modifiers_if_new_week()
        except Exception as e:
            print(f'[FACTIONS] Weekly check error: {e}')

    async def _apply_weekly_modifiers_if_new_week(self):
        from .utils import get_current_week_start_str
        current_week = get_current_week_start_str()

        mods = supabase.table('faction_modifiers').select('*').execute()
        if not mods or not mods.data:
            return

        # Determine which guilds haven't been processed this week
        guilds_to_process = set()
        for row in mods.data:
            gid_str = row['guild_id']
            cfg = get_guild_cfg(int(gid_str))
            last_week = cfg.get('last_faction_week')
            if last_week != current_week:
                guilds_to_process.add(gid_str)

        if not guilds_to_process:
            return

        # 1) Collect all point changes without writing yet
        changes = {}  # guild_id -> channel_id -> faction_name -> delta
        for row in mods.data:
            if row['guild_id'] not in guilds_to_process:
                continue
            if row['min_change'] == 0 and row['max_change'] == 0:
                continue
            delta = random.randint(row['min_change'], row['max_change'])
            if delta == 0:
                continue
            gid = row['guild_id']
            cid = row['channel_id']
            fname = row['faction_name']
            changes.setdefault(gid, {}).setdefault(cid, {}).setdefault(fname, 0)
            changes[gid][cid][fname] += delta

        if not changes:
            # No actual changes, but mark guilds as processed
            for gid_str in guilds_to_process:
                set_config(int(gid_str), 'last_faction_week', current_week)
            return

        # 2) Apply changes to DB and collect affected channels
        affected_channels = set()
        for gid, ch_map in changes.items():
            for cid, f_deltas in ch_map.items():
                for fname, delta in f_deltas.items():
                    # Get current points
                    pts = supabase.table('faction_points').select('points') \
                        .eq('guild_id', gid).eq('channel_id', cid).eq('faction_name', fname) \
                        .maybe_single().execute()
                    current = pts.data.get('points', 0) if pts and pts.data else 0
                    new_points = max(0, current + delta)
                    supabase.table('faction_points').upsert({
                        'guild_id': gid,
                        'channel_id': cid,
                        'faction_name': fname,
                        'points': new_points,
                        'updated_at': utc_now_iso()
                    }, on_conflict='guild_id,channel_id,faction_name').execute()
                affected_channels.add((int(gid), int(cid)))

        # 3) For each affected channel, compute final statuses and announce changes
        for (guild_id, channel_id) in affected_channels:
            await self._announce_all_status_changes(guild_id, channel_id)

        # 4) Mark processed guilds
        for gid_str in guilds_to_process:
            set_config(int(gid_str), 'last_faction_week', current_week)

        print(f'[FACTIONS] Weekly modifiers applied to {len(guilds_to_process)} guild(s).')

    async def _announce_all_status_changes(self, guild_id: int, channel_id: int):
        """Recompute statuses for all factions in a channel and announce any changes."""
        try:
            points_data = await self._get_channel_points(guild_id, channel_id)
        except Exception:
            return
        total = sum(f['points'] for f in points_data)
        loc_name = 'esta ubicación'
        if supabase:
            try:
                loc = supabase.table('channel_locations') \
                    .select('name,alias').eq('guild_id', str(guild_id)) \
                    .eq('channel_id', str(channel_id)).maybe_single().execute()
                if loc and loc.data:
                    loc_name = loc.data.get('alias') or loc.data.get('name') or loc_name
            except Exception:
                pass

        channel = self.bot.get_channel(channel_id)
        for f in points_data:
            pct = (f['points'] / total * 100) if total > 0 else 0
            new_status = get_status(pct)
            cache_key = (guild_id, channel_id, f['name'].lower())
            old_status = self._status_cache.get(cache_key)
            if old_status != new_status and new_status is not None:
                self._status_cache[cache_key] = new_status
                if channel:
                    try:
                        await channel.send(
                            f'📢 La influencia de **{f["name"]}** en **{loc_name}** cambió a **{new_status}**'
                        )
                    except Exception:
                        pass

    # -------------------------------------------------------------------
    # Status change detection & notification (used by manual commands)
    # -------------------------------------------------------------------
    async def _check_status_change(self, guild_id: int, channel_id: int, faction_name: str):
        """Check and announce a single faction's status change (for manual updates)."""
        try:
            points_data = await self._get_channel_points(guild_id, channel_id)
        except Exception:
            return
        total = sum(f['points'] for f in points_data)
        faction_points = next((f['points'] for f in points_data if f['name'].lower() == faction_name.lower()), 0)
        pct = (faction_points / total * 100) if total > 0 else 0
        new_status = get_status(pct)

        cache_key = (guild_id, channel_id, faction_name.lower())
        old_status = self._status_cache.get(cache_key)
        if old_status != new_status and new_status is not None:
            self._status_cache[cache_key] = new_status
            loc_name = 'esta ubicación'
            try:
                if supabase:
                    loc = supabase.table('channel_locations') \
                        .select('name,alias').eq('guild_id', str(guild_id)) \
                        .eq('channel_id', str(channel_id)).maybe_single().execute()
                    if loc and loc.data:
                        loc_name = loc.data.get('alias') or loc.data.get('name') or loc_name
            except Exception:
                pass
            channel = self.bot.get_channel(channel_id)
            if channel:
                try:
                    await channel.send(
                        f'📢 La influencia de **{faction_name}** en **{loc_name}** cambió a **{new_status}**'
                    )
                except Exception:
                    pass

    # -------------------------------------------------------------------
    # Database helpers
    # -------------------------------------------------------------------
    async def _get_channel_points(self, guild_id: int, channel_id: int) -> list[dict]:
        if not supabase:
            return []
        res = supabase.table('faction_points') \
            .select('faction_name,points') \
            .eq('guild_id', str(guild_id)) \
            .eq('channel_id', str(channel_id)) \
            .execute()
        if not res:
            return []
        return [{'name': r['faction_name'], 'points': r['points']} for r in (res.data or [])]

    async def _get_faction_info(self, guild_id: int, faction_name: str) -> dict | None:
        if not supabase:
            return None
        res = supabase.table('factions') \
            .select('*').eq('guild_id', str(guild_id)) \
            .eq('name', faction_name).maybe_single().execute()
        if res and res.data:
            return res.data
        all_res = supabase.table('factions') \
            .select('*').eq('guild_id', str(guild_id)).execute()
        if all_res and all_res.data:
            for f in all_res.data:
                if f['name'].lower() == faction_name.lower():
                    return f
        return None

    async def _get_all_factions(self, guild_id: int) -> list[dict]:
        if not supabase:
            return []
        res = supabase.table('factions') \
            .select('name').eq('guild_id', str(guild_id)).execute()
        if not res:
            return []
        return res.data or []

    async def _get_faction_channels(self, guild_id: int, faction_name: str) -> list[dict]:
        if not supabase:
            return []
        all_points = supabase.table('faction_points') \
            .select('channel_id,points').eq('guild_id', str(guild_id)) \
            .eq('faction_name', faction_name).execute()
        if not all_points or not all_points.data:
            return []
        result = []
        for p in all_points.data:
            total_res = supabase.table('faction_points') \
                .select('points').eq('guild_id', str(guild_id)) \
                .eq('channel_id', p['channel_id']).execute()
            total = sum(r['points'] for r in (total_res.data or []))
            pct = (p['points'] / total * 100) if total else 0
            status = get_status(pct)
            if status:
                result.append({
                    'channel_id': p['channel_id'],
                    'points': p['points'],
                    'total': total,
                    'pct': pct,
                    'status': status
                })
        return result

    @staticmethod
    def _resolve_channel(ctx: commands.Context, arg: str | None = None):
        if arg is None:
            return ctx.channel
        raw = arg
        arg = _remove_invisible(raw).strip()
        if arg.startswith('<#') and arg.endswith('>'):
            id_str = arg[2:-1]
            if id_str.isdigit():
                channel_id = int(id_str)
                ch = ctx.guild.get_channel(channel_id)
                if ch:
                    return ch
                thread = ctx.guild.get_thread(channel_id)
                if thread:
                    return thread
        if arg.isdigit():
            channel_id = int(arg)
            ch = ctx.guild.get_channel(channel_id)
            if ch:
                return ch
            thread = ctx.guild.get_thread(channel_id)
            if thread:
                return thread
        ch = discord.utils.get(ctx.guild.text_channels, name=arg)
        if ch:
            return ch
        thread = discord.utils.get(ctx.guild.threads, name=arg)
        if thread:
            return thread
        ch = discord.utils.get(ctx.guild.channels, mention=raw)
        if ch:
            return ch
        thread = discord.utils.get(ctx.guild.threads, mention=raw)
        if thread:
            return thread
        return None

    # -------------------------------------------------------------------
    # >factions  (display)
    # -------------------------------------------------------------------
    @commands.group(name='factions', aliases=['faction', 'facciones', 'fa', 'f'], invoke_without_command=True)
    async def factions_group(self, ctx: commands.Context, channel_arg: str = None):
        try:
            channel = self._resolve_channel(ctx, channel_arg)
            if channel is None:
                await ctx.send('❌ Canal no encontrado.')
                return

            points_data = await self._get_channel_points(ctx.guild.id, channel.id)
            if not points_data:
                all_factions = await self._get_all_factions(ctx.guild.id)
                if not all_factions:
                    await ctx.send('ℹ️ No hay facciones creadas en este servidor.')
                    return
                desc = '**Facciones del servidor:**\n' + '\n'.join(f'• {f["name"]}' for f in all_factions)
                embed = discord.Embed(title='⚔️ Facciones', description=desc, color=discord.Color.gold())
                await ctx.send(embed=embed)
                return

            loc = {}
            if supabase:
                try:
                    loc_res = supabase.table('channel_locations') \
                        .select('name,alias,description,image_url') \
                        .eq('guild_id', str(ctx.guild.id)) \
                        .eq('channel_id', str(channel.id)).maybe_single().execute()
                    if loc_res and loc_res.data:
                        loc = loc_res.data
                except Exception as e:
                    print(f'[FACTIONS] Error loading location: {e}')

            enriched = []
            total_points = sum(f['points'] for f in points_data)
            for f in points_data:
                info = await self._get_faction_info(ctx.guild.id, f['name']) or {}
                pct = (f['points'] / total_points * 100) if total_points > 0 else 0
                status = get_status(pct)
                if status is None:
                    continue
                enriched.append({
                    'name': f['name'],
                    'points': f['points'],
                    'pct': pct,
                    'status': status,
                    'color': info.get('color', '#FFFFFF'),
                    'image_url': info.get('image_url', ''),
                    'description': info.get('description', '')
                })

            if not enriched:
                all_factions = await self._get_all_factions(ctx.guild.id)
                desc = 'No hay influencia en este canal. Facciones disponibles:\n' + \
                       '\n'.join(f'• {f["name"]} (0 pts)' for f in all_factions)
                embed = discord.Embed(title=f'📍 {channel.name}', description=desc, color=discord.Color.greyple())
                await ctx.send(embed=embed)
                return

            enriched.sort(key=faction_sort_key)
            dominant_color = discord.Color.blue()
            try:
                dom_hex = enriched[0]['color'].lstrip('#')
                dominant_color = int(dom_hex, 16)
            except Exception:
                pass

            loc_name = (loc.get('alias') or loc.get('name')) if loc else channel.name
            embed = discord.Embed(
                title=f'📍 {loc_name}',
                description=loc.get('description', '') if loc else '',
                color=dominant_color
            )
            if loc.get('image_url'):
                embed.set_thumbnail(url=loc['image_url'])

            for f in enriched:
                embed.add_field(
                    name=f'{f["name"]} – {f["status"]}',
                    value=f'{f["pct"]:.1f}%',
                    inline=True
                )

            bar_file = _build_bar_image(enriched, total_points)
            embed.set_image(url=f'attachment://{bar_file.filename}')
            await ctx.send(embed=embed, file=bar_file)

        except Exception as e:
            print(f'[FACTIONS] Display error: {e}')
            import traceback
            traceback.print_exc()
            await ctx.send(f'❌ Error al mostrar facciones: {e}')

    # -------------------------------------------------------------------
    # >factions create <name>
    # -------------------------------------------------------------------
    @factions_group.command(name='create')
    @commands.check(is_admin_or_bot_admin)
    async def factions_create(self, ctx: commands.Context, *, name: str):
        faction_name = name.strip()
        if not faction_name:
            await ctx.send('❌ Debes dar un nombre.')
            return
        view = CreateFactionButton(ctx.guild.id, faction_name)
        await ctx.send(f'🖊️ Haz clic para configurar la facción **{faction_name}**:', view=view)

    # -------------------------------------------------------------------
    # >factions edit <name>
    # -------------------------------------------------------------------
    @factions_group.command(name='edit')
    @commands.check(is_admin_or_bot_admin)
    async def factions_edit(self, ctx: commands.Context, *, name: str):
        faction_name = name.strip()
        info = await self._get_faction_info(ctx.guild.id, faction_name)
        if not info:
            await ctx.send(f'❌ Facción **{faction_name}** no encontrada.')
            return
        view = EditFactionButton(ctx.guild.id, info['name'])
        await ctx.send(f'🖊️ Haz clic para editar la facción **{info["name"]}**:', view=view)

    # -------------------------------------------------------------------
    # >factions set [channel] F1 10, F2 20
    # -------------------------------------------------------------------
    @factions_group.command(name='set')
    @commands.check(is_admin_or_bot_admin)
    async def factions_set(self, ctx: commands.Context, channel_arg: str = None, *, points_str: str = None):
        channel = self._resolve_channel(ctx, channel_arg)
        if channel is None:
            await ctx.send('❌ Canal no encontrado.')
            return

        if not points_str:
            await ctx.send('❌ Debes especificar los puntos. Ej: `F1 10, F2 20`')
            return

        pairs = re.findall(r'([A-Za-z0-9_]+)\s+(-?\d+)', points_str)
        if not pairs:
            await ctx.send('❌ Formato inválido. Ej: `Carnaval 10, Hexen 20`')
            return

        all_factions = await self._get_all_factions(ctx.guild.id)
        valid_names = {f['name'].lower(): f['name'] for f in all_factions}
        invalid = []
        resolved = []
        for name, pts_str in pairs:
            real_name = valid_names.get(name.strip().lower())
            if not real_name:
                invalid.append(name.strip())
            else:
                resolved.append((real_name, pts_str))
        if invalid:
            await ctx.send(
                f'❌ Las siguientes facciones no existen: {", ".join(invalid)}\n'
                f'Usa `>factions create <nombre>` primero.'
            )
            return

        try:
            for fname, pts_str in resolved:
                pts = max(0, int(pts_str))
                supabase.table('faction_points').upsert({
                    'guild_id': str(ctx.guild.id),
                    'channel_id': str(channel.id),
                    'faction_name': fname,
                    'points': pts,
                    'updated_at': utc_now_iso()
                }, on_conflict='guild_id,channel_id,faction_name').execute()
            await ctx.send(f'✅ Puntos actualizados en {channel.mention}.')
            # After all points set, announce all changes at once
            await self._announce_all_status_changes(ctx.guild.id, channel.id)
        except Exception as e:
            await ctx.send(f'❌ Error: {e}')

    # -------------------------------------------------------------------
    # >factions points [#canal] [Faction +/-n]
    # -------------------------------------------------------------------
    @factions_group.command(name='points')
    @commands.check(is_admin_or_bot_admin)
    async def factions_points(self, ctx: commands.Context, arg1: str = None, arg2: str = None, arg3: str = None):
        channel = ctx.channel
        faction_name = None
        delta_str = None

        ch = self._resolve_channel(ctx, arg1) if arg1 else None
        if ch:
            channel = ch
            if arg2 and arg3:
                faction_name = arg2.strip()
                delta_str = arg3.strip()
        else:
            if arg1:
                faction_name = arg1.strip()
                delta_str = arg2.strip() if arg2 else None

        # Show all points if no faction/delta
        if not faction_name and not delta_str:
            points_data = await self._get_channel_points(ctx.guild.id, channel.id)
            if not points_data:
                await ctx.send(f'ℹ️ No hay puntos de facciones en {channel.mention}.')
                return
            lines = [f'**{f["name"]}**: {f["points"]} pts' for f in points_data]
            embed = discord.Embed(
                title=f'📊 Puntos en {channel.mention}',
                description='\n'.join(lines),
                color=discord.Color.blue()
            )
            await ctx.send(embed=embed)
            return

        if not faction_name or not delta_str:
            await ctx.send('❌ Uso: `>factions points [canal] [Facción +/-cantidad]`')
            return

        info = await self._get_faction_info(ctx.guild.id, faction_name)
        if not info:
            await ctx.send(f'❌ Facción **{faction_name}** no existe.')
            return
        real_name = info['name']

        try:
            delta = int(delta_str)
        except ValueError:
            await ctx.send('❌ La cantidad debe ser un número entero (ej: +10, -5).')
            return

        current = 0
        if supabase:
            res = supabase.table('faction_points') \
                .select('points') \
                .eq('guild_id', str(ctx.guild.id)) \
                .eq('channel_id', str(channel.id)) \
                .eq('faction_name', real_name) \
                .maybe_single().execute()
            if res and res.data:
                current = res.data['points']

        new_pts = max(0, current + delta)

        try:
            supabase.table('faction_points').upsert({
                'guild_id': str(ctx.guild.id),
                'channel_id': str(channel.id),
                'faction_name': real_name,
                'points': new_pts,
                'updated_at': utc_now_iso()
            }, on_conflict='guild_id,channel_id,faction_name').execute()
            await ctx.send(f'✅ **{real_name}**: {current} → {new_pts} pts en {channel.mention}')
            # Single faction change – check immediately
            await self._check_status_change(ctx.guild.id, channel.id, real_name)
        except Exception as e:
            await ctx.send(f'❌ Error: {e}')

    # -------------------------------------------------------------------
    # >factions location / loc [channel]
    # -------------------------------------------------------------------
    @factions_group.command(name='location', aliases=['loc'])
    @commands.check(is_admin_or_bot_admin)
    async def factions_location(self, ctx: commands.Context, channel_arg: str = None):
        channel = self._resolve_channel(ctx, channel_arg)
        if channel is None:
            await ctx.send('❌ Canal no encontrado.')
            return
        current = {}
        if supabase:
            try:
                res = supabase.table('channel_locations') \
                    .select('*').eq('guild_id', str(ctx.guild.id)) \
                    .eq('channel_id', str(channel.id)).maybe_single().execute()
                if res and res.data:
                    current = res.data
            except Exception as e:
                print(f'[FACTIONS] Error loading location for edit: {e}')
        view = LocationButton(ctx.guild.id, channel.id, current)
        await ctx.send(f'📍 Haz clic para editar la ubicación de {channel.mention}:', view=view)

    # -------------------------------------------------------------------
    # >factions modifiers / modifier / mod / mods [channel]
    # -------------------------------------------------------------------
    @factions_group.command(name='modifiers', aliases=['modifier', 'mod', 'mods'])
    @commands.check(is_admin_or_bot_admin)
    async def factions_modifiers(self, ctx: commands.Context, channel_arg: str = None):
        channel = self._resolve_channel(ctx, channel_arg)
        if channel is None:
            await ctx.send('❌ Canal no encontrado.')
            return
        faction_names = []
        current_mods = {}
        if supabase:
            fac_res = supabase.table('factions').select('name').eq('guild_id', str(ctx.guild.id)).execute()
            if fac_res:
                faction_names = [r['name'] for r in (fac_res.data or [])]
            mod_res = supabase.table('faction_modifiers').select('*').eq('guild_id', str(ctx.guild.id)) \
                .eq('channel_id', str(channel.id)).execute()
            if mod_res:
                for r in (mod_res.data or []):
                    current_mods[r['faction_name']] = {
                        'min_change': r['min_change'],
                        'max_change': r['max_change']
                    }
        if not faction_names:
            await ctx.send('❌ No hay facciones creadas. Usa `>factions create <nombre>` primero.')
            return
        if len(faction_names) > 5:
            await ctx.send(f'⚠️ Demasiadas facciones ({len(faction_names)}). Solo se pueden editar las 5 primeras.')
            faction_names = faction_names[:5]
        view = ModifiersButton(ctx.guild.id, channel.id, faction_names, current_mods)
        await ctx.send(f'🎲 Haz clic para configurar los modificadores semanales de {channel.mention}:', view=view)

    # -------------------------------------------------------------------
    # >factions info / inf / show <name>
    # -------------------------------------------------------------------
    @factions_group.command(name='info', aliases=['inf', 'show'])
    async def factions_info(self, ctx: commands.Context, *, name: str):
        info = await self._get_faction_info(ctx.guild.id, name.strip())
        if not info:
            await ctx.send(f'❌ Facción **{name}** no encontrada.')
            return
        color_hex = info.get('color', '#FFFFFF').lstrip('#')
        try:
            color = int(color_hex, 16)
        except Exception:
            color = discord.Color.blue()
        embed = discord.Embed(
            title=info['name'],
            description=info.get('description', 'Sin descripción.'),
            color=color
        )
        if info.get('image_url'):
            embed.set_thumbnail(url=info['image_url'])
        channels = await self._get_faction_channels(ctx.guild.id, info['name'])
        if channels:
            lines = []
            for ch in channels:
                channel_obj = self.bot.get_channel(int(ch['channel_id']))
                ch_name = channel_obj.mention if channel_obj else f"<#{ch['channel_id']}>"
                lines.append(f"{ch_name}: ({ch['pct']:.1f}%) – {ch['status']}")
            embed.add_field(name='📍 Territorios', value='\n'.join(lines[:20]), inline=False)
        else:
            embed.add_field(name='📍 Territorios', value='Sin influencia en ningún canal.', inline=False)
        await ctx.send(embed=embed)

    # -------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------
    @commands.Cog.listener()
    async def on_ready(self):
        await self.init_status_cache()
        if not self.faction_weekly_check.is_running():
            self.faction_weekly_check.start()