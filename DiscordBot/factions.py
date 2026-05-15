import asyncio
import random
import re
import discord
from discord.ext import commands, tasks
from .config import supabase, get_guild_cfg, utc_now_iso
from .faction_views import (
    FactionCreateModal, LocationModal, ModifiersModal,
    CreateFactionButton, EditFactionButton, LocationButton, ModifiersButton
)

def get_status(percentage: float) -> str:
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

def build_bar(factions: list[dict]) -> str:
    total = sum(f['points'] for f in factions)
    if total == 0:
        return '▬' * 20 + ' (sin influencia)'
    bar = ''
    for f in factions:
        pct = f['points'] / total
        blocks = max(1, round(pct * 20))
        bar += '█' * blocks
    return bar

def faction_sort_key(f):
    return (-f['points'], f['name'])


class Factions(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._status_cache = {}

    # -------------------------------------------------------------------
    # Weekly random modifiers
    # -------------------------------------------------------------------
    @tasks.loop(hours=168)
    async def weekly_faction_modifiers(self):
        if not supabase:
            return
        try:
            mods = supabase.table('faction_modifiers').select('*').execute()
            for row in (mods.data or []):
                if row['min_change'] == 0 and row['max_change'] == 0:
                    continue
                delta = random.randint(row['min_change'], row['max_change'])
                if delta == 0:
                    continue
                pts = supabase.table('faction_points').select('points') \
                    .eq('guild_id', row['guild_id']) \
                    .eq('channel_id', row['channel_id']) \
                    .eq('faction_name', row['faction_name']) \
                    .maybe_single().execute()
                current = pts.data.get('points', 0) if pts.data else 0
                new_points = max(0, current + delta)
                supabase.table('faction_points').upsert({
                    'guild_id': row['guild_id'],
                    'channel_id': row['channel_id'],
                    'faction_name': row['faction_name'],
                    'points': new_points,
                    'updated_at': utc_now_iso()
                }, on_conflict='guild_id,channel_id,faction_name').execute()
                await self._check_status_change(
                    int(row['guild_id']), int(row['channel_id']),
                    row['faction_name']
                )
            print('[FACTIONS] Weekly modifiers applied.')
        except Exception as e:
            print(f'[FACTIONS] Weekly modifier error: {e}')

    # -------------------------------------------------------------------
    # Status change detection & notification
    # -------------------------------------------------------------------
    async def _check_status_change(self, guild_id: int, channel_id: int, faction_name: str):
        points_data = await self._get_channel_points(guild_id, channel_id)
        total = sum(f['points'] for f in points_data)
        faction_points = next((f['points'] for f in points_data if f['name'] == faction_name), 0)
        pct = (faction_points / total * 100) if total > 0 else 0
        new_status = get_status(pct)

        cache_key = (guild_id, channel_id, faction_name)
        old_status = self._status_cache.get(cache_key)
        if old_status != new_status and new_status is not None:
            self._status_cache[cache_key] = new_status
            loc = supabase.table('channel_locations') \
                .select('name,alias').eq('guild_id', str(guild_id)) \
                .eq('channel_id', str(channel_id)).maybe_single().execute()
            loc_name = 'esta ubicación'
            if loc.data:
                loc_name = loc.data.get('alias') or loc.data.get('name') or loc_name

            channel = self.bot.get_channel(channel_id)
            if channel:
                try:
                    await channel.send(
                        f'📢 **{faction_name}** influencia en **{loc_name}** cambió a **{new_status}**'
                    )
                except Exception:
                    pass

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------
    async def _get_channel_points(self, guild_id: int, channel_id: int) -> list[dict]:
        if not supabase:
            return []
        res = supabase.table('faction_points') \
            .select('faction_name,points') \
            .eq('guild_id', str(guild_id)) \
            .eq('channel_id', str(channel_id)) \
            .execute()
        return [{'name': r['faction_name'], 'points': r['points']} for r in (res.data or [])]

    async def _get_faction_info(self, guild_id: int, faction_name: str) -> dict:
        if not supabase:
            return None
        res = supabase.table('factions') \
            .select('*').eq('guild_id', str(guild_id)) \
            .eq('name', faction_name).maybe_single().execute()
        return res.data

    async def _get_all_factions(self, guild_id: int) -> list[dict]:
        if not supabase:
            return []
        res = supabase.table('factions') \
            .select('*').eq('guild_id', str(guild_id)).execute()
        return res.data or []

    @staticmethod
    def _clean_channel_arg(arg: str):
        """Remove invisible Unicode characters and extract a numeric ID."""
        if not arg:
            return None
        # Remove all characters except digits, #, <, >, and spaces
        cleaned = re.sub(r'[^\d<># ]', '', arg)
        # Try to extract a numeric ID
        match = re.search(r'(\d+)', cleaned)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def _resolve_channel(ctx, arg=None):
        """Robust channel resolution with invisible‑character cleaning."""
        if arg is None:
            return ctx.channel
        # Clean the argument of invisible characters and extract ID
        channel_id = Factions._clean_channel_arg(arg)
        if channel_id:
            ch = ctx.guild.get_channel(int(channel_id))
            if ch:
                return ch
        # Also try direct mention parsing (in case the regex missed)
        arg = arg.strip()
        if arg.startswith('<#') and arg.endswith('>'):
            id_str = arg[2:-1]
            if id_str.isdigit():
                ch = ctx.guild.get_channel(int(id_str))
                if ch:
                    return ch
        # Debug log
        print(f'[FACTIONS] Could not resolve channel: arg={repr(arg)}')
        return None

    # -------------------------------------------------------------------
    # >factions  (display)
    # -------------------------------------------------------------------
    @commands.group(name='factions', aliases=['faction'], invoke_without_command=True)
    async def factions_group(self, ctx: commands.Context, channel_arg: str = None):
        """Muestra la influencia de facciones en este canal o en #canal."""
        channel = self._resolve_channel(ctx, channel_arg)
        if channel is None:
            await ctx.send('❌ Canal no encontrado.')
            return

        points_data = await self._get_channel_points(ctx.guild.id, channel.id)

        # If no points set in this channel, show server-wide faction list
        if not points_data:
            all_factions = await self._get_all_factions(ctx.guild.id)
            if not all_factions:
                await ctx.send('ℹ️ No hay facciones creadas en este servidor.')
                return
            desc = '**Facciones del servidor:**\n' + '\n'.join(f'• {f["name"]}' for f in all_factions)
            embed = discord.Embed(title='⚔️ Facciones', description=desc, color=discord.Color.gold())
            await ctx.send(embed=embed)
            return

        # Channel has faction points – build influence embed
        loc = None
        if supabase:
            loc_res = supabase.table('channel_locations') \
                .select('*').eq('guild_id', str(ctx.guild.id)) \
                .eq('channel_id', str(channel.id)).maybe_single().execute()
            loc = loc_res.data

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
            # All factions have 0% – show faction names with 0 pts
            all_factions = await self._get_all_factions(ctx.guild.id)
            desc = 'No hay influencia en este canal. Facciones disponibles:\n' + \
                   '\n'.join(f'• {f["name"]} (0 pts)' for f in all_factions)
            embed = discord.Embed(title=f'📍 {channel.name}', description=desc, color=discord.Color.greyple())
            await ctx.send(embed=embed)
            return

        enriched.sort(key=faction_sort_key)

        loc_name = (loc.get('alias') or loc.get('name')) if loc else channel.name
        embed = discord.Embed(
            title=f'📍 {loc_name}',
            description=loc.get('description', '') if loc else '',
            color=discord.Color.blue()
        )
        if loc and loc.get('image_url'):
            embed.set_thumbnail(url=loc['image_url'])

        for f in enriched:
            embed.add_field(
                name=f'{f["name"]} – {f["status"]}',
                value=f'{f["points"]} pts ({f["pct"]:.1f}%)',
                inline=True
            )

        bar = build_bar(enriched)
        embed.add_field(name='\u200b', value=f'`{bar}`', inline=False)

        await ctx.send(embed=embed)

    # -------------------------------------------------------------------
    # Admin subcommands
    # -------------------------------------------------------------------
    @factions_group.command(name='create')
    @commands.has_permissions(administrator=True)
    async def factions_create(self, ctx: commands.Context, *, name: str):
        faction_name = name.strip()
        if not faction_name:
            await ctx.send('❌ Debes dar un nombre.')
            return
        view = CreateFactionButton(ctx.guild.id, faction_name)
        await ctx.send(f'🖊️ Haz clic para configurar la facción **{faction_name}**:', view=view)

    @factions_group.command(name='edit')
    @commands.has_permissions(administrator=True)
    async def factions_edit(self, ctx: commands.Context, *, name: str):
        faction_name = name.strip()
        info = await self._get_faction_info(ctx.guild.id, faction_name)
        if not info:
            await ctx.send(f'❌ Facción **{faction_name}** no encontrada.')
            return
        view = EditFactionButton(ctx.guild.id, faction_name)
        await ctx.send(f'🖊️ Haz clic para editar la facción **{faction_name}**:', view=view)

    @factions_group.command(name='set')
    @commands.has_permissions(administrator=True)
    async def factions_set(self, ctx: commands.Context, channel_arg: str, *, points_str: str):
        channel = self._resolve_channel(ctx, channel_arg)
        if channel is None:
            await ctx.send('❌ Canal no encontrado.')
            return

        pairs = re.findall(r'([A-Za-z0-9_]+)\s+(-?\d+)', points_str)
        if not pairs:
            await ctx.send('❌ Formato inválido. Ej: `Carnaval 10, Hexen 20`')
            return

        try:
            for fname, pts_str in pairs:
                pts = max(0, int(pts_str))
                supabase.table('faction_points').upsert({
                    'guild_id': str(ctx.guild.id),
                    'channel_id': str(channel.id),
                    'faction_name': fname.strip(),
                    'points': pts,
                    'updated_at': utc_now_iso()
                }, on_conflict='guild_id,channel_id,faction_name').execute()
            await ctx.send(f'✅ Puntos actualizados en {channel.mention}.')
        except Exception as e:
            await ctx.send(f'❌ Error: {e}')

    @factions_group.command(name='points')
    @commands.has_permissions(administrator=True)
    async def factions_points(self, ctx: commands.Context, arg1: str, arg2: str = None, arg3: str = None):
        channel = ctx.channel
        faction_name = None
        delta_str = None

        ch = self._resolve_channel(ctx, arg1)
        if ch:
            channel = ch
            if arg2 and arg3:
                faction_name = arg2
                delta_str = arg3
        else:
            faction_name = arg1
            delta_str = arg2

        if not faction_name or not delta_str:
            await ctx.send('❌ Uso: `>factions points [#canal] Facción +/-cantidad`')
            return

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
                .eq('faction_name', faction_name) \
                .maybe_single().execute()
            if res.data:
                current = res.data['points']

        new_pts = max(0, current + delta)

        try:
            supabase.table('faction_points').upsert({
                'guild_id': str(ctx.guild.id),
                'channel_id': str(channel.id),
                'faction_name': faction_name,
                'points': new_pts,
                'updated_at': utc_now_iso()
            }, on_conflict='guild_id,channel_id,faction_name').execute()
            await ctx.send(f'✅ **{faction_name}**: {current} → {new_pts} pts en {channel.mention}')
            await self._check_status_change(ctx.guild.id, channel.id, faction_name)
        except Exception as e:
            await ctx.send(f'❌ Error: {e}')

    @factions_group.command(name='location')
    @commands.has_permissions(administrator=True)
    async def factions_location(self, ctx: commands.Context, channel_arg: str = None):
        channel = self._resolve_channel(ctx, channel_arg)
        if channel is None:
            await ctx.send('❌ Canal no encontrado.')
            return

        current = {}
        if supabase:
            res = supabase.table('channel_locations') \
                .select('*').eq('guild_id', str(ctx.guild.id)) \
                .eq('channel_id', str(channel.id)).maybe_single().execute()
            if res.data:
                current = res.data

        view = LocationButton(ctx.guild.id, channel.id, current)
        await ctx.send(f'📍 Haz clic para editar la ubicación de {channel.mention}:', view=view)

    @factions_group.command(name='modifiers')
    @commands.has_permissions(administrator=True)
    async def factions_modifiers(self, ctx: commands.Context, channel_arg: str = None):
        channel = self._resolve_channel(ctx, channel_arg)
        if channel is None:
            await ctx.send('❌ Canal no encontrado.')
            return

        faction_names = []
        current_mods = {}
        if supabase:
            fac_res = supabase.table('factions').select('name').eq('guild_id', str(ctx.guild.id)).execute()
            faction_names = [r['name'] for r in (fac_res.data or [])]
            mod_res = supabase.table('faction_modifiers').select('*').eq('guild_id', str(ctx.guild.id)) \
                .eq('channel_id', str(channel.id)).execute()
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
    # Lifecycle
    # -------------------------------------------------------------------
    @commands.Cog.listener()
    async def on_ready(self):
        if not self.weekly_faction_modifiers.is_running():
            self.weekly_faction_modifiers.start()