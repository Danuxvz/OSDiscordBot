import random
import re
import discord
from discord.ext import commands
from .config import supabase

class TableCommand(commands.Command):
    """Dynamic command for a table. Handles >name (roll) and >name show."""
    def __init__(self, table_name: str, cog):
        self.table_name = table_name
        self.cog = cog
        super().__init__(
            func=self._handler,
            name=table_name,
            aliases=[],
            help=f"Roll on the {table_name} table",
            cog=cog,
        )

    async def _handler(self, ctx: commands.Context, sub: str = None):
        if sub and sub.lower() == "show":
            return await self.cog.table_show(ctx, name=self.table_name)
        # Default: roll
        entries = await self.cog._get_table_entries(self.table_name, str(ctx.guild.id))
        if not entries:
            await ctx.send(f"❌ The `{self.table_name}` table is empty.")
            return
        chosen = random.choice(entries)
        await ctx.send(f"🎲 **{self.table_name.title()} #{chosen['entry_order']}:** {chosen['description']}")

class Tables(commands.Cog):
    """Dynamic table system with direct commands per table."""

    def __init__(self, bot):
        self.bot = bot
        self._table_commands = {}

    @commands.Cog.listener()
    async def on_ready(self):
        await self._load_table_commands()

    async def _load_table_commands(self):
        """Register commands for all existing tables."""
        if not supabase:
            return
        # Get distinct table names (global only? We'll use guild-specific later but for simplicity, global for now)
        res = supabase.table("custom_tables") \
            .select("table_name") \
            .is_("guild_id", "null") \
            .execute()
        if not res or not res.data:
            return
        names = set(r["table_name"] for r in res.data)
        for name in names:
            if name not in self._table_commands:
                await self._add_table_command(name)

    async def _add_table_command(self, table_name: str):
        """Add a new command for the given table."""
        if table_name in self._table_commands:
            return
        # Remove any existing command with that name to avoid duplicates
        self.bot.remove_command(table_name)
        cmd = TableCommand(table_name, self)
        self.bot.add_command(cmd)
        self._table_commands[table_name] = cmd

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------
    async def _get_table_entries(self, table_name: str, guild_id: str = None) -> list[dict]:
        if not supabase:
            return []
        if guild_id:
            res = supabase.table("custom_tables") \
                .select("id, description, entry_order") \
                .eq("table_name", table_name) \
                .eq("guild_id", guild_id) \
                .order("entry_order") \
                .execute()
            if res and res.data:
                return res.data
        res = supabase.table("custom_tables") \
            .select("id, description, entry_order") \
            .eq("table_name", table_name) \
            .is_("guild_id", "null") \
            .order("entry_order") \
            .execute()
        return res.data if res else []

    async def _get_all_table_names(self, guild_id: str = None) -> list[str]:
        names = set()
        res = supabase.table("custom_tables") \
            .select("table_name") \
            .is_("guild_id", "null") \
            .execute()
        if res and res.data:
            for r in res.data:
                names.add(r["table_name"])
        if guild_id:
            res = supabase.table("custom_tables") \
                .select("table_name") \
                .eq("guild_id", guild_id) \
                .execute()
            if res and res.data:
                for r in res.data:
                    names.add(r["table_name"])
        return sorted(names)

    @staticmethod
    def _validate_table_name(name: str) -> bool:
        return bool(re.match(r"^[A-Za-z0-9_]+$", name))

    # -----------------------------------------------------------------
    # >table (group) – management
    # -----------------------------------------------------------------
    @commands.group(name="table", aliases=["tbl"], invoke_without_command=True)
    async def table_group(self, ctx: commands.Context):
        """List all available tables."""
        names = await self._get_all_table_names(str(ctx.guild.id))
        if not names:
            await ctx.send("No tables available in this server.")
            return
        desc = "\n".join(f"• `{n}`" for n in names)
        embed = discord.Embed(title="📋 Available Tables", description=desc, color=discord.Color.blurple())
        await ctx.send(embed=embed)

    @table_group.command(name="create")
    @commands.has_permissions(administrator=True)
    async def table_create(self, ctx: commands.Context, name: str):
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return
        name = name.strip().lower()
        if not self._validate_table_name(name):
            await ctx.send("❌ Table name can only contain letters, numbers and underscores.")
            return
        # Check for conflicts with existing bot commands
        if self.bot.get_command(name):
            await ctx.send(f"❌ A command named `{name}` already exists.")
            return
        existing = await self._get_table_entries(name, str(ctx.guild.id))
        if existing:
            await ctx.send(f"❌ Table `{name}` already exists.")
            return
        try:
            supabase.table("custom_tables").insert({
                "guild_id": str(ctx.guild.id),
                "table_name": name,
                "description": "Placeholder – table created",
                "entry_order": 0
            }).execute()
            # Add the command immediately
            await self._add_table_command(name)
            await ctx.send(f"✅ Table `{name}` created. Use `>{name} show` or `>table add {name} <desc>`.")
        except Exception as e:
            await ctx.send(f"❌ Error creating table: {e}")

    @table_group.command(name="add")
    @commands.has_permissions(administrator=True)
    async def table_add(self, ctx: commands.Context, name: str, *, description: str):
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return
        name = name.strip().lower()
        desc = description.strip()
        if not desc:
            await ctx.send("❌ Please provide a description.")
            return
        entries = await self._get_table_entries(name, str(ctx.guild.id))
        next_order = max((e["entry_order"] for e in entries), default=0) + 1
        try:
            supabase.table("custom_tables").insert({
                "guild_id": str(ctx.guild.id),
                "table_name": name,
                "description": desc,
                "entry_order": next_order
            }).execute()
            await ctx.send(f"✅ Added entry #{next_order} to table `{name}`.")
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")

    @table_group.command(name="remove")
    @commands.has_permissions(administrator=True)
    async def table_remove(self, ctx: commands.Context, name: str, entry_order: int):
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return
        name = name.strip().lower()
        try:
            supabase.table("custom_tables") \
                .delete() \
                .eq("guild_id", str(ctx.guild.id)) \
                .eq("table_name", name) \
                .eq("entry_order", entry_order) \
                .execute()
            await ctx.send(f"✅ Removed entry #{entry_order} from table `{name}`.")
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")

    @table_group.command(name="show", aliases=["list"])
    async def table_show(self, ctx: commands.Context, *, name: str):
        """Display all entries of a table."""
        name = name.strip().lower()
        entries = await self._get_table_entries(name, str(ctx.guild.id))
        if not entries:
            await ctx.send(f"❌ Table `{name}` not found.")
            return
        desc = "\n".join(f"**#{e['entry_order']}** – {e['description']}" for e in entries)
        embed = discord.Embed(title=f"{name.title()} Table", description=desc[:4096], color=discord.Color.dark_purple())
        await ctx.send(embed=embed)

    # -----------------------------------------------------------------
    # Alternative >roll command (still works)
    # -----------------------------------------------------------------
    @commands.command(name="roll", aliases=["r"])
    async def roll_command(self, ctx: commands.Context, *, table_name: str):
        entries = await self._get_table_entries(table_name.strip().lower(), str(ctx.guild.id))
        if not entries:
            await ctx.send(f"❌ Table `{table_name}` not found.")
            return
        chosen = random.choice(entries)
        await ctx.send(f"🎲 *{table_name.title()} #{chosen['entry_order']}:* {chosen['description']}*")