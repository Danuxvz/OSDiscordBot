import random
import re
import discord
from discord.ext import commands
from .config import supabase

class Tables(commands.Cog):
    """Dynamic table system. Each server can have its own tables, or use global ones."""

    def __init__(self, bot):
        self.bot = bot

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------
    async def _get_table_entries(self, table_name: str, guild_id: str = None) -> list[dict]:
        """Return all entries for a table. Tries guild-specific first, then global."""
        if not supabase:
            return []
        # First try guild-specific
        if guild_id:
            res = supabase.table("custom_tables") \
                .select("id, description, entry_order") \
                .eq("table_name", table_name) \
                .eq("guild_id", guild_id) \
                .order("entry_order") \
                .execute()
            if res and res.data:
                return res.data
        # Fallback: global entries (guild_id IS NULL)
        res = supabase.table("custom_tables") \
            .select("id, description, entry_order") \
            .eq("table_name", table_name) \
            .is_("guild_id", "null") \
            .order("entry_order") \
            .execute()
        return res.data if res else []

    async def _get_all_table_names(self, guild_id: str = None) -> list[str]:
        """Return distinct table names available to this guild."""
        if not supabase:
            return []
        names = set()
        # Global
        res = supabase.table("custom_tables") \
            .select("table_name") \
            .is_("guild_id", "null") \
            .execute()
        if res and res.data:
            for r in res.data:
                names.add(r["table_name"])
        # Guild-specific
        if guild_id:
            res = supabase.table("custom_tables") \
                .select("table_name") \
                .eq("guild_id", guild_id) \
                .execute()
            if res and res.data:
                for r in res.data:
                    names.add(r["table_name"])
        return sorted(names)

    async def _validate_table_name(self, table_name: str) -> bool:
        """Check table_name contains only letters, numbers, underscores."""
        return bool(re.match(r"^[A-Za-z0-9_]+$", table_name))

    # -----------------------------------------------------------------
    # >table (group) – without subcommand it rolls
    # -----------------------------------------------------------------
    @commands.group(name="table", aliases=["tbl"], invoke_without_command=True)
    async def table_group(self, ctx: commands.Context, *, table_name: str = None):
        """Roll a random entry from a table, or list all tables if no name given."""
        if table_name is None:
            # List available tables
            names = await self._get_all_table_names(str(ctx.guild.id))
            if not names:
                await ctx.send("ℹ️ No tables available in this server.")
                return
            desc = "\n".join(f"• `{n}`" for n in names)
            embed = discord.Embed(title="📋 Available Tables", description=desc, color=discord.Color.blurple())
            await ctx.send(embed=embed)
            return

        table_name = table_name.strip().lower()
        entries = await self._get_table_entries(table_name, str(ctx.guild.id))
        if not entries:
            await ctx.send(f"❌ Table `{table_name}` not found.")
            return

        chosen = random.choice(entries)
        await ctx.send(f"🎲 **{table_name.title()} #{chosen['entry_order']}:** {chosen['description']}")

    # -----------------------------------------------------------------
    # >table create <name>  (admin)
    # -----------------------------------------------------------------
    @table_group.command(name="create")
    @commands.has_permissions(administrator=True)
    async def table_create(self, ctx: commands.Context, name: str):
        """Create a new table (admin only)."""
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        name = name.strip().lower()
        if not await self._validate_table_name(name):
            await ctx.send("❌ Table name can only contain letters, numbers and underscores.")
            return

        # Check if table already exists (global or guild)
        existing = await self._get_table_entries(name, str(ctx.guild.id))
        if existing:
            await ctx.send(f"❌ Table `{name}` already exists in this server.")
            return

        # Insert a placeholder entry so the table exists (we'll add actual entries with >table add)
        try:
            supabase.table("custom_tables").insert({
                "guild_id": str(ctx.guild.id),
                "table_name": name,
                "description": "Placeholder – table created",
                "entry_order": 0
            }).execute()
            await ctx.send(f"✅ Table `{name}` created. Use `>table add {name} <description>` to add entries.")
        except Exception as e:
            await ctx.send(f"❌ Error creating table: {e}")

    # -----------------------------------------------------------------
    # >table add <name> <description>  (admin)
    # -----------------------------------------------------------------
    @table_group.command(name="add")
    @commands.has_permissions(administrator=True)
    async def table_add(self, ctx: commands.Context, name: str, *, description: str):
        """Add an entry to a table (admin only)."""
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        name = name.strip().lower()
        desc = description.strip()
        if not desc:
            await ctx.send("❌ Please provide a description.")
            return

        # Find the next entry_order
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

    # -----------------------------------------------------------------
    # >table remove <name> <entry_order>  (admin)
    # -----------------------------------------------------------------
    @table_group.command(name="remove")
    @commands.has_permissions(administrator=True)
    async def table_remove(self, ctx: commands.Context, name: str, entry_order: int):
        """Remove an entry from a table by its entry number (admin only)."""
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        name = name.strip().lower()

        # Find and delete the exact entry
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

    # -----------------------------------------------------------------
    # >table show <name>  (any)
    # -----------------------------------------------------------------
    @table_group.command(name="show", aliases=["list"])
    async def table_show(self, ctx: commands.Context, *, name: str):
        """Display all entries of a table."""
        name = name.strip().lower()
        entries = await self._get_table_entries(name, str(ctx.guild.id))
        if not entries:
            await ctx.send(f"❌ Table `{name}` not found.")
            return

        desc = "\n".join(
            f"**#{e['entry_order']}** – {e['description']}" for e in entries
        )
        embed = discord.Embed(
            title=f"📜 {name.title()} Table",
            description=desc[:4096],
            color=discord.Color.dark_purple()
        )
        await ctx.send(embed=embed)

    # -----------------------------------------------------------------
    # >roll <table_name>  (alternative to >table <name>)
    # -----------------------------------------------------------------
    @commands.command(name="roll", aliases=["r"])
    async def roll_command(self, ctx: commands.Context, *, table_name: str):
        """Roll a random entry from a table."""
        table_name = table_name.strip().lower()
        entries = await self._get_table_entries(table_name, str(ctx.guild.id))
        if not entries:
            await ctx.send(f"❌ Table `{table_name}` not found.")
            return
        chosen = random.choice(entries)
        await ctx.send(f"🎲 **{table_name.title()} #{chosen['entry_order']}:** {chosen['description']}")

    # -----------------------------------------------------------------
    # >ritual  (backwards compatibility)
    # -----------------------------------------------------------------
    @commands.command(name="ritual", aliases=["rit"])
    async def ritual_roll(self, ctx: commands.Context):
        """Roll a random ritual effect."""
        entries = await self._get_table_entries("ritual", str(ctx.guild.id))
        if not entries:
            await ctx.send("❌ The ritual table is empty.")
            return
        chosen = random.choice(entries)
        await ctx.send(f"🎲 **Ritual #{chosen['entry_order']}:** {chosen['description']}")

    # -----------------------------------------------------------------
    # >ritual show  (backwards compatibility)
    # -----------------------------------------------------------------
    @commands.command(name="ritual_show", aliases=["rit_show", "rituals"])
    async def ritual_show_legacy(self, ctx: commands.Context):
        """Display all ritual effects."""
        await self.table_show(ctx, name="ritual")