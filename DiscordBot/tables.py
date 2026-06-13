import random
import re
import traceback
import discord
from discord.ext import commands
from .config import supabase

class TableView(discord.ui.View):
    """Pagination view for table entries."""
    def __init__(self, table_name: str, entries: list[dict], timeout=120):
        super().__init__(timeout=timeout)
        self.table_name = table_name
        self.entries = entries
        self.page = 0
        self.items_per_page = 15

    def get_page_data(self):
        """Return (embed) for the current page."""
        start = self.page * self.items_per_page
        end = start + self.items_per_page
        page_entries = self.entries[start:end]

        desc = "\n".join(f"**#{e['entry_order']}** – {e['description']}" for e in page_entries)
        embed = discord.Embed(
            title=f"{self.table_name.title()} Table",
            description=desc,
            color=discord.Color.dark_purple()
        )
        embed.set_footer(text=f"Page {self.page + 1}/{(len(self.entries)-1)//self.items_per_page + 1}")
        return embed

    async def update_message(self, interaction):
        embed = self.get_page_data()
        # Disable buttons if on first/last page
        self.children[0].disabled = (self.page == 0)
        self.children[1].disabled = (self.page >= (len(self.entries)-1) // self.items_per_page)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.primary)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
            await self.update_message(interaction)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if (self.page + 1) * self.items_per_page < len(self.entries):
            self.page += 1
            await self.update_message(interaction)

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True


class Tables(commands.Cog):
    """Dynamic table system with direct commands per table."""

    def __init__(self, bot):
        self.bot = bot
        self._table_commands = {}

    @commands.Cog.listener()
    async def on_ready(self):
        await self._load_table_commands()

    async def _load_table_commands(self):
        if not supabase:
            return
        try:
            names = await self._get_all_table_names()
            for name in names:
                await self._add_table_command(name)
            print(f"[TABLES] Loaded {len(names)} table commands.")
        except Exception as e:
            print(f"[TABLES] Error loading tables: {e}")
            traceback.print_exc()

    async def reload_tables(self):
        for name in list(self._table_commands.keys()):
            try:
                self.bot.remove_command(name)
            except Exception:
                pass
        self._table_commands.clear()
        await self._load_table_commands()
        print("[TABLES] Reloaded all table commands.")

    async def _add_table_command(self, table_name: str):
        table_name = table_name.strip().lower()
        if not table_name:
            return
        if self.bot.get_command(table_name):
            return

        async def table_handler(ctx: commands.Context, *, args: str = ""):
            args = args.strip().lower()
            if args == "show":
                return await self.table_show(ctx, name=table_name)
            entries = await self._get_table_entries(table_name, str(ctx.guild.id))
            if not entries:
                await ctx.send(f"❌ The `{table_name}` table is empty.")
                return
            chosen = random.choice(entries)
            await ctx.send(f"- **{table_name.title()} #{chosen['entry_order']}:** – {chosen['description']}")

        cmd = commands.Command(table_handler, name=table_name, help=f"Roll on the {table_name} table")
        try:
            self.bot.remove_command(table_name)
        except Exception:
            pass
        self.bot.add_command(cmd)
        self._table_commands[table_name] = cmd

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------
    async def _get_table_entries(self, table_name: str, guild_id: str = None) -> list[dict]:
        if not supabase:
            return []
        table_name = table_name.strip().lower()
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
        if supabase:
            res = supabase.table("custom_tables") \
                .select("table_name") \
                .is_("guild_id", "null") \
                .execute()
            if res and res.data:
                for r in res.data:
                    names.add(r["table_name"].strip().lower())
            if guild_id:
                res = supabase.table("custom_tables") \
                    .select("table_name") \
                    .eq("guild_id", guild_id) \
                    .execute()
                if res and res.data:
                    for r in res.data:
                        names.add(r["table_name"].strip().lower())
        return sorted(names)

    @staticmethod
    def _validate_table_name(name: str) -> bool:
        return bool(re.match(r"^[A-Za-z0-9_]+$", name))

    # -----------------------------------------------------------------
    # >table (group) – management
    # -----------------------------------------------------------------
    @commands.group(name="table", aliases=["tbl"], invoke_without_command=True)
    async def table_group(self, ctx: commands.Context):
        names = await self._get_all_table_names(str(ctx.guild.id))
        if not names:
            await ctx.send("ℹ️ No tables available in this server.")
            return
        desc = "\n".join(f"• `{n}`" for n in names)
        embed = discord.Embed(title="Available Tables", description=desc, color=discord.Color.blurple())
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
            await self._add_table_command(name)
            await ctx.send(f"✅ Table `{name}` created. Use `>{name}` to roll or `>{name} show` to view.")
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
        """Display all entries of a table (paginated)."""
        name = name.strip().lower()
        entries = await self._get_table_entries(name, str(ctx.guild.id))
        if not entries:
            await ctx.send(f"❌ Table `{name}` not found.")
            return
        view = TableView(name, entries)
        embed = view.get_page_data()
        await ctx.send(embed=embed, view=view)

    @commands.command(name="roll", aliases=["r"])
    async def roll_command(self, ctx: commands.Context, *, table_name: str):
        entries = await self._get_table_entries(table_name.strip().lower(), str(ctx.guild.id))
        if not entries:
            await ctx.send(f"❌ Table `{table_name}` not found.")
            return
        chosen = random.choice(entries)
        await ctx.send(f"- **{table_name.title()} #{chosen['entry_order']}** – {chosen['description']}")