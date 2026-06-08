import random
import re
import discord
from discord.ext import commands
from .config import supabase

class Rituals(commands.Cog):
    """Ritual table commands."""

    def __init__(self, bot):
        self.bot = bot

    # -----------------------------------------------------------------
    # Helper: fetch all options (ordered by id)
    # -----------------------------------------------------------------
    async def _get_all_options(self) -> list[dict]:
        if not supabase:
            return []
        res = supabase.table("ritual_options") \
            .select("id, description") \
            .order("id") \
            .execute()
        return res.data if res else []

    # -----------------------------------------------------------------
    # >ritual
    # -----------------------------------------------------------------
    @commands.group(name='ritual', aliases=['rit'], invoke_without_command=True)
    async def ritual_group(self, ctx: commands.Context):
        """Roll a random ritual effect from the table."""
        options = await self._get_all_options()
        if not options:
            await ctx.send("❌ The ritual table is empty.")
            return

        chosen = random.choice(options)
        await ctx.send(f"🎲 **Ritual #{chosen['id']}:** {chosen['description']}")

    # -----------------------------------------------------------------
    # >ritual show
    # -----------------------------------------------------------------
    @ritual_group.command(name='show', aliases=['list', 'all'])
    async def ritual_show(self, ctx: commands.Context):
        """Display all ritual effects."""
        options = await self._get_all_options()
        if not options:
            await ctx.send("❌ The ritual table is empty.")
            return

        description = "\n".join(
            f"**#{opt['id']}** – {opt['description']}" for opt in options
        )
        # Discord message length limit ~2000 chars – if too long, paginate later
        embed = discord.Embed(
            title="📜 Ritual Table",
            description=description[:4096],  # embed description limit
            color=discord.Color.dark_purple()
        )
        await ctx.send(embed=embed)

    # -----------------------------------------------------------------
    # >ritual add <description>  (admin only)
    # -----------------------------------------------------------------
    @ritual_group.command(name='add')
    @commands.has_permissions(administrator=True)
    async def ritual_add(self, ctx: commands.Context, *, description: str):
        """Add a new ritual effect (admin only)."""
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        desc = description.strip()
        if not desc:
            await ctx.send("❌ You must provide a description.")
            return

        try:
            supabase.table("ritual_options").insert({
                "description": desc
            }).execute()
            await ctx.send(f"✅ Ritual effect added:\n> {desc}")
        except Exception as e:
            await ctx.send(f"❌ Failed to add ritual: {e}")

    # -----------------------------------------------------------------
    # >ritual remove <number>  (admin only)
    # -----------------------------------------------------------------
    @ritual_group.command(name='remove')
    @commands.has_permissions(administrator=True)
    async def ritual_remove(self, ctx: commands.Context, number: str):
        """Remove a ritual effect by its number (admin only)."""
        if not supabase:
            await ctx.send("❌ Supabase not configured.")
            return

        try:
            idx = int(number.strip())
        except ValueError:
            await ctx.send("❌ Please provide a valid number (e.g., `>ritual remove 5`).")
            return

        # Fetch the option with that id
        option = supabase.table("ritual_options") \
            .select("id, description") \
            .eq("id", idx) \
            .maybe_single() \
            .execute()
        if not option or not option.data:
            await ctx.send(f"❌ No ritual effect with number {idx}.")
            return

        try:
            supabase.table("ritual_options") \
                .delete() \
                .eq("id", idx) \
                .execute()
            await ctx.send(f"🗑 Removed ritual effect #{idx}: {option.data['description']}")
        except Exception as e:
            await ctx.send(f"❌ Failed to remove ritual: {e}")