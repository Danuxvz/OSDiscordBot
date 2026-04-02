import asyncio
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
import os

# Absolute imports using the package name
from DiscordBot.config import supabase, load_config_from_db, config_sync_loop, pull_updates_from_db
from DiscordBot.commands import BotCommands
from DiscordBot.scanning import check_weekly_thread, scan_guild
from DiscordBot.items import refresh_items_table

load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.message_content = True

def get_prefix(bot, message):
    if message.guild is None:
        return ">"
    # Inside function we also need absolute import
    from DiscordBot.config import get_guild_cfg
    cfg = get_guild_cfg(message.guild.id)
    return cfg.get("prefix", ">")

bot = commands.Bot(command_prefix=get_prefix, intents=intents, case_insensitive=True, help_command=None)

@tasks.loop(minutes=60)
async def scan_busquedas_thread():
    from DiscordBot.config import config_cache, get_guild_cfg
    from DiscordBot.utils import get_local_now
    now = get_local_now()
    for gid in list(config_cache.keys()):
        try:
            guild_id = int(gid)
        except:
            continue
        cfg = get_guild_cfg(guild_id)
        scan_hour = cfg.get("scan_hour", 17)
        if scan_hour is None:
            continue
        if now.hour == int(scan_hour):
            await scan_guild(bot, guild_id, force=False)

@tasks.loop(minutes=60)
async def check_weekly_thread_task():
    await check_weekly_thread(bot)

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}!")
    await load_config_from_db()
    await pull_updates_from_db()
    # Start background tasks
    asyncio.create_task(config_sync_loop())
    check_weekly_thread_task.start()
    scan_busquedas_thread.start()
    await check_weekly_thread_task()  # run immediately

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply("❌ No tienes permisos para usar este comando.", mention_author=False)
    else:
        raise error

async def main():
    async with bot:
        await bot.add_cog(BotCommands(bot))
        await bot.start(DISCORD_TOKEN)

if __name__ == "__main__":
    asyncio.run(main())


# python -m DiscordBot.main