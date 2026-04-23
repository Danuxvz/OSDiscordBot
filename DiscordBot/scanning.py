import os
import json
import random
import asyncio
import discord
import re
from datetime import datetime, timezone
from .config import supabase, get_guild_cfg, set_config
from .utils import get_local_now, get_current_week_range, get_current_week_start_str, get_weekly_log_path, utc_now_iso
from .items import load_items_table
from .routes import match_route
from .parsing import parse_busqueda_message
from .services.supabase_service import upload_characters_csv_to_supabase

scan_locks = {}

def get_lock(guild_id):
    if guild_id not in scan_locks:
        scan_locks[guild_id] = asyncio.Lock()
    return scan_locks[guild_id]

def roll_tier():
    r = random.randint(1, 20)
    if 1 <= r <= 14:
        return "E", r
    if 15 <= r <= 19:
        return "D", r
    return "C", r

async def wait_for_rpforge_file(bot, channel, timeout=120):
    def check(msg):
        return msg.channel == channel and msg.attachments
    try:
        msg = await bot.wait_for("message", timeout=timeout, check=check)
        return msg.attachments[0]
    except asyncio.TimeoutError:
        return None

async def deliver_item(bot, ops_channel, item_id, target_code, message_id, route, timeout=120):
    """
    Send rp!giveitem command and wait for RPForge confirmation.
    Returns a dict with keys: success, transaction_id, error_message, raw_response.
    Logs to Supabase table 'item_deliveries'.
    """
    cmd = f"rp!giveitem {item_id}x1 {target_code}"
    try:
        await ops_channel.send(cmd)
    except Exception as e:
        print(f"[DELIVERY] Failed to send command: {e}")
        return {"success": False, "error_message": str(e), "transaction_id": None}

    # Wait for a response from RPForge (a bot) that contains the transaction details.
    def check(msg):
        if msg.channel != ops_channel:
            return False
        if not msg.author.bot:
            return False
        # Only accept messages from RPForge Premium (case-insensitive name check)
        if "rpforge" not in msg.author.name.lower():
            return False

        # Check content
        if "Transaction Record" in msg.content:
            return True
        # Check embeds
        if msg.embeds:
            for embed in msg.embeds:
                # Check embed title, description, and fields
                if embed.title and "Transaction Record" in embed.title:
                    return True
                if embed.description and "Transaction Record" in embed.description:
                    return True
                for field in embed.fields:
                    if "Transaction Record" in field.name or "Transaction Record" in field.value:
                        return True
        return False

    try:
        response = await bot.wait_for("message", timeout=timeout, check=check)
    except asyncio.TimeoutError:
        return {"success": False, "error_message": "Timeout waiting for RPForge response", "transaction_id": None}

    # Build a combined text from all parts of the response
    combined_parts = [response.content or ""]
    for embed in response.embeds:
        if embed.title:
            combined_parts.append(embed.title)
        if embed.description:
            combined_parts.append(embed.description)
        for field in embed.fields:
            combined_parts.append(f"{field.name}: {field.value}")
        if embed.footer and embed.footer.text:
            combined_parts.append(embed.footer.text)

    combined_text = "\n".join(part for part in combined_parts if part)

    # Determine success and extract transaction ID
    success = False
    transaction_id = None
    error_msg = None

    # Success is indicated by "Successfully gave items" anywhere in the combined text
    if "Successfully gave items" in combined_text:
        success = True
        # Extract transaction ID from pattern "Transaction Record ARPA_..."
        match = re.search(r"Transaction Record\s+(\S+)", combined_text)
        if match:
            transaction_id = match.group(1)
    elif "failed" in combined_text.lower() or "error" in combined_text.lower():
        error_msg = combined_text[:200]
        success = False
    else:
        error_msg = f"Response did not contain success or failure markers"
        success = False
        # Debug: print the raw combined text for inspection
        print(f"[DELIVERY] Unrecognized response from RPForge:\n{combined_text[:500]}")

    # Log to Supabase
    if supabase:
        try:
            supabase.table("item_deliveries").insert({
                "guild_id": str(ops_channel.guild.id),
                "message_id": str(message_id),
                "target_code": target_code,
                "item_id": item_id,
                "route": route,
                "command_sent": cmd,
                "transaction_id": transaction_id,
                "success": success,
                "error_message": error_msg,
                "raw_response": combined_text[:1000],
                "created_at": utc_now_iso()
            }).execute()
        except Exception as e:
            print(f"[SUPABASE] Failed to log delivery: {e}")

    return {
        "success": success,
        "transaction_id": transaction_id,
        "error_message": error_msg,
        "raw_response": combined_text
    }

async def scan_guild(bot, guild_id, force=False):
    cfg = get_guild_cfg(guild_id)
    scan_hour = cfg.get("scan_hour", 17)
    now = get_local_now()
    if not force:
        if scan_hour is None:
            return
        if now.hour != int(scan_hour):
            return

    busq_ch_id = cfg.get("busquedas_channel")
    if not busq_ch_id:
        print(f"[SCAN] guild {guild_id} has no busquedas channel set.")
        return

    lock = get_lock(guild_id)
    if lock.locked():
        print(f"[SCAN] guild {guild_id} already scanning; skipping")
        return

    async with lock:
        channel = bot.get_channel(busq_ch_id)
        if not channel:
            print(f"[SCAN] cannot find channel {busq_ch_id} for guild {guild_id}")
            return

        start, end = get_current_week_range()
        week_start_str = start.date().isoformat()
        log_path = get_weekly_log_path(start, guild_id)

        # Load weekly log (from Supabase or file)
        weekly_log = None
        if supabase:
            try:
                res = supabase.table("weekly_logs") \
                    .select("data") \
                    .eq("guild_id", str(guild_id)) \
                    .eq("week_start", week_start_str) \
                    .maybe_single() \
                    .execute()
                if res.data and res.data.get("data"):
                    weekly_log = res.data["data"]
            except Exception as e:
                print("[SUPABASE] Failed to load weekly log:", e)
        if not weekly_log and os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8") as f:
                weekly_log = json.load(f)
        elif not weekly_log:
            weekly_log = {}

        thread_name = f"BUSQUEDAS {start.strftime('%d %b')} - {end.strftime('%d %b')}"
        threads = list(channel.threads)
        thread = discord.utils.get(threads, name=thread_name)
        if not thread:
            archived = await channel.archived_threads(limit=50).flatten()
            thread = discord.utils.get(archived, name=thread_name)
        if not thread:
            print(f"[SCAN] No thread for guild {guild_id}")
            return

        items_table = load_items_table()
        ops_channel_id = cfg.get("operations_channel")
        ops_channel = bot.get_channel(ops_channel_id) if ops_channel_id else None

        async for message in thread.history(limit=1000, oldest_first=True):
            if message.author.bot:
                continue
            parsed = parse_busqueda_message(message.content)
            if not parsed:
                continue
            user_id = str(message.author.id)
            codigo = parsed["codigo"]
            log_key = f"{user_id}:{codigo}"
            if log_key in weekly_log:
                continue

            # Validate routes
            mapped_routes = []
            invalid_routes = []
            for raw_route in parsed["rutas"]:
                matched = match_route(raw_route, items_table)
                if not matched:
                    invalid_routes.append(raw_route)
                else:
                    mapped_routes.append(matched)

            if not mapped_routes:
                try:
                    await message.reply("❌ Ninguna ruta válida encontrada. Pide ayuda a un admin.", mention_author=True)
                except Exception as e:
                    print("Failed to reply (no valid routes):", e)

                weekly_log[log_key] = {
                    "user_id": user_id,
                    "author": message.author.name,
                    "codigo": codigo,
                    "rutas": parsed["rutas"],
                    "results": [],
                    "invalid": invalid_routes,
                    "delivered": False,
                    "timestamp": get_local_now().isoformat()
                }
                with open(log_path, "w", encoding="utf-8") as f:
                    json.dump(weekly_log, f, indent=4, ensure_ascii=False)

                if supabase:
                    try:
                        supabase.table("processed_users").upsert({
                            "guild_id": str(guild_id),
                            "week_start": week_start_str,
                            "user_id": user_id,
                            "codigo": codigo,
                            "payload": {"routes": parsed["rutas"], "failed": True}
                        }).execute()
                    except Exception as e:
                        print("[SUPABASE] processed_users error:", e)
                continue

            # Roll items
            results = []
            failed = False
            for route in mapped_routes:
                tier, roll = roll_tier()
                tier_list = items_table.get(route, {}).get(tier, [])
                if not tier_list:
                    try:
                        await thread.send("Invalid Entry, pide ayuda a un admin")
                    except Exception:
                        pass
                    print(f"[SCAN] No items for route {route} tier {tier} (user {message.author})")
                    failed = True
                    break
                item = random.choice(tier_list)
                results.append({
                    "route": route, "tier": tier, "roll": roll,
                    "id": item.get("id"), "name": item.get("name"),
                    "image": item.get("id") + ".png"
                })

            if failed:
                weekly_log[log_key] = {
                    "user_id": user_id,
                    "author": message.author.name,
                    "codigo": codigo,
                    "rutas": parsed["rutas"],
                    "results": [],
                    "delivered": False,
                    "error": "Unrecognized route(s)",
                    "timestamp": get_local_now().isoformat()
                }
                with open(log_path, "w", encoding="utf-8") as f:
                    json.dump(weekly_log, f, indent=4, ensure_ascii=False)
                if supabase:
                    try:
                        supabase.table("processed_users").upsert({
                            "guild_id": str(guild_id),
                            "week_start": week_start_str,
                            "user_id": user_id,
                            "codigo": codigo,
                            "payload": {"routes": parsed["rutas"], "failed": True}
                        }).execute()
                    except Exception as e:
                        print("[SUPABASE] processed_users error:", e)
                continue

            # Build reply
            formatted_names = []
            for r in results:
                name = f"{r['name']}"
                if r['tier'] == "C":
                    formatted_names.append(f"✨**{name}**✨")
                else:
                    formatted_names.append(f"**{name}**")
            if len(formatted_names) == 1:
                descr = f"¡Felicidades! Has hecho un contrato con {formatted_names[0]}!"
            else:
                descr = f"¡Felicidades! Has hecho un contrato con {', '.join(formatted_names[:-1])} y {formatted_names[-1]}!"
            if invalid_routes:
                invalid_str = ", ".join([f"- {r}" for r in invalid_routes])
                descr += f"\n\nRuta inválida: **{invalid_str}**"

            files = []
            for r in results:
                tier_folder = f"RANK {r['tier']}"
                img_path = os.path.join("ENTES", tier_folder, r["id"] + ".png")
                if os.path.exists(img_path):
                    try:
                        files.append(discord.File(img_path))
                    except Exception as e:
                        print("Failed to attach image", img_path, e)
                else:
                    print("Image not found:", img_path)

            try:
                if files:
                    await message.reply(descr, files=files, mention_author=True)
                else:
                    await message.reply(descr, mention_author=True)
            except Exception as e:
                print("Failed to send reply:", e)

            # Deliver items with verification (skip C-tier, log as success automatically)
            delivery_results = []
            all_delivered = True
            for r in results:
                item_id = r.get("id")
                if not item_id:
                    continue
                route = r["route"]
                tier = r["tier"]

                if tier == "C":
                    # C-tier items are designed to fail; we log as success without actual delivery
                    delivery_results.append({
                        "item_id": item_id,
                        "route": route,
                        "success": True,
                        "transaction_id": None,
                        "note": "C-tier item – delivery skipped"
                    })
                    if supabase:
                        try:
                            supabase.table("item_deliveries").insert({
                                "guild_id": str(guild_id),
                                "message_id": str(message.id),
                                "target_code": codigo,
                                "item_id": item_id,
                                "route": route,
                                "command_sent": None,
                                "transaction_id": None,
                                "success": True,
                                "error_message": "C-tier item (skipped)",
                                "raw_response": None,
                                "created_at": utc_now_iso()
                            }).execute()
                        except Exception as e:
                            print(f"[SUPABASE] Failed to log C-tier delivery: {e}")
                    continue

                if ops_channel is None:
                    # No operations channel, cannot deliver
                    delivery_results.append({
                        "item_id": item_id,
                        "route": route,
                        "success": False,
                        "error": "Operations channel not set"
                    })
                    all_delivered = False
                    continue

                result = await deliver_item(
                    bot, ops_channel, item_id, codigo,
                    message.id, route, timeout=120
                )
                delivery_results.append({
                    "item_id": item_id,
                    "route": route,
                    "success": result["success"],
                    "transaction_id": result.get("transaction_id"),
                    "error": result.get("error_message")
                })
                if not result["success"]:
                    all_delivered = False
                    # Optionally notify user of failure
                    try:
                        await message.reply(
                            f"⚠️ Error al entregar {item_id}. Un administrador revisará.",
                            mention_author=True
                        )
                    except:
                        pass

            weekly_log[log_key] = {
                "user_id": user_id,
                "author": message.author.name,
                "codigo": codigo,
                "rutas": parsed["rutas"],
                "results": results,
                "deliveries": delivery_results,
                "delivered": all_delivered,
                "timestamp": get_local_now().isoformat()
            }
            with open(log_path, "w", encoding="utf-8") as f:
                json.dump(weekly_log, f, indent=4, ensure_ascii=False)
            if supabase:
                try:
                    supabase.table("processed_users").upsert({
                        "guild_id": str(guild_id),
                        "week_start": week_start_str,
                        "user_id": user_id,
                        "codigo": codigo,
                        "payload": {
                            "routes": parsed["rutas"],
                            "results": results,
                            "deliveries": delivery_results
                        }
                    }).execute()
                except Exception as e:
                    print("[SUPABASE] processed_users error:", e)

        # Final save
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(weekly_log, f, indent=4, ensure_ascii=False)
        if supabase:
            try:
                supabase.table("weekly_logs").upsert({
                    "guild_id": str(guild_id),
                    "week_start": week_start_str,
                    "data": weekly_log,
                    "updated_at": utc_now_iso()
                }).execute()
                print(f"[SCAN] Upserted weekly log for guild {guild_id} ({len(weekly_log)} entries)")
            except Exception as e:
                print("[SUPABASE] weekly_logs error:", e)

        # Request characters export
        if ops_channel_id:
            ops_channel = bot.get_channel(ops_channel_id)
            if ops_channel:
                try:
                    print(f"[RPFORGE] Requesting characters export for guild {guild_id}")
                    await ops_channel.send("rp!export characters")
                    attachment = await wait_for_rpforge_file(bot, ops_channel, timeout=500)
                    if attachment:
                        save_path = os.path.join("exports", f"characters_{guild_id}.csv")
                        await attachment.save(save_path)
                        print(f"[RPFORGE] Saved file to {save_path}")
                        await upload_characters_csv_to_supabase(save_path)
                    else:
                        print(f"[RPFORGE] No file received for guild {guild_id}")
                except Exception as e:
                    print("[RPFORGE] Error during export:", e)
        print(f"[SCAN] Finished scanning guild {guild_id}")

async def check_weekly_thread(bot):
    now = get_local_now()
    start, end = get_current_week_range()
    current_week_str = start.strftime("%Y%m%d")
    for guild in bot.guilds:
        guild_id = guild.id
        cfg = get_guild_cfg(guild_id)
        busq_ch_id = cfg.get("busquedas_channel")
        last_week = cfg.get("last_week")
        if not busq_ch_id:
            continue
        if last_week != current_week_str:
            channel = bot.get_channel(busq_ch_id)
            if not channel:
                print(f"[WEEK] cannot find channel {busq_ch_id} for guild {guild_id}")
                continue
            title = f"BUSQUEDAS {start.strftime('%d %b')} - {end.strftime('%d %b')}"
            thread = discord.utils.get(channel.threads, name=title)
            if not thread:
                try:
                    thread = await channel.create_thread(
                        name=title,
                        type=discord.ChannelType.public_thread
                    )
                    from .routes import VALID_ROUTES
                    chunked_routes = [VALID_ROUTES[i:i+6] for i in range(0, len(VALID_ROUTES), 6)]
                    route_lines = ["- " + ", ".join(chunk) for chunk in chunked_routes]
                    routes_text = "\n".join(route_lines)
                    message = (
                        "# Hilo para Búsquedas Semanales\n\n"
                        "Para hacer sus búsquedas deben dejar un mensaje en el formato:\n"
                        "```Código de Usuario: `H0XX`\n"
                        "Rutas a Visitar:\n"
                        "Ruta 1\nRuta 2```\n"
                        f"**Rutas disponibles esta semana:**\n{routes_text}\n\n"
                        "¡Buena Suerte con sus Búsquedas esta semana! 🍀"
                    )
                    await thread.send(message)
                    print(f"[WEEK] Created thread for guild {guild_id}: {title}")
                except Exception as e:
                    print("[WEEK] failed to create thread:", e)
            set_config(guild_id, "last_week", current_week_str)