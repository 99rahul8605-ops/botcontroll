import os
import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiohttp import web
from pymongo import MongoClient, UpdateOne
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter, Forbidden, BadRequest, NetworkError
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

MASTER_BOT_TOKEN = os.getenv("MASTER_BOT_TOKEN", "").strip()
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
DEST_MONGO_URI = os.getenv("DEST_MONGO_URI", "").strip()
DEST_DB = os.getenv("DEST_DB", "telegram_migration").strip()
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata").strip()
BROADCAST_TIME = os.getenv("BROADCAST_TIME", "10:00").strip()
BUTTON_TEXT = os.getenv("BUTTON_TEXT", "🚀 Open New Bot")
BROADCAST_TEXT = os.getenv(
    "BROADCAST_TEXT",
    "⚠️ This bot has been shut down.\n\n"
    "We have moved to our new bot.\n\n"
    "Please tap the button below to continue 👇",
)
PORT = int(os.getenv("PORT", "10000"))

(
    ADD_TOKEN,
    ADD_NEW_BOT,
    ADD_SOURCE_MONGO,
    ADD_SOURCE_DB,
    CHOOSE_COLLECTION,
    CHOOSE_UID_FIELD,
) = range(6)

mongo = None
db = None
running = {}
configs = {}


def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_USER_ID)


def panel():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ Add Bot", callback_data="add"),
            InlineKeyboardButton("📋 My Bots", callback_data="list"),
        ],
        [
            InlineKeyboardButton("📣 Broadcast All", callback_data="broadcast_all"),
            InlineKeyboardButton("🔄 Reload", callback_data="reload"),
        ],
    ])


def migrate_markup(username):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(BUTTON_TEXT, url=f"https://t.me/{username}")]
    ])


def chunk_buttons(items, prefix, cols=2):
    rows = []
    row = []
    for i, label in enumerate(items):
        row.append(InlineKeyboardButton(str(label)[:45], callback_data=f"{prefix}:{i}"))
        if len(row) >= cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def dotted_get(document, dotted_key):
    cur = document
    for part in dotted_key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def normalize_chat_id(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("-") and s[1:].isdigit():
            return int(s)
        if s.isdigit():
            return int(s)
    return None


def flatten_keys(doc, prefix="", depth=0, max_depth=2):
    keys = []
    if not isinstance(doc, dict) or depth > max_depth:
        return keys
    for key, value in doc.items():
        if key == "_id":
            continue
        full = f"{prefix}.{key}" if prefix else key
        keys.append(full)
        if isinstance(value, dict) and depth < max_depth:
            keys.extend(flatten_keys(value, full, depth + 1, max_depth))
    return keys


def rank_uid_fields(fields):
    preferred = [
        "userId", "user_id", "chat_id", "chatId",
        "telegram_id", "telegramId", "telegram_user_id",
        "user.id", "chat.id", "id",
    ]
    scores = {}
    for f in fields:
        low = f.lower()
        score = 0
        for idx, p in enumerate(preferred):
            if low == p.lower():
                score = 1000 - idx
                break
        if score == 0:
            if "user" in low and "id" in low:
                score = 700
            elif "chat" in low and "id" in low:
                score = 650
            elif low.endswith("id"):
                score = 300
        scores[f] = score
    return sorted(fields, key=lambda x: (-scores[x], x.lower()))


async def start_cmd(update, context):
    if not is_admin(update):
        return
    await update.message.reply_text(
        "🛠 Telegram Migration Master",
        reply_markup=panel(),
    )


async def add_start(update, context):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        return ConversationHandler.END

    context.user_data["cfg"] = {}
    await q.edit_message_text(
        "➕ Add Bot\n\nOld bot TOKEN bhejo.\n\n/cancel to stop"
    )
    return ADD_TOKEN


async def take_token(update, context):
    token = update.message.text.strip()
    try:
        app = Application.builder().token(token).build()
        await app.initialize()
        info = await app.bot.get_me()
        await app.shutdown()
    except Exception as e:
        await update.message.reply_text(
            f"❌ Token invalid / Telegram error:\n{e}\n\nToken dobara bhejo."
        )
        return ADD_TOKEN

    cfg = context.user_data["cfg"]
    cfg.update(
        token=token,
        bot_id=info.id,
        old_bot_username=info.username,
    )

    await update.message.reply_text(
        f"✅ Old bot: @{info.username}\n\n"
        "Ab NEW bot username bhejo.\nExample: MyNewBot"
    )
    return ADD_NEW_BOT


async def take_new_bot(update, context):
    context.user_data["cfg"]["new_bot"] = (
        update.message.text.strip().replace("@", "")
    )
    await update.message.reply_text(
        "Old/source MongoDB URL bhejo.\n\n"
        "Example:\n"
        "mongodb+srv://USER:PASSWORD@cluster.mongodb.net/"
    )
    return ADD_SOURCE_MONGO


async def take_source_mongo(update, context):
    uri = update.message.text.strip()

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        await asyncio.to_thread(client.admin.command, "ping")
        client.close()
    except Exception as e:
        await update.message.reply_text(
            f"❌ Mongo connection failed:\n{e}\n\nURL dobara bhejo."
        )
        return ADD_SOURCE_MONGO

    context.user_data["cfg"]["source_mongo_uri"] = uri

    await update.message.reply_text(
        "✅ Mongo connected.\n\n"
        "Ab source DATABASE name bhejo.\nExample: mybotdb"
    )
    return ADD_SOURCE_DB


async def take_source_db(update, context):
    db_name = update.message.text.strip()
    cfg = context.user_data["cfg"]
    cfg["source_db"] = db_name

    client = MongoClient(cfg["source_mongo_uri"], serverSelectionTimeoutMS=10000)
    try:
        collections = await asyncio.to_thread(
            client[db_name].list_collection_names
        )
    except Exception as e:
        client.close()
        await update.message.reply_text(
            f"❌ Database read failed:\n{e}\n\nDatabase name dobara bhejo."
        )
        return ADD_SOURCE_DB
    finally:
        try:
            client.close()
        except Exception:
            pass

    collections = sorted(collections)

    if not collections:
        await update.message.reply_text(
            "❌ Is database me koi collection nahi mili.\n"
            "Database name dobara bhejo."
        )
        return ADD_SOURCE_DB

    context.user_data["collection_choices"] = collections

    rows = chunk_buttons(collections, "pickcol", cols=2)
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="addcancel")])

    await update.message.reply_text(
        f"📂 Database: {db_name}\n"
        f"Collections found: {len(collections)}\n\n"
        "Collection choose karo:",
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return CHOOSE_COLLECTION


async def choose_collection(update, context):
    q = update.callback_query
    await q.answer()

    if not is_admin(update):
        return ConversationHandler.END

    try:
        idx = int(q.data.split(":", 1)[1])
        choices = context.user_data["collection_choices"]
        collection_name = choices[idx]
    except Exception:
        await q.edit_message_text("❌ Collection selection invalid. /start se retry karo.")
        return ConversationHandler.END

    cfg = context.user_data["cfg"]
    cfg["source_collection"] = collection_name

    client = MongoClient(cfg["source_mongo_uri"], serverSelectionTimeoutMS=10000)
    try:
        col = client[cfg["source_db"]][collection_name]

        # Inspect a handful of docs to discover possible UID fields.
        samples = await asyncio.to_thread(lambda: list(col.find({}).limit(20)))
        count = await asyncio.to_thread(col.estimated_document_count)

    except Exception as e:
        await q.edit_message_text(
            f"❌ Collection read failed:\n{e}",
            reply_markup=panel(),
        )
        client.close()
        return ConversationHandler.END
    finally:
        try:
            client.close()
        except Exception:
            pass

    fields = set()
    for doc in samples:
        fields.update(flatten_keys(doc))

    fields = rank_uid_fields(list(fields))

    if not fields:
        await q.edit_message_text(
            f"📂 Collection: {collection_name}\n"
            f"Documents: ~{count}\n\n"
            "❌ Sample documents me koi usable field nahi mili.",
            reply_markup=panel(),
        )
        return ConversationHandler.END

    # Keep Telegram keyboard manageable.
    fields = fields[:30]
    context.user_data["uid_field_choices"] = fields

    rows = chunk_buttons(fields, "pickuid", cols=2)
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="addcancel")])

    await q.edit_message_text(
        f"✅ Collection: {collection_name}\n"
        f"Approx documents: {count}\n\n"
        "Ab UID field choose karo.\n"
        "Likely fields upar rakhe gaye hain:",
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return CHOOSE_UID_FIELD


async def choose_uid_field(update, context):
    q = update.callback_query
    await q.answer()

    if not is_admin(update):
        return ConversationHandler.END

    try:
        idx = int(q.data.split(":", 1)[1])
        choices = context.user_data["uid_field_choices"]
        uid_field = choices[idx]
    except Exception:
        await q.edit_message_text("❌ UID field selection invalid.", reply_markup=panel())
        return ConversationHandler.END

    cfg = context.user_data["cfg"]
    cfg["uid_fields"] = [uid_field]
    cfg["enabled"] = True
    cfg["created_at"] = datetime.utcnow()
    cfg["updated_at"] = datetime.utcnow()

    db.bot_configs.update_one(
        {"bot_id": cfg["bot_id"]},
        {"$set": cfg},
        upsert=True,
    )

    await q.edit_message_text(
        f"⏳ Saving + importing...\n\n"
        f"DB: {cfg['source_db']}\n"
        f"Collection: {cfg['source_collection']}\n"
        f"UID field: {uid_field}"
    )

    try:
        await start_managed(cfg)
        saved = db.users.count_documents({"bot_id": cfg["bot_id"]})
        stats = db.bot_configs.find_one({"bot_id": cfg["bot_id"]}) or {}
        scanned = stats.get("last_import_scanned", 0)
        found = stats.get("last_import_found", 0)

        text = (
            f"✅ Bot added successfully\n\n"
            f"Old: @{cfg['old_bot_username']}\n"
            f"New: @{cfg['new_bot']}\n"
            f"DB: {cfg['source_db']}\n"
            f"Collection: {cfg['source_collection']}\n"
            f"UID field: {uid_field}\n\n"
            f"📊 Scanned: {scanned}\n"
            f"📥 IDs found: {found}\n"
            f"💾 Unique users saved: {saved}"
        )
    except Exception as e:
        log.exception("New bot startup failed")
        text = (
            "⚠️ Config save ho gayi, but bot start/import failed:\n\n"
            f"{e}"
        )

    for key in ("cfg", "collection_choices", "uid_field_choices"):
        context.user_data.pop(key, None)

    await q.edit_message_text(text, reply_markup=panel())
    return ConversationHandler.END


async def cancel_add(update, context):
    if update.callback_query:
        q = update.callback_query
        await q.answer()
        for key in ("cfg", "collection_choices", "uid_field_choices"):
            context.user_data.pop(key, None)
        await q.edit_message_text("❌ Add bot cancelled.", reply_markup=panel())
    else:
        for key in ("cfg", "collection_choices", "uid_field_choices"):
            context.user_data.pop(key, None)
        await update.message.reply_text("❌ Add bot cancelled.", reply_markup=panel())
    return ConversationHandler.END


async def import_users(cfg):
    source = MongoClient(
        cfg["source_mongo_uri"],
        serverSelectionTimeoutMS=15000,
    )
    try:
        await asyncio.to_thread(source.admin.command, "ping")
        col = source[cfg["source_db"]][cfg["source_collection"]]

        scanned = found = invalid = 0
        ops = []

        for doc in col.find({}):
            scanned += 1
            chat_id = None

            for field in cfg.get("uid_fields", []):
                candidate = normalize_chat_id(dotted_get(doc, field))
                if candidate is not None:
                    chat_id = candidate
                    break

            if chat_id is None:
                invalid += 1
                continue

            # Telegram groups/channels/supergroups use negative chat IDs.
            # This migration service is intentionally private-user-only.
            if chat_id <= 0:
                invalid += 1
                continue

            found += 1
            ops.append(
                UpdateOne(
                    {"bot_id": cfg["bot_id"], "chat_id": chat_id},
                    {
                        "$set": {
                            "bot_id": cfg["bot_id"],
                            "old_bot_username": cfg["old_bot_username"],
                            "new_bot_username": cfg["new_bot"],
                            "chat_id": chat_id,
                            "chat_type": "private",
                            "active": True,
                            "source_db": cfg["source_db"],
                            "source_collection": cfg["source_collection"],
                            "updated_at": datetime.utcnow(),
                        },
                        "$setOnInsert": {
                            "created_at": datetime.utcnow(),
                            "imported_from_old_db": True,
                        },
                    },
                    upsert=True,
                )
            )

            if len(ops) >= 1000:
                db.users.bulk_write(ops, ordered=False)
                ops = []

        if ops:
            db.users.bulk_write(ops, ordered=False)

        unique_saved = db.users.count_documents({"bot_id": cfg["bot_id"]})

        db.bot_configs.update_one(
            {"bot_id": cfg["bot_id"]},
            {"$set": {
                "last_import_at": datetime.utcnow(),
                "last_import_scanned": scanned,
                "last_import_found": found,
                "last_import_invalid": invalid,
                "last_import_unique_saved": unique_saved,
            }},
        )

        return scanned, found, invalid, unique_saved
    finally:
        source.close()


async def old_bot_handler(update, context):
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if not message or not chat:
        return

    # Migration responder is DM-only. Never reply/save from groups,
    # supergroups, or channels.
    if chat.type != "private":
        return

    cfg = configs.get(context.bot.id)
    if not cfg:
        return

    db.users.update_one(
        {"bot_id": context.bot.id, "chat_id": chat.id},
        {
            "$set": {
                "bot_id": context.bot.id,
                "old_bot_username": cfg["old_bot_username"],
                "new_bot_username": cfg["new_bot"],
                "chat_id": chat.id,
                "chat_type": "private",
                "telegram_user_id": user.id if user else None,
                "username": user.username if user else None,
                "first_name": user.first_name if user else None,
                "last_name": user.last_name if user else None,
                "active": True,
                "last_seen_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            },
            "$setOnInsert": {
                "created_at": datetime.utcnow(),
                "imported_from_old_db": False,
            },
        },
        upsert=True,
    )

    try:
        await message.reply_text(
            BROADCAST_TEXT,
            reply_markup=migrate_markup(cfg["new_bot"]),
        )
    except Exception:
        log.exception("Old bot reply failed")


async def start_managed(cfg):
    bot_id = int(cfg["bot_id"])
    if bot_id in running:
        return

    app = Application.builder().token(cfg["token"]).build()
    await app.initialize()
    await app.bot.delete_webhook(drop_pending_updates=False)
    info = await app.bot.get_me()

    cfg["bot_id"] = info.id
    cfg["old_bot_username"] = info.username
    configs[info.id] = cfg

    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.ALL, old_bot_handler))

    await import_users(cfg)

    await app.start()
    await app.updater.start_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
    )

    running[info.id] = app

    db.bot_configs.update_one(
        {"bot_id": info.id},
        {"$set": {
            "enabled": True,
            "old_bot_username": info.username,
            "updated_at": datetime.utcnow(),
        }},
        upsert=True,
    )


async def stop_managed(bot_id):
    app = running.pop(bot_id, None)
    configs.pop(bot_id, None)
    if not app:
        return

    if app.updater and app.updater.running:
        await app.updater.stop()
    if app.running:
        await app.stop()
    await app.shutdown()


async def send_one(app, chat_id, text, markup):
    while True:
        try:
            await app.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=markup,
            )
            return "sent"
        except RetryAfter as e:
            await asyncio.sleep(float(e.retry_after) + 1)
        except Forbidden:
            return "blocked"
        except BadRequest:
            return "failed"
        except NetworkError:
            await asyncio.sleep(2)
        except Exception:
            return "failed"


async def broadcast_bot(bot_id):
    app = running.get(bot_id)
    cfg = configs.get(bot_id)
    if not app or not cfg:
        return 0, 0, 0

    sent = blocked = failed = 0
    markup = migrate_markup(cfg["new_bot"])

    for row in db.users.find(
        {
            "bot_id": bot_id,
            "active": True,
            "chat_id": {"$gt": 0},
            "$or": [
                {"chat_type": "private"},
                {"chat_type": {"$exists": False}},
            ],
        },
        {"chat_id": 1},
    ):
        chat_id = row.get("chat_id")
        if chat_id is None or chat_id <= 0:
            continue

        status = await send_one(
            app,
            chat_id,
            BROADCAST_TEXT,
            markup,
        )

        if status == "sent":
            sent += 1
        elif status == "blocked":
            blocked += 1
            db.users.update_one(
                {"bot_id": bot_id, "chat_id": chat_id},
                {"$set": {"active": False}},
            )
        else:
            failed += 1

        db.users.update_one(
            {"bot_id": bot_id, "chat_id": chat_id},
            {"$set": {
                "last_broadcast_at": datetime.utcnow(),
                "last_broadcast_status": status,
            }},
        )

        await asyncio.sleep(0.05)

    return sent, blocked, failed


async def menu_callback(update, context):
    q = update.callback_query
    await q.answer()

    if not is_admin(update):
        return

    data = q.data

    if data == "panel":
        await q.edit_message_text(
            "🛠 Telegram Migration Master",
            reply_markup=panel(),
        )
        return

    if data == "list":
        bots = list(db.bot_configs.find({}).sort("created_at", 1))

        if not bots:
            await q.edit_message_text(
                "Abhi koi bot add nahi hai.",
                reply_markup=panel(),
            )
            return

        rows = []
        for item in bots:
            icon = "🟢" if item["bot_id"] in running else "🔴"
            rows.append([
                InlineKeyboardButton(
                    f"{icon} @{item.get('old_bot_username', item['bot_id'])}",
                    callback_data=f"bot:{item['bot_id']}",
                )
            ])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="panel")])

        await q.edit_message_text(
            "📋 Managed Bots",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "reload":
        started = 0
        for cfg in db.bot_configs.find({"enabled": True}):
            try:
                before = len(running)
                await start_managed(cfg)
                if len(running) > before:
                    started += 1
            except Exception:
                log.exception("Reload failed")

        await q.edit_message_text(
            f"✅ Reload complete\nNewly started: {started}",
            reply_markup=panel(),
        )
        return

    if data == "broadcast_all":
        sent = blocked = failed = 0
        for bot_id in list(running):
            a, b, c = await broadcast_bot(bot_id)
            sent += a
            blocked += b
            failed += c

        await q.edit_message_text(
            f"✅ Broadcast All complete\n\n"
            f"Sent: {sent}\nBlocked: {blocked}\nFailed: {failed}",
            reply_markup=panel(),
        )
        return

    if data.startswith("bot:"):
        bot_id = int(data.split(":", 1)[1])
        cfg = db.bot_configs.find_one({"bot_id": bot_id})
        if not cfg:
            await q.edit_message_text("Bot config nahi mili.", reply_markup=panel())
            return

        users = db.users.count_documents({"bot_id": bot_id})
        active = db.users.count_documents({"bot_id": bot_id, "active": True})

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📣 Broadcast", callback_data=f"bc:{bot_id}"),
                InlineKeyboardButton("📥 Re-import", callback_data=f"imp:{bot_id}"),
            ],
            [
                InlineKeyboardButton("🛑 Stop", callback_data=f"stop:{bot_id}"),
                InlineKeyboardButton("🗑 Remove", callback_data=f"rm:{bot_id}"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="list")],
        ])

        await q.edit_message_text(
            f"{'🟢 Running' if bot_id in running else '🔴 Stopped'}\n\n"
            f"Old: @{cfg.get('old_bot_username', '-')}\n"
            f"New: @{cfg.get('new_bot', '-')}\n"
            f"DB: {cfg.get('source_db', '-')}\n"
            f"Collection: {cfg.get('source_collection', '-')}\n"
            f"UID: {', '.join(cfg.get('uid_fields', []))}\n\n"
            f"Users: {users}\n"
            f"Active: {active}\n"
            f"Last import IDs: {cfg.get('last_import_found', '-')}",
            reply_markup=keyboard,
        )
        return

    action, bot_id_raw = data.split(":", 1)
    bot_id = int(bot_id_raw)
    cfg = db.bot_configs.find_one({"bot_id": bot_id})

    if action == "bc":
        result = await broadcast_bot(bot_id)
        await q.edit_message_text(
            f"✅ Broadcast complete\n\n"
            f"Sent: {result[0]}\nBlocked: {result[1]}\nFailed: {result[2]}",
            reply_markup=panel(),
        )

    elif action == "imp":
        if not cfg:
            await q.edit_message_text("Bot config nahi mili.", reply_markup=panel())
            return

        scanned, found, invalid, unique_saved = await import_users(cfg)
        await q.edit_message_text(
            f"✅ Import complete\n\n"
            f"Scanned: {scanned}\n"
            f"Valid IDs found: {found}\n"
            f"Invalid/no ID: {invalid}\n"
            f"Unique users saved: {unique_saved}",
            reply_markup=panel(),
        )

    elif action == "stop":
        await stop_managed(bot_id)
        db.bot_configs.update_one(
            {"bot_id": bot_id},
            {"$set": {"enabled": False}},
        )
        await q.edit_message_text("🛑 Bot stopped.", reply_markup=panel())

    elif action == "rm":
        await stop_managed(bot_id)
        db.bot_configs.delete_one({"bot_id": bot_id})
        await q.edit_message_text(
            "🗑 Bot config removed.\nImported users retained in destination DB.",
            reply_markup=panel(),
        )


def seconds_until_daily():
    tz = ZoneInfo(TIMEZONE)
    now = datetime.now(tz)
    hour, minute = map(int, BROADCAST_TIME.split(":"))
    target = now.replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0,
    )
    if target <= now:
        target += timedelta(days=1)
    return max(1, (target - now).total_seconds())


async def daily_loop():
    while True:
        await asyncio.sleep(seconds_until_daily())
        for bot_id in list(running):
            try:
                await broadcast_bot(bot_id)
            except Exception:
                log.exception("Daily broadcast failed bot_id=%s", bot_id)


async def start_web():
    async def health(request):
        return web.json_response({
            "status": "ok",
            "running_bots": len(running),
        })

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def main():
    global mongo, db

    if not MASTER_BOT_TOKEN:
        raise RuntimeError("MASTER_BOT_TOKEN missing")
    if not ADMIN_USER_ID:
        raise RuntimeError("ADMIN_USER_ID missing")
    if not DEST_MONGO_URI:
        raise RuntimeError("DEST_MONGO_URI missing")

    mongo = MongoClient(
        DEST_MONGO_URI,
        serverSelectionTimeoutMS=15000,
    )
    await asyncio.to_thread(mongo.admin.command, "ping")
    db = mongo[DEST_DB]

    db.users.create_index(
        [("bot_id", 1), ("chat_id", 1)],
        unique=True,
    )
    db.bot_configs.create_index("bot_id", unique=True)

    # Safety cleanup for older versions that may have imported group/channel IDs.
    db.users.update_many(
        {"chat_id": {"$lte": 0}},
        {"$set": {"active": False, "chat_type": "non_private"}},
    )

    await start_web()

    master = Application.builder().token(MASTER_BOT_TOKEN).build()

    add_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(add_start, pattern=r"^add$")
        ],
        states={
            ADD_TOKEN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, take_token)
            ],
            ADD_NEW_BOT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, take_new_bot)
            ],
            ADD_SOURCE_MONGO: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, take_source_mongo)
            ],
            ADD_SOURCE_DB: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, take_source_db)
            ],
            CHOOSE_COLLECTION: [
                CallbackQueryHandler(
                    choose_collection,
                    pattern=r"^pickcol:\d+$",
                ),
                CallbackQueryHandler(
                    cancel_add,
                    pattern=r"^addcancel$",
                ),
            ],
            CHOOSE_UID_FIELD: [
                CallbackQueryHandler(
                    choose_uid_field,
                    pattern=r"^pickuid:\d+$",
                ),
                CallbackQueryHandler(
                    cancel_add,
                    pattern=r"^addcancel$",
                ),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_add)],
        allow_reentry=True,
    )

    master.add_handler(CommandHandler("start", start_cmd))
    master.add_handler(add_conv)
    master.add_handler(
        CallbackQueryHandler(
            menu_callback,
            pattern=r"^(panel|list|reload|broadcast_all|bot:\d+|bc:\d+|imp:\d+|stop:\d+|rm:\d+)$",
        )
    )

    await master.initialize()
    await master.bot.delete_webhook(drop_pending_updates=False)
    await master.start()
    await master.updater.start_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
    )

    for cfg in db.bot_configs.find({"enabled": True}):
        try:
            await start_managed(cfg)
        except Exception:
            log.exception("Saved bot start failed")

    daily_task = asyncio.create_task(daily_loop())

    try:
        await asyncio.Event().wait()
    finally:
        daily_task.cancel()

        for bot_id in list(running):
            await stop_managed(bot_id)

        if master.updater and master.updater.running:
            await master.updater.stop()
        if master.running:
            await master.stop()
        await master.shutdown()
        mongo.close()


if __name__ == "__main__":
    asyncio.run(main())
