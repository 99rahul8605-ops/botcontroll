import os, asyncio, logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiohttp import web
from pymongo import MongoClient, UpdateOne
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter, Forbidden, BadRequest, NetworkError
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ConversationHandler, MessageHandler, ContextTypes, filters

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

MASTER_BOT_TOKEN = os.getenv("MASTER_BOT_TOKEN","").strip()
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID","0"))
DEST_MONGO_URI = os.getenv("DEST_MONGO_URI","").strip()
DEST_DB = os.getenv("DEST_DB","telegram_migration")
TIMEZONE = os.getenv("TIMEZONE","Asia/Kolkata")
BROADCAST_TIME = os.getenv("BROADCAST_TIME","10:00")
BUTTON_TEXT = os.getenv("BUTTON_TEXT","🚀 Open New Bot")
BROADCAST_TEXT = os.getenv("BROADCAST_TEXT","⚠️ This bot has been shut down.\n\nWe have moved to our new bot.\n\nPlease tap the button below to continue 👇")
PORT = int(os.getenv("PORT","10000"))

TOKEN, NEWBOT, MONGO, DBNAME, COLLECTION, UIDFIELDS = range(6)
mongo = MongoClient(DEST_MONGO_URI, serverSelectionTimeoutMS=15000)
db = mongo[DEST_DB]
running = {}
configs = {}

def admin(update): return bool(update.effective_user and update.effective_user.id == ADMIN_USER_ID)

def panel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Bot",callback_data="add"), InlineKeyboardButton("📋 My Bots",callback_data="list")],
        [InlineKeyboardButton("📣 Broadcast All",callback_data="broadcast_all"), InlineKeyboardButton("🔄 Reload",callback_data="reload")]
    ])

def migrate_markup(username):
    return InlineKeyboardMarkup([[InlineKeyboardButton(BUTTON_TEXT,url=f"https://t.me/{username}")]])

def dotted(doc,key):
    cur=doc
    for p in key.split("."):
        if not isinstance(cur,dict) or p not in cur: return None
        cur=cur[p]
    return cur

def as_id(v):
    if isinstance(v,bool) or v is None: return None
    if isinstance(v,int): return v
    if isinstance(v,float) and v.is_integer(): return int(v)
    if isinstance(v,str):
        s=v.strip()
        if s.startswith("-") and s[1:].isdigit(): return int(s)
        if s.isdigit(): return int(s)
    return None

async def start_cmd(update,context):
    if not admin(update): return
    await update.message.reply_text("🛠 Migration Master",reply_markup=panel())

async def add_start(update,context):
    q=update.callback_query; await q.answer()
    if not admin(update): return ConversationHandler.END
    context.user_data["cfg"]={}
    await q.edit_message_text("Old bot TOKEN bhejo.\n\n/cancel to stop")
    return TOKEN

async def take_token(update,context):
    token=update.message.text.strip()
    try:
        a=Application.builder().token(token).build(); await a.initialize()
        info=await a.bot.get_me(); await a.shutdown()
    except Exception as e:
        await update.message.reply_text(f"❌ Token invalid:\n{e}\n\nDobara bhejo.")
        return TOKEN
    c=context.user_data["cfg"]; c.update(token=token,bot_id=info.id,old_bot_username=info.username)
    await update.message.reply_text(f"✅ @{info.username}\n\nNew bot username bhejo:")
    return NEWBOT

async def take_new(update,context):
    context.user_data["cfg"]["new_bot"]=update.message.text.strip().replace("@","")
    await update.message.reply_text("Old/source MongoDB URL bhejo:")
    return MONGO

async def take_mongo(update,context):
    uri=update.message.text.strip()
    try:
        c=MongoClient(uri,serverSelectionTimeoutMS=10000)
        await asyncio.to_thread(c.admin.command,"ping"); c.close()
    except Exception as e:
        await update.message.reply_text(f"❌ Mongo connect failed:\n{e}\n\nDobara URL bhejo.")
        return MONGO
    context.user_data["cfg"]["source_mongo_uri"]=uri
    await update.message.reply_text("Source DB name bhejo:")
    return DBNAME

async def take_db(update,context):
    context.user_data["cfg"]["source_db"]=update.message.text.strip()
    await update.message.reply_text("Source collection name bhejo:")
    return COLLECTION

async def take_collection(update,context):
    context.user_data["cfg"]["source_collection"]=update.message.text.strip()
    await update.message.reply_text("UID fields comma-separated bhejo.\nExample: chat_id,user_id,telegram_id,id\nYa `default` bhejo.")
    return UIDFIELDS

async def take_fields(update,context):
    txt=update.message.text.strip()
    fields=["chat_id","user_id","telegram_id","telegram_user_id","id"] if txt.lower()=="default" else [x.strip() for x in txt.split(",") if x.strip()]
    c=context.user_data["cfg"]; c.update(uid_fields=fields,enabled=True,updated_at=datetime.utcnow(),created_at=datetime.utcnow())
    db.bot_configs.update_one({"bot_id":c["bot_id"]},{"$set":c},upsert=True)
    try:
        await start_managed(c)
        msg=f"✅ Added & started @{c['old_bot_username']} → @{c['new_bot']}"
    except Exception as e:
        msg=f"⚠️ Config saved but bot start failed:\n{e}"
    await update.message.reply_text(msg,reply_markup=panel())
    context.user_data.pop("cfg",None)
    return ConversationHandler.END

async def cancel(update,context):
    context.user_data.pop("cfg",None)
    await update.message.reply_text("Cancelled.",reply_markup=panel())
    return ConversationHandler.END

async def import_users(c):
    src=MongoClient(c["source_mongo_uri"],serverSelectionTimeoutMS=15000)
    await asyncio.to_thread(src.admin.command,"ping")
    col=src[c["source_db"]][c["source_collection"]]
    scanned=found=0; ops=[]
    for doc in col.find({}):
        scanned+=1; cid=None
        for f in c["uid_fields"]:
            cid=as_id(dotted(doc,f))
            if cid is not None: break
        if cid is None: continue
        found+=1
        ops.append(UpdateOne({"bot_id":c["bot_id"],"chat_id":cid},{"$set":{
            "bot_id":c["bot_id"],"old_bot_username":c["old_bot_username"],"new_bot_username":c["new_bot"],
            "chat_id":cid,"active":True,"source_db":c["source_db"],"source_collection":c["source_collection"],"updated_at":datetime.utcnow()
        },"$setOnInsert":{"created_at":datetime.utcnow(),"imported_from_old_db":True}},upsert=True))
        if len(ops)>=1000: db.users.bulk_write(ops,ordered=False); ops=[]
    if ops: db.users.bulk_write(ops,ordered=False)
    src.close()
    db.bot_configs.update_one({"bot_id":c["bot_id"]},{"$set":{"last_import_at":datetime.utcnow(),"last_import_scanned":scanned,"last_import_found":found}})
    return scanned,found

async def oldbot_handler(update,context):
    m=update.effective_message; chat=update.effective_chat; user=update.effective_user
    if not m or not chat: return
    c=configs.get(context.bot.id)
    if not c: return
    db.users.update_one({"bot_id":context.bot.id,"chat_id":chat.id},{"$set":{
        "bot_id":context.bot.id,"old_bot_username":c["old_bot_username"],"new_bot_username":c["new_bot"],"chat_id":chat.id,
        "telegram_user_id":user.id if user else None,"username":user.username if user else None,
        "first_name":user.first_name if user else None,"last_name":user.last_name if user else None,
        "active":True,"last_seen_at":datetime.utcnow(),"updated_at":datetime.utcnow()
    },"$setOnInsert":{"created_at":datetime.utcnow(),"imported_from_old_db":False}},upsert=True)
    await m.reply_text(BROADCAST_TEXT,reply_markup=migrate_markup(c["new_bot"]))

async def start_managed(c):
    bid=int(c["bot_id"])
    if bid in running: return
    app=Application.builder().token(c["token"]).build(); await app.initialize()
    await app.bot.delete_webhook(drop_pending_updates=False)
    info=await app.bot.get_me()
    c["bot_id"]=info.id; c["old_bot_username"]=info.username
    configs[info.id]=c
    app.add_handler(MessageHandler(filters.ALL,oldbot_handler))
    await import_users(c)
    await app.start(); await app.updater.start_polling(allowed_updates=Update.ALL_TYPES,drop_pending_updates=False)
    running[info.id]=app
    db.bot_configs.update_one({"bot_id":info.id},{"$set":{"enabled":True,"old_bot_username":info.username,"updated_at":datetime.utcnow()}},upsert=True)

async def stop_managed(bid):
    app=running.pop(bid,None); configs.pop(bid,None)
    if not app:return
    if app.updater and app.updater.running: await app.updater.stop()
    if app.running: await app.stop()
    await app.shutdown()

async def send_one(app,cid,text,markup):
    while True:
        try:
            await app.bot.send_message(cid,text,reply_markup=markup); return "sent"
        except RetryAfter as e: await asyncio.sleep(float(e.retry_after)+1)
        except Forbidden: return "blocked"
        except (BadRequest,NetworkError): return "failed"
        except Exception: return "failed"

async def broadcast_bot(bid):
    app=running.get(bid); c=configs.get(bid)
    if not app or not c: return (0,0,0)
    sent=blocked=failed=0; markup=migrate_markup(c["new_bot"])
    for row in db.users.find({"bot_id":bid,"active":True},{"chat_id":1}):
        cid=row.get("chat_id")
        if cid is None: continue
        st=await send_one(app,cid,BROADCAST_TEXT,markup)
        if st=="sent": sent+=1
        elif st=="blocked":
            blocked+=1; db.users.update_one({"bot_id":bid,"chat_id":cid},{"$set":{"active":False}})
        else: failed+=1
        db.users.update_one({"bot_id":bid,"chat_id":cid},{"$set":{"last_broadcast_at":datetime.utcnow(),"last_broadcast_status":st}})
        await asyncio.sleep(.05)
    return sent,blocked,failed

async def menu_callback(update,context):
    q=update.callback_query; await q.answer()
    if not admin(update): return
    d=q.data
    if d=="panel":
        await q.edit_message_text("🛠 Migration Master",reply_markup=panel()); return
    if d=="list":
        bots=list(db.bot_configs.find({}))
        kb=[[InlineKeyboardButton(("🟢 " if b["bot_id"] in running else "🔴 ")+f"@{b.get('old_bot_username',b['bot_id'])}",callback_data=f"bot:{b['bot_id']}")] for b in bots]
        kb.append([InlineKeyboardButton("⬅️ Back",callback_data="panel")])
        await q.edit_message_text("📋 Bots",reply_markup=InlineKeyboardMarkup(kb)); return
    if d=="reload":
        for c in db.bot_configs.find({"enabled":True}):
            try: await start_managed(c)
            except Exception: log.exception("reload failed")
        await q.edit_message_text("Reload complete.",reply_markup=panel()); return
    if d=="broadcast_all":
        total=[0,0,0]
        for bid in list(running):
            r=await broadcast_bot(bid); total=[total[i]+r[i] for i in range(3)]
        await q.edit_message_text(f"✅ Broadcast All complete\nSent: {total[0]}\nBlocked: {total[1]}\nFailed: {total[2]}",reply_markup=panel()); return
    if d.startswith("bot:"):
        bid=int(d.split(":")[1]); c=db.bot_configs.find_one({"bot_id":bid})
        n=db.users.count_documents({"bot_id":bid}); a=db.users.count_documents({"bot_id":bid,"active":True})
        kb=InlineKeyboardMarkup([
            [InlineKeyboardButton("📣 Broadcast",callback_data=f"bc:{bid}"),InlineKeyboardButton("📥 Re-import",callback_data=f"imp:{bid}")],
            [InlineKeyboardButton("🛑 Stop",callback_data=f"stop:{bid}"),InlineKeyboardButton("🗑 Remove",callback_data=f"rm:{bid}")],
            [InlineKeyboardButton("⬅️ Back",callback_data="list")]
        ])
        await q.edit_message_text(f"Old: @{c.get('old_bot_username','-')}\nNew: @{c.get('new_bot','-')}\nUsers: {n}\nActive: {a}",reply_markup=kb); return
    action,bid_s=d.split(":"); bid=int(bid_s); c=db.bot_configs.find_one({"bot_id":bid})
    if action=="bc":
        r=await broadcast_bot(bid); await q.edit_message_text(f"✅ Sent {r[0]} | Blocked {r[1]} | Failed {r[2]}",reply_markup=panel())
    elif action=="imp":
        r=await import_users(c); await q.edit_message_text(f"✅ Scanned {r[0]} | Imported IDs {r[1]}",reply_markup=panel())
    elif action=="stop":
        await stop_managed(bid); db.bot_configs.update_one({"bot_id":bid},{"$set":{"enabled":False}})
        await q.edit_message_text("🛑 Stopped.",reply_markup=panel())
    elif action=="rm":
        await stop_managed(bid); db.bot_configs.delete_one({"bot_id":bid})
        await q.edit_message_text("🗑 Removed config. Imported users retained.",reply_markup=panel())

def secs_to_daily():
    tz=ZoneInfo(TIMEZONE); now=datetime.now(tz); h,m=map(int,BROADCAST_TIME.split(":"))
    t=now.replace(hour=h,minute=m,second=0,microsecond=0)
    if t<=now:t+=timedelta(days=1)
    return max(1,(t-now).total_seconds())

async def daily_loop():
    while True:
        await asyncio.sleep(secs_to_daily())
        for bid in list(running):
            try: await broadcast_bot(bid)
            except Exception: log.exception("daily broadcast failed")

async def webserver():
    async def health(req): return web.json_response({"status":"ok","running_bots":len(running)})
    app=web.Application(); app.router.add_get("/",health); app.router.add_get("/health",health)
    r=web.AppRunner(app); await r.setup(); await web.TCPSite(r,"0.0.0.0",PORT).start()

async def main():
    if not MASTER_BOT_TOKEN or not ADMIN_USER_ID or not DEST_MONGO_URI:
        raise RuntimeError("MASTER_BOT_TOKEN, ADMIN_USER_ID, DEST_MONGO_URI are required")
    await asyncio.to_thread(mongo.admin.command,"ping")
    db.users.create_index([("bot_id",1),("chat_id",1)],unique=True)
    db.bot_configs.create_index("bot_id",unique=True)
    await webserver()

    master=Application.builder().token(MASTER_BOT_TOKEN).build()
    conv=ConversationHandler(
        entry_points=[CallbackQueryHandler(add_start,pattern="^add$")],
        states={
            TOKEN:[MessageHandler(filters.TEXT & ~filters.COMMAND,take_token)],
            NEWBOT:[MessageHandler(filters.TEXT & ~filters.COMMAND,take_new)],
            MONGO:[MessageHandler(filters.TEXT & ~filters.COMMAND,take_mongo)],
            DBNAME:[MessageHandler(filters.TEXT & ~filters.COMMAND,take_db)],
            COLLECTION:[MessageHandler(filters.TEXT & ~filters.COMMAND,take_collection)],
            UIDFIELDS:[MessageHandler(filters.TEXT & ~filters.COMMAND,take_fields)],
        },
        fallbacks=[CommandHandler("cancel",cancel)]
    )
    master.add_handler(CommandHandler("start",start_cmd))
    master.add_handler(conv)
    master.add_handler(CallbackQueryHandler(menu_callback,pattern=r"^(panel|list|reload|broadcast_all|bot:\d+|bc:\d+|imp:\d+|stop:\d+|rm:\d+)$"))

    await master.initialize(); await master.bot.delete_webhook(drop_pending_updates=False)
    await master.start(); await master.updater.start_polling(allowed_updates=Update.ALL_TYPES,drop_pending_updates=False)

    for c in db.bot_configs.find({"enabled":True}):
        try: await start_managed(c)
        except Exception: log.exception("saved bot start failed")

    task=asyncio.create_task(daily_loop())
    try: await asyncio.Event().wait()
    finally:
        task.cancel()
        for bid in list(running): await stop_managed(bid)
        if master.updater.running: await master.updater.stop()
        await master.stop(); await master.shutdown(); mongo.close()

if __name__=="__main__":
    asyncio.run(main())
