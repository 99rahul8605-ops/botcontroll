# Telegram Migration Master Panel

Render ENV me sirf:
- MASTER_BOT_TOKEN
- ADMIN_USER_ID
- DEST_MONGO_URI
- DEST_DB (optional)
- TIMEZONE / BROADCAST_TIME (optional)

Deploy ke baad master bot me `/start` bhejo.

Buttons:
- Add Bot
- My Bots
- Broadcast All
- Reload

Add Bot wizard asks for:
1. Old bot token
2. New bot username
3. Old/source MongoDB URL
4. Source database
5. Source collection
6. UID fields

Each bot configuration is stored in the destination MongoDB, so Render restarts do not erase it.

Important:
- Master panel works only for ADMIN_USER_ID.
- Telegram Bot API cannot fetch every historical user. The old DB must contain chat/user IDs.
- Daily migration broadcast is sent from the OLD bot.
- A NEW bot cannot proactively DM a user until that user starts the new bot.
- Keep tokens and Mongo credentials only in Render Environment Variables / the private admin panel.
