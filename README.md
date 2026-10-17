# Telegram Migration Master Panel v3

## New easier Add Bot flow

1. `/start`
2. `➕ Add Bot`
3. Send OLD bot token
4. Send NEW bot username
5. Send old/source MongoDB URL
6. Send database name, e.g. `mybotdb`
7. Bot automatically lists all collections as inline buttons
8. Tap the collection (e.g. `users`)
9. Bot inspects sample documents and lists possible fields as inline buttons
10. Tap UID field (e.g. `userId`)
11. Import starts automatically and shows:
   - documents scanned
   - valid IDs found
   - unique users saved

No need to type collection or UID field manually.

## Render ENV

Required:

MASTER_BOT_TOKEN
ADMIN_USER_ID
DEST_MONGO_URI

Optional:

DEST_DB=telegram_migration
TIMEZONE=Asia/Kolkata
BROADCAST_TIME=10:00

## Existing features

- Multiple old bots from one master bot
- Bot configs persist in destination MongoDB
- Old DB users imported per bot
- New messages to old bot auto-save user + show migration button
- Daily broadcast
- Manual broadcast per bot
- Broadcast all
- Re-import
- Stop/remove bot

Important:
Telegram Bot API cannot list historical users. Historical user IDs must exist in the old database.


## v4 anti-spam safety

This version is private-chat-only:

- Does not auto-reply in groups
- Does not auto-reply in supergroups
- Does not auto-reply in channels
- Does not import negative Telegram chat IDs
- Broadcast only targets positive private-user chat IDs
- Existing negative IDs from older versions are automatically marked inactive on startup


## v5 multitasking/performance

Long operations no longer run inside Telegram callback handlers.

- Broadcast runs as a background task.
- Re-import runs as a background task.
- Multiple different bots can broadcast/import concurrently.
- A single bot cannot accidentally start duplicate broadcasts/imports.
- Synchronous PyMongo import is moved to a worker thread (`asyncio.to_thread`).
- Broadcast uses controlled parallel sends (`BROADCAST_CONCURRENCY`, default 12).
- Huge user lists are processed in chunks instead of creating one asyncio task per user.
- Master and managed Telegram applications enable concurrent update processing.
- Starting/reloading an old bot no longer waits for its Mongo import to finish.
- Stopping a bot cancels its active import/broadcast jobs.

### Tuning

Start with:

`BROADCAST_CONCURRENCY=12`

On a small Render instance, 8-15 is a sensible range. Higher is not always faster because Telegram rate limits can trigger RetryAfter.


## v6 live status / responsiveness fix

- Broadcast button returns immediately.
- `📊 Live Status` shows current sent/blocked/failed counters and progress.
- `🔄 Refresh` refreshes the same bot page.
- Re-import status is shown on the same page.
- Background-job launcher is lazy, avoiding orphan coroutine objects on duplicate clicks.
- Mongo counts used by UI run in worker threads.
- Default broadcast concurrency reduced to 8 for smoother Telegram rate-limit behaviour.
