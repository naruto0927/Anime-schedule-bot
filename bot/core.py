"""
bot/core.py — AnimeBot: the top-level bot class.

Assembles all components via mixin inheritance:

    AnimeBot
      ├── KeyboardsMixin   (bot/keyboards.py)
      ├── CommandsMixin    (bot/commands.py)
      └── CallbacksMixin   (bot/callbacks.py)

Lifecycle:  __init__ → _register → run → idle → shutdown
Background: _nyaa_poll_loop (polls RSS every NYAA_POLL_INTERVAL seconds)
"""

from __future__ import annotations
import asyncio
import logging
from typing import Optional

from pyrogram import Client, filters, idle, enums
from pyrogram.enums import ChatType
from pyrogram.types import Message, CallbackQuery

from config import API_ID, API_HASH, BOT_TOKEN, ADMIN_IDS, TZ, TZ_NAME
from utils.database import Database
from utils.health import start_health_server
from utils.scheduler import ReminderScheduler
from scrapers.animeschedule import AnimeScheduleScraper
from scrapers.anilist import AniListAPI
from scrapers.season import SeasonScraper
from scrapers.nyaa import NyaaScraper
from bot.keyboards import KeyboardsMixin
from bot.commands import CommandsMixin
from bot.callbacks import CallbacksMixin

logger = logging.getLogger("NarutoTimekeeper")


class AnimeBot(KeyboardsMixin, CommandsMixin, CallbacksMixin):
    """Top-level bot — instantiate and call: await AnimeBot().run()"""

    def __init__(self):
        self.db      = Database(MONGO_URI)
        self.scraper = AnimeScheduleScraper(self.db)
        self.al_api  = AniListAPI(self.db)
        self.season  = SeasonScraper(self.db)
        self.sched   = ReminderScheduler(self.db, self.scraper)
        self.nyaa    = NyaaScraper(self.db)
        self.app: Optional[Client] = None
        self._health = None

    def _get_topic_id(msg: Message) -> Optional[int]:
        """
        Robustly extract the forum topic ID from a Pyrogram Message.

        Log from production showed: reply_to=2, is_topic=None
        So we must NOT gate on is_topic_message — just use reply_to_message_id directly.

          msg.topic.id                — newest pyrogram (forum topic object)
          msg.reply_to_top_message_id — set when user replies inside a topic
          msg.reply_to_message_id     — set to topic root ID for direct sends
          msg.message_thread_id       — some pyrogram forks
        """
        # 1. Newest pyrogram: topic object
        topic = getattr(msg, "topic", None)
        if topic:
            tid = getattr(topic, "id", None)
            if tid:
                return int(tid)

        # 2. reply_to_top_message_id — reliable for replies inside a topic
        tid = getattr(msg, "reply_to_top_message_id", None)
        if tid:
            return int(tid)

        # 3. reply_to_message_id — used for direct sends in a topic
        #    Do NOT gate on is_topic_message; it is None in older Pyrogram builds
        tid = getattr(msg, "reply_to_message_id", None)
        if tid:
            return int(tid)

        # 4. Some pyrogram forks expose message_thread_id
        tid = getattr(msg, "message_thread_id", None)
        if tid:
            return int(tid)

        return None

    async def _auth(self, msg: Message) -> bool:
        if await self.db.is_authorized(msg.chat.id, msg.chat.type):
            return True
        try:
            await msg.reply(
                "\U0001f512 <b>Group not authorized.</b>\n"
                "A group admin must send /auth first.",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return False

    async def _is_group_admin(self, msg: Message) -> bool:
        if msg.from_user and msg.from_user.id in ADMIN_IDS:
            return True
        if msg.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
            try:
                m = await self.app.get_chat_member(msg.chat.id, msg.from_user.id)
                return m.status.value in ("administrator", "owner", "creator")
            except Exception:
                return False
        return False

    async def _is_bot_admin(self, msg: Message) -> bool:
        if msg.from_user and msg.from_user.id in ADMIN_IDS:
            return True
        if msg.from_user and await self.db.is_dynamic_admin(msg.from_user.id):
            return True
        await msg.reply("⛔ Bot admin only.")
        return False

    def _register(self, app: Client):
        app.on_message(filters.command("start"))(self.cmd_start)
        app.on_message(filters.command("help"))(self.cmd_help)
        app.on_message(filters.command("settings"))(self.cmd_settings)
        app.on_message(filters.command("anime"))(self.cmd_anime)
        app.on_message(filters.command("manga"))(self.cmd_manga)
        app.on_message(filters.command("auth"))(self.cmd_auth)
        app.on_message(filters.command("deauth"))(self.cmd_deauth)
        app.on_message(filters.command("mode"))(self.cmd_mode)
        app.on_message(filters.command("filter"))(self.cmd_filter)
        app.on_message(filters.command("season"))(self.cmd_season)
        # Bot admin commands
        app.on_message(filters.command("reload"))(self.cmd_reload)
        app.on_message(filters.command("stats"))(self.cmd_stats)
        app.on_message(filters.command("broadcast"))(self.cmd_broadcast)
        app.on_message(filters.command("addadmin"))(self.cmd_addadmin)
        app.on_message(filters.command("remadmin"))(self.cmd_remadmin)
        app.on_message(filters.command("admins"))(self.cmd_admins)
        app.on_message(filters.command("grouplist"))(self.cmd_grouplist)
        app.on_message(filters.command("users"))(self.cmd_users)
        app.on_message(filters.command("restart"))(self.cmd_restart)
        app.on_callback_query()(self.handle_callback)

    async def run(self):
        if not AS_TOKEN:
            logger.error(
                "=" * 60 + "\n"
                "ANIMESCHEDULE_TOKEN env var is not set!\n"
                "The schedule scraper will return no data.\n"
                "Get a free token at:\n"
                "  animeschedule.net/users/<your_username>/settings/api\n"
                "Then set ANIMESCHEDULE_TOKEN=<token> in your environment.\n"
                + "=" * 60
            )
        self._health = await start_health_server()
        await self.db.init_indexes()

        self.app = Client(
            "naruto_timekeeper",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            parse_mode=enums.ParseMode.HTML,
        )
        self._register(self.app)
        self.sched.app_ref = self.app

        await self.app.start()
        self.sched.start()
        await self.sched.build_all_jobs()

        # Start nyaa polling background task
        nyaa_task = asyncio.create_task(self._nyaa_poll_loop())

        # Notify all bot admins that the bot is alive
        all_admin_ids = list(set(ADMIN_IDS) | set(await self.db.get_dynamic_admins()))
        startup_text = (
            "🟢 <b>Naruto Timekeeper is online!</b>\n"
            f"⏰ {datetime.now(TZ).strftime('%d %b %Y %I:%M %p')} ({TZ_NAME})\n"
            f"📋 Reminder jobs: {len(self.sched.scheduler.get_jobs())}"
        )
        for admin_id in all_admin_ids:
            try:
                await self.app.send_message(admin_id, startup_text, parse_mode=enums.ParseMode.HTML)
            except Exception:
                pass

        logger.info("Naruto Timekeeper is running")
        await idle()

        logger.info("Shutting down\u2026")
        nyaa_task.cancel()
        try:
            await nyaa_task
        except asyncio.CancelledError:
            pass
        self.sched.shutdown()
        await self.scraper.close()
        await self.season.close()
        await self.al_api.close()
        await self.nyaa.close()
        await self.app.stop()
        if self._health:
            await self._health.cleanup()
        self.db.close()
        logger.info("Shutdown complete")

    async def _nyaa_poll_loop(self):
        """Poll nyaa.si / subsplease.org periodically via RSS and push new 1080p releases.
        Uses pubDate watermarking stored in MongoDB — never reset by /reload.
        First poll seeds the watermark silently so no backlog is sent on restart."""
        logger.info("Nyaa polling loop started (interval=%ds)", NYAA_POLL_INTERVAL)
        while True:
            try:
                await asyncio.sleep(NYAA_POLL_INTERVAL)
                new_entries = await self.nyaa.fetch_new()

                if not new_entries:
                    continue

                targets = await self.db.all_chats_with_nyaa_topic()
                if not targets:
                    continue

                for entry in new_entries:
                    text = NyaaScraper.format_entry(entry)
                    for cid, tid in targets:
                        # Respect per-topic uploader filter
                        cfg      = await self.db.get_topic_cfg(cid, tid)
                        # Merge over full defaults — partial DB saves must not
                        # silently default missing uploaders to always-send.
                        _nyaa_defaults = {"varyg": True, "toonshub": True, "subsplease": True}
                        nyaa_cfg = {**_nyaa_defaults, **cfg.get("nyaa", {})}
                        uploader_key = {
                            "varyg1001":  "varyg",
                            "ToonsHub":   "toonshub",
                            "subsplease": "subsplease",
                        }.get(entry.get("uploader", ""))
                        if uploader_key and not nyaa_cfg[uploader_key]:
                            continue
                        # Send with FloodWait handling
                        sent = False
                        for attempt in range(3):
                            try:
                                await self.app.send_message(
                                    cid, text,
                                    parse_mode=enums.ParseMode.HTML,
                                    reply_to_message_id=tid,
                                    disable_web_page_preview=True,
                                )
                                sent = True
                                break
                            except Exception as exc:
                                err = str(exc)
                                if "FLOOD_WAIT" in err:
                                    # Parse wait seconds and sleep
                                    import re as _re
                                    m = _re.search(r"wait of (\d+) seconds", err)
                                    wait = int(m.group(1)) + 2 if m else 30
                                    logger.info("FloodWait %ds on nyaa send, sleeping…", wait)
                                    await asyncio.sleep(wait)
                                else:
                                    logger.warning("Nyaa send to %s/%s failed: %s", cid, tid, exc)
                                    break
                        if not sent:
                            logger.warning("Nyaa send to %s/%s gave up after retries", cid, tid)
                        # Polite delay between each message to avoid hitting limits
                        await asyncio.sleep(1.5)

            except asyncio.CancelledError:
                logger.info("Nyaa polling loop stopped")
                return
            except Exception as exc:
                logger.error("Nyaa poll loop error: %s", exc)
                await asyncio.sleep(60)


# ── Entry ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(AnimeBot().run())
