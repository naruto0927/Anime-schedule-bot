"""
Naruto Timekeeper — Anime Schedule Telegram Bot
Pyrogram + Motor + httpx + APScheduler + AniList
Schedule data sourced via AnimeScheduleScraper (animeschedule.net)
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx
from aiohttp import web
from bs4 import BeautifulSoup
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters, idle, enums
from pyrogram.enums import ChatType
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InlineQueryResultPhoto,
    InputTextMessageContent,
    Message,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("NarutoTimekeeper")
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# ── Environment ───────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
MONGO_URI = os.getenv("MONGO_URI")
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "6672752177").split(",") if x.strip()]
OWNER_ID  = ADMIN_IDS[0] if ADMIN_IDS else 0   # First entry is always the bot owner
TZ_NAME   = os.environ.get("TIMEZONE", "Asia/Kolkata")
TZ        = ZoneInfo(TZ_NAME)
AS_TOKEN  = os.environ.get("ANIMESCHEDULE_TOKEN", "")  # Required — get free token at animeschedule.net/users/<user>/settings/api
PORT      = int(os.environ.get("PORT", "8000"))
BOT_IMAGE = os.environ.get("BOT_IMAGE_URL", "")

ANILIST_API  = "https://graphql.anilist.co"
AS_BASE_URL  = "https://animeschedule.net"
AS_CDN_BASE  = "https://img.animeschedule.net/production/assets/public/img/"
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# ── New feature config ─────────────────────────────────────────────────────────
GEMINI_API_KEY   = os.environ.get("GEMINI_API_KEY", "")          # Required for /naruto
GEMINI_MODEL     = "gemini-2.5-flash"
GEMINI_API_URL   = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
GEMINI_MAX_HIST  = int(os.environ.get("GEMINI_MAX_HIST", "20"))   # messages to keep per user
BANNER_DB_CHAT   = int(os.environ.get("BANNER_DB_CHAT", "-1003922180872"))     # Channel ID used as banner DB
AIRING_PAGE_SIZE = int(os.environ.get("AIRING_PAGE_SIZE", "8"))   # entries per airing page

# Telegram caption/message limits
TG_CAPTION_LIMIT = 1024
TG_MESSAGE_LIMIT = 4096

# Null sentinel used by animeschedule.net for absent datetime values
_AS_NULL_DT = "0001-01-01T00:00:00Z"

# ── Nyaa.si config ────────────────────────────────────────────────────────────
NYAA_BASE_URL   = "https://nyaa.si"
NYAA_USER_URL   = f"{NYAA_BASE_URL}/user/varyg1001"  # uploader 1
NYAA_SEARCH_URL = f"{NYAA_BASE_URL}/"                 # search for ToonsHub
NYAA_POLL_INTERVAL = int(os.environ.get("NYAA_POLL_INTERVAL", "120"))  # seconds



# ── Chat Filter ──────────────────────────────────────────────────────────────
# Media type options from animeschedule.net (slug → display label)
MEDIA_TYPES: dict = {
    "tv":       "TV",
    "tv-short": "TV Short",
    "ona":      "ONA",
    "ova":      "OVA",
    "special":  "Special",
    "movie":    "Movie",
}

# Streaming platform options (slug → display label)
STREAMS: dict = {
    "crunchyroll": "Crunchyroll",
    "netflix":     "Netflix",
    "hidive":      "HiDive",
    "amazon":      "Amazon",
    "hulu":        "Hulu",
    "youtube":     "YouTube",
    "disney+":     "Disney+",
    "bilibili":    "BiliBili",
    "appletv":     "Apple TV",
    "oceanveil":   "OceanVeil",
}


class ChatFilter:
    """
    Per-chat schedule filter.  Stored inside the existing chats collection.

    Filters matching animeschedule.net's own filter panel:
        air_types    — set of enabled air types: {"raw","sub","dub"} (default: all)
        media_types  — set of enabled media type slugs (default: all = empty set means no filter)
        streams      — set of required streaming platform slugs (default: any)
        hide_donghua — exclude Chinese anime (default: False)
    """
    def __init__(
        self,
        air_types:    Optional[set] = None,
        media_types:  Optional[set] = None,
        streams:      Optional[set] = None,
        hide_donghua: bool = False,
    ):
        # None/empty means "show all"
        self.air_types    = set(air_types)   if air_types    else {"raw", "sub", "dub"}
        self.media_types  = set(media_types) if media_types  else set()
        self.streams      = set(streams)     if streams      else set()
        self.hide_donghua = hide_donghua

    @classmethod
    def from_doc(cls, doc: Optional[dict]) -> "ChatFilter":
        if not doc:
            return cls()
        f = doc.get("filter", {})
        return cls(
            air_types    = set(f.get("air_types",   ["raw", "sub", "dub"])),
            media_types  = set(f.get("media_types", [])),
            streams      = set(f.get("streams",     [])),
            hide_donghua = f.get("hide_donghua", False),
        )

    def to_dict(self) -> dict:
        return {
            "air_types":    list(self.air_types),
            "media_types":  list(self.media_types),
            "streams":      list(self.streams),
            "hide_donghua": self.hide_donghua,
        }

    def allows_air_type(self, air_type: str) -> bool:
        return air_type.lower() in self.air_types

    # Backward-compat alias used by _send_reminder
    def allows(self, air_type: str) -> bool:
        return self.allows_air_type(air_type)

    def as_api_params(self) -> dict:
        """
        Return extra query params to pass to /api/v3/timetables so the API
        itself filters by media type, stream, and donghua at source.
        Air-type filtering is still done post-fetch in process().
        """
        params: dict = {}
        for mt in self.media_types:
            params.setdefault("media-types", []).append(mt)
        for st in self.streams:
            params.setdefault("streams", []).append(st)
        # donghua: animeschedule uses a "no-donghua" style filter param
        # The API accepts genres-exclude but donghua is a boolean on entries
        # We handle it post-fetch via _filter_donghua flag on the entry
        return params

    def is_default(self) -> bool:
        return (
            self.air_types == {"raw", "sub", "dub"}
            and not self.media_types
            and not self.streams
            and not self.hide_donghua
        )


# ── Health server ───────────────────────────────────────────────────────────────
async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", lambda _: web.Response(text="OK"))
    app.router.add_get("/health", lambda _: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logger.info("Health server on port %s", PORT)
    return runner


# ── Database ──────────────────────────────────────────────────────────────────
class Database:
    def __init__(self, uri: str):
        self.client    = AsyncIOMotorClient(uri)
        self.db        = self.client["animebot"]
        self.chats     = self.db["chats"]
        self.jobs      = self.db["jobs"]
        self.cache     = self.db["cache"]
        self.stats     = self.db["stats"]
        self.auth      = self.db["auth"]
        self.banners    = self.db["banners"]     # Banner DB: {name, category, msg_id, file_type}
        self.reminders  = self.db["reminders"]   # User reminders: {user_id, day_of_week, time, message}
        self.savemonday = self.db["savemonday"]  # Monday saves: {chat_id, text, saved_at}
        self.afk_users  = self.db["afk_users"]   # AFK: {user_id, message, since}

    def close(self):
        self.client.close()

    async def init_indexes(self):
        await self.chats.create_index("chat_id", unique=True)
        await self.jobs.create_index(
            [("anime_slug", 1), ("episode", 1), ("version", 1)], unique=True
        )
        await self.cache.create_index("key", unique=True)
        await self.auth.create_index("chat_id", unique=True)
        await self.db["admins"].create_index("user_id", unique=True)
        # Banners: no unique on name (can have same name in different categories)
        await self.banners.create_index([("name", 1), ("category", 1)])
        await self.reminders.create_index("user_id")
        await self.savemonday.create_index("chat_id", unique=True)
        await self.afk_users.create_index("user_id", unique=True)
        logger.info("DB indexes ensured")

    # ── Auth ──────────────────────────────────────────────────────────────
    async def authorize_group(self, chat_id: int, by: int):
        await self.auth.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, "authorized": True, "by": by}},
            upsert=True,
        )

    async def deauthorize_group(self, chat_id: int):
        await self.auth.update_one({"chat_id": chat_id}, {"$set": {"authorized": False}})

    async def is_authorized(self, chat_id: int, chat_type: ChatType) -> bool:
        if chat_type == ChatType.PRIVATE:
            return True
        doc = await self.auth.find_one({"chat_id": chat_id})
        return bool(doc and doc.get("authorized"))

    async def count_auth(self) -> int:
        return await self.auth.count_documents({"authorized": True})

    # ── Subscriptions ─────────────────────────────────────────────────────
    async def subscribe(self, chat_id: int):
        await self.chats.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, "subscribed": True}},
            upsert=True,
        )

    async def unsubscribe(self, chat_id: int):
        await self.chats.update_one({"chat_id": chat_id}, {"$set": {"subscribed": False}})

    async def is_subscribed(self, chat_id: int) -> bool:
        doc = await self.chats.find_one({"chat_id": chat_id})
        return bool(doc and doc.get("subscribed"))

    async def all_subscribed_chats(self) -> list:
        return [d["chat_id"] async for d in self.chats.find({"subscribed": True})]

    # ── Jobs ──────────────────────────────────────────────────────────────
    async def bulk_save_jobs(self, job_list: list):
        if not job_list:
            return
        from pymongo import UpdateOne
        ops = [
            UpdateOne(
                {
                    "anime_slug": j["anime_slug"],
                    "episode":    j["episode"],
                    "version":    j["version"],
                },
                {"$set": j},
                upsert=True,
            )
            for j in job_list
        ]
        await self.jobs.bulk_write(ops, ordered=False)

    async def clear_jobs(self):
        await self.jobs.delete_many({})

    # ── Cache ─────────────────────────────────────────────────────────────
    async def set_cache(self, key: str, value: Any, ttl: int = 21600):
        exp = datetime.now(timezone.utc) + timedelta(seconds=ttl)
        await self.cache.update_one(
            {"key": key},
            {"$set": {"key": key, "value": value, "expires_at": exp}},
            upsert=True,
        )

    async def get_cache(self, key: str) -> Any:
        doc = await self.cache.find_one({"key": key})
        if not doc:
            return None
        exp = doc["expires_at"]
        now = datetime.now(timezone.utc)
        # MongoDB may return naive UTC datetimes — normalise both to aware UTC
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if exp < now:
            await self.cache.delete_one({"key": key})
            return None
        return doc["value"]

    async def get_cache_ignore_ttl(self, key: str) -> Any:
        """Return cached value even if expired — used as scrape fallback."""
        doc = await self.cache.find_one({"key": key})
        return doc["value"] if doc else None

    # ── Stats ─────────────────────────────────────────────────────────────
    async def inc_stat(self, field: str, n: int = 1):
        await self.stats.update_one({"_id": "global"}, {"$inc": {field: n}}, upsert=True)

    async def get_stats(self) -> dict:
        doc = await self.stats.find_one({"_id": "global"}) or {}
        doc.pop("_id", None)
        return doc

    async def count(self, col: str) -> int:
        return await self.db[col].count_documents({})

    # ── Dynamic admins ────────────────────────────────────────────────────
    async def add_admin(self, user_id: int):
        await self.db["admins"].update_one(
            {"user_id": user_id}, {"$set": {"user_id": user_id}}, upsert=True
        )

    async def remove_admin(self, user_id: int):
        await self.db["admins"].delete_one({"user_id": user_id})

    async def get_dynamic_admins(self) -> list:
        return [d["user_id"] async for d in self.db["admins"].find()]

    async def is_dynamic_admin(self, user_id: int) -> bool:
        return bool(await self.db["admins"].find_one({"user_id": user_id}))

    # ── Group / user lists ────────────────────────────────────────────────
    async def all_authorized_groups(self) -> list:
        """Return list of {chat_id, by} for all authorized groups."""
        return [
            {"chat_id": d["chat_id"], "by": d.get("by")}
            async for d in self.auth.find({"authorized": True})
        ]

    async def count_private_users(self) -> int:
        """Count unique private chats (individual users) that have interacted."""
        return await self.chats.count_documents({"chat_type": "private"})

    async def track_user(self, msg):
        """Record private user interaction for user count tracking."""
        from pyrogram.enums import ChatType as _CT
        if msg.chat.type == _CT.PRIVATE:
            await self.chats.update_one(
                {"chat_id": msg.chat.id},
                {"$set": {"chat_id": msg.chat.id, "chat_type": "private"}},
                upsert=True,
            )
    async def get_filter(self, chat_id: int) -> "ChatFilter":
        doc = await self.chats.find_one({"chat_id": chat_id})
        return ChatFilter.from_doc(doc)

    async def set_filter(self, chat_id: int, flt: "ChatFilter"):
        await self.chats.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, "filter": flt.to_dict()}},
            upsert=True,
        )

    async def reset_filter(self, chat_id: int):
        await self.chats.update_one(
            {"chat_id": chat_id},
            {"$unset": {"filter": ""}},
        )

    async def all_subscribed_with_filters(self) -> list:
        """Return list of (chat_id, ChatFilter) for all subscribed chats."""
        result = []
        async for doc in self.chats.find({"subscribed": True}):
            result.append((doc["chat_id"], ChatFilter.from_doc(doc)))
        return result

    # ── Topic / Mode management ───────────────────────────────────────────
    async def set_topic_mode(self, chat_id: int, topic_id: int, mode: str):
        """
        Bind a group topic (message_thread_id) to a mode: 'rem' or 'nyaa'.
        Stored in chats document under topics: {str(topic_id): mode}.
        """
        await self.chats.update_one(
            {"chat_id": chat_id},
            {"$set": {f"topics.{topic_id}": mode, "chat_id": chat_id}},
            upsert=True,
        )

    async def get_topic_mode(self, chat_id: int, topic_id: Optional[int]) -> Optional[str]:
        """Return mode ('rem'|'nyaa') for a given chat+topic, or None."""
        if not topic_id:
            return None
        doc = await self.chats.find_one({"chat_id": chat_id})
        if not doc:
            return None
        return (doc.get("topics") or {}).get(str(topic_id))

    async def get_topic_by_mode(self, chat_id: int, mode: str) -> Optional[int]:
        """Return the topic_id bound to a given mode for a chat, or None."""
        doc = await self.chats.find_one({"chat_id": chat_id})
        if not doc:
            return None
        topics = doc.get("topics") or {}
        for tid, m in topics.items():
            if m == mode:
                return int(tid)
        return None

    async def all_chats_with_nyaa_topic(self) -> list:
        """Return list of (chat_id, topic_id) that have a 'nyaa' topic set."""
        result = []
        async for doc in self.chats.find({"topics": {"$exists": True}}):
            cid = doc["chat_id"]
            # Skip deauthorized groups
            auth_doc = await self.auth.find_one({"chat_id": cid})
            if not auth_doc or not auth_doc.get("authorized"):
                continue
            for tid, mode in (doc.get("topics") or {}).items():
                if mode == "nyaa":
                    result.append((cid, int(tid)))
        return result

    async def all_chats_with_rem_topic(self) -> list:
        """
        Return list of (chat_id, topic_id, ChatFilter) for reminder delivery.

        Includes two groups:
          1. Chats with a 'rem' topic bound — use that topic_id (topic-mode)
          2. Subscribed chats with no topic — use topic_id=None (legacy mode)
        """
        result = []
        seen: set = set()

        # Group 1: chats with 'rem' topic(s) — deliver to EVERY rem topic, not just first
        async for doc in self.chats.find({"topics": {"$exists": True}}):
            topics = doc.get("topics") or {}
            cid    = doc["chat_id"]
            # Skip deauthorized groups
            auth_doc = await self.auth.find_one({"chat_id": cid})
            if not auth_doc or not auth_doc.get("authorized"):
                continue
            # Mark ALL chats that have ANY topic assignment in seen so that
            # nyaa-only chats never fall through to Group 2 as plain subscribers.
            seen.add(cid)
            for tid, mode in topics.items():
                if mode == "rem":
                    result.append((cid, int(tid), ChatFilter.from_doc(doc)))

        # Group 2: subscribed GROUP chats with NO topic assignment at all (legacy mode).
        # Chats with any topic (rem or nyaa) are excluded via seen.
        # Private chats are excluded — reminders only go to groups.
        async for doc in self.chats.find({"subscribed": True}):
            cid = doc["chat_id"]
            if cid in seen:
                continue
            # Skip private chats (positive IDs = users) — reminders are group-only
            if doc.get("chat_type") == "private" or cid > 0:
                continue
            # Skip deauthorized groups
            auth_doc = await self.auth.find_one({"chat_id": cid})
            if not auth_doc or not auth_doc.get("authorized"):
                continue
            result.append((cid, None, ChatFilter.from_doc(doc)))

        return result

    # nyaa_seen methods removed — replaced by watermark-based monitoring in NyaaScraper

    # ── Per-topic config ──────────────────────────────────────────────────
    async def get_topic_cfg(self, chat_id: int, topic_id: int) -> dict:
        """Return per-topic config dict (rem or nyaa sub-settings)."""
        doc = await self.chats.find_one({"chat_id": chat_id})
        if not doc:
            return {}
        return (doc.get("topic_cfg") or {}).get(str(topic_id), {})

    async def set_topic_cfg(self, chat_id: int, topic_id: int, cfg: dict):
        """Persist per-topic config."""
        await self.chats.update_one(
            {"chat_id": chat_id},
            {"$set": {f"topic_cfg.{topic_id}": cfg, "chat_id": chat_id}},
            upsert=True,
        )

    async def get_topic_filter(self, chat_id: int, topic_id: int) -> "ChatFilter":
        """Return per-topic schedule filter (stored inside topic_cfg)."""
        cfg = await self.get_topic_cfg(chat_id, topic_id)
        flt_doc = cfg.get("filter")
        return ChatFilter.from_doc({"filter": flt_doc}) if flt_doc else ChatFilter()

    async def set_topic_filter(self, chat_id: int, topic_id: int, flt: "ChatFilter"):
        """Persist per-topic schedule filter."""
        cfg = await self.get_topic_cfg(chat_id, topic_id)
        cfg["filter"] = flt.to_dict()
        await self.set_topic_cfg(chat_id, topic_id, cfg)

    async def reset_topic_filter(self, chat_id: int, topic_id: int):
        """Reset per-topic schedule filter to defaults."""
        cfg = await self.get_topic_cfg(chat_id, topic_id)
        cfg.pop("filter", None)
        await self.set_topic_cfg(chat_id, topic_id, cfg)

    # ── Day messages (Animes Assigned) ───────────────────────────────────
    async def set_day_message(self, chat_id: int, day: str, text: str):
        """Save a custom message for a day of the week (e.g. 'monday')."""
        await self.chats.update_one(
            {"chat_id": chat_id},
            {"$set": {f"day_msgs.{day.lower()}": text, "chat_id": chat_id}},
            upsert=True,
        )

    async def get_day_message(self, chat_id: int, day: str) -> Optional[str]:
        """Return the custom message for a day, or None."""
        doc = await self.chats.find_one({"chat_id": chat_id})
        if not doc:
            return None
        return (doc.get("day_msgs") or {}).get(day.lower())

    async def get_all_day_messages(self, chat_id: int) -> dict:
        """Return dict of {day: message} for all days that have a message set."""
        doc = await self.chats.find_one({"chat_id": chat_id})
        if not doc:
            return {}
        return doc.get("day_msgs") or {}

    async def clear_day_message(self, chat_id: int, day: str):
        """Remove the custom message for a day."""
        await self.chats.update_one(
            {"chat_id": chat_id},
            {"$unset": {f"day_msgs.{day.lower()}": ""}},
        )

    # ── User reminder CRUD ─────────────────────────────────────────────────
    async def save_reminder(self, user_id: int, day: str, time_str: str, text: str) -> str:
        """
        Upsert a reminder for (user_id, day_of_week).
        Returns the _id string of the upserted document.
        Schema: {user_id, day_of_week, time (HH:MM), message, active}
        """
        from bson import ObjectId
        result = await self.reminders.update_one(
            {"user_id": user_id, "day_of_week": day.capitalize()},
            {"$set": {
                "user_id":     user_id,
                "day_of_week": day.capitalize(),
                "time":        time_str,   # "HH:MM"
                "message":     text,
                "active":      True,
            }},
            upsert=True,
        )
        if result.upserted_id:
            return str(result.upserted_id)
        doc = await self.reminders.find_one(
            {"user_id": user_id, "day_of_week": day.capitalize()}
        )
        return str(doc["_id"]) if doc else ""

    async def get_user_reminders(self, user_id: int) -> list:
        """Return all active reminders for a user."""
        return [
            doc async for doc in
            self.reminders.find({"user_id": user_id, "active": True})
        ]

    async def delete_reminder(self, user_id: int, day: str):
        """Remove the reminder for a specific day."""
        await self.reminders.delete_one(
            {"user_id": user_id, "day_of_week": day.capitalize()}
        )

    async def get_all_active_reminders(self) -> list:
        """Return all active reminders (used by scheduler on startup)."""
        return [doc async for doc in self.reminders.find({"active": True})]

    # ── SaveMonday CRUD ───────────────────────────────────────────────────
    async def save_monday(self, chat_id: int, text: str):
        """Save a Monday message for a chat. Overwrites any existing one."""
        await self.savemonday.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, "text": text, "saved_at": datetime.now(timezone.utc)}},
            upsert=True,
        )

    async def get_monday(self, chat_id: int) -> Optional[str]:
        """Return the saved Monday message for a chat, or None."""
        doc = await self.savemonday.find_one({"chat_id": chat_id})
        return doc.get("text") if doc else None

    # ── AFK CRUD ──────────────────────────────────────────────────────────
    async def set_afk(self, user_id: int, message: str):
        """Set a user as AFK with a reason and timestamp."""
        await self.afk_users.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "message": message, "since": datetime.now(timezone.utc)}},
            upsert=True,
        )

    async def remove_afk(self, user_id: int):
        """Remove AFK status for a user."""
        await self.afk_users.delete_one({"user_id": user_id})

    async def get_afk(self, user_id: int) -> Optional[dict]:
        """Return AFK doc for a user, or None if not AFK."""
        return await self.afk_users.find_one({"user_id": user_id})

    # ── Mode / state reset ─────────────────────────────────────────────────
    async def clear_all_modes_and_topics(self, chat_id: int):
        """
        /mode_clear — wipe all saved topics, modes, per-topic configs,
        and day messages for a chat. Resets to factory state.
        """
        await self.chats.update_one(
            {"chat_id": chat_id},
            {"$unset": {
                "topics":    "",
                "topic_cfg": "",
                "day_msgs":  "",
                "filter":    "",
            }},
        )


class NyaaScraper:
    """
    Polls three 1080p torrent sources:
      1. varyg1001  — AnimeTosho JSON (search VARYG 1080, filter -VARYG in title)
      2. ToonsHub   — AnimeTosho JSON (search [ToonsHub] 1080)
      3. SubsPlease — subsplease.org/rss (their own RSS, always reliable)

    nyaa.si is permanently blocked on this server's IP range (Cloudflare 504).
    AnimeTosho mirrors all nyaa.si releases with ~5-15 min indexing delay.
    Uses pubDate watermarking in MongoDB. First run seeds silently, no backlog.
    """

    # AnimeTosho JSON search API
    ANIMETOSHO_JSON = "https://feed.animetosho.org/json"
    ANIMETOSHO_SOURCES = {
        "varyg1001": {"q": "VARYG 1080",     "filter": lambda t: "-VARYG" in t.upper() and "1080" in t},
        "ToonsHub":  {"q": "[ToonsHub] 1080", "filter": lambda t: "[ToonsHub]" in t and "1080" in t},
    }
    # SubsPlease own RSS
    SUBSPLEASE_RSS = "https://subsplease.org/rss/?t&r=1080"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,application/rss+xml,application/xml,*/*",
    }

    def __init__(self, db: "Database"):
        self.db      = db
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=30,
                headers=self.HEADERS,
                follow_redirects=True,
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── Watermark helpers (stored in MongoDB, never wiped by /reload) ─────
    async def _get_watermark(self, source: str) -> Optional[datetime]:
        """Return last-seen pubDate for a source, or None if never set."""
        doc = await self.db.db["nyaa_watermark"].find_one({"_id": source})
        if not doc:
            return None
        ts = doc.get("watermark")
        if isinstance(ts, datetime):
            return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
        return None

    async def _set_watermark(self, source: str, dt: datetime):
        """Persist the pubDate watermark for a source."""
        aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        await self.db.db["nyaa_watermark"].update_one(
            {"_id": source},
            {"$set": {"watermark": aware}},
            upsert=True,
        )

    # ── Main interface ────────────────────────────────────────────────────
    async def fetch_new(self) -> list:
        """
        Poll all sources and return entries published after the stored watermark.
        First-run: seeds watermark silently so no backlog is sent on restart.
        """
        new_entries: list = []

        # varyg1001 + ToonsHub via AnimeTosho JSON
        for source, cfg in self.ANIMETOSHO_SOURCES.items():
            entries = await self._fetch_animetosho(source, cfg["q"], cfg["filter"])
            if entries:
                new_entries.extend(await self._filter_new(source, entries))

        # SubsPlease via their own RSS
        sp_entries = await self._fetch_subsplease_rss()
        if sp_entries:
            new_entries.extend(await self._filter_new("subsplease", sp_entries))

        new_entries.sort(key=lambda e: e["pub_dt"])
        return new_entries

    async def _filter_new(self, source: str, entries: list) -> list:
        """Apply watermark logic: return only entries newer than watermark, update it."""
        watermark = await self._get_watermark(source)
        newest_dt = max(e["pub_dt"] for e in entries)
        if watermark is None:
            await self._set_watermark(source, newest_dt)
            logger.info("Nyaa [%s] seeded watermark at %s", source, newest_dt)
            return []
        fresh = [e for e in entries if e["pub_dt"] > watermark]
        logger.info(
            "Nyaa [%s] %d total / %d new (watermark=%s newest=%s)",
            source, len(entries), len(fresh),
            watermark.strftime("%d %b %H:%M"),
            newest_dt.strftime("%d %b %H:%M"),
        )
        if fresh:
            await self._set_watermark(source, newest_dt)
        return fresh

    async def _fetch_animetosho(self, source: str, q: str, title_filter) -> list:
        """Fetch entries from AnimeTosho JSON search and apply title filter."""
        params = {"t": "search", "q": q, "limit": 100}
        for attempt in range(3):
            try:
                r = await self.client.get(self.ANIMETOSHO_JSON, params=params)
                if r.status_code == 429:
                    logger.warning("AnimeTosho [%s] rate-limited, waiting 30s", source)
                    await asyncio.sleep(30)
                    continue
                if r.status_code != 200 or not r.text.strip():
                    logger.warning("AnimeTosho [%s] status %d", source, r.status_code)
                    if attempt < 2:
                        await asyncio.sleep(10)
                    continue
                data = r.json()
                if not isinstance(data, list):
                    data = data.get("items", []) if isinstance(data, dict) else []
                entries = []
                for item in data:
                    title = item.get("title", "")
                    if not title or not title_filter(title):
                        continue
                    ts = item.get("timestamp") or item.get("date") or 0
                    try:
                        pub_dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    except Exception:
                        pub_dt = datetime.now(timezone.utc)
                    nyaa_id     = item.get("nyaa_id") or item.get("id", "")
                    view_url    = item.get("link") or (f"https://nyaa.si/view/{nyaa_id}" if nyaa_id else "")
                    torrent_url = item.get("torrent_url") or (f"https://nyaa.si/download/{nyaa_id}.torrent" if nyaa_id else "")
                    size_b = item.get("total_size") or item.get("size") or 0
                    try:
                        size = f"{int(size_b) / (1024**3):.2f} GiB" if size_b else "?"
                    except Exception:
                        size = "?"
                    entries.append({
                        "id":          view_url or title,
                        "title":       title,
                        "torrent_url": torrent_url,
                        "view_url":    view_url,
                        "size":        size,
                        "uploader":    source,
                        "pub_dt":      pub_dt,
                    })
                logger.info("AnimeTosho [%s] got %d entries", source, len(entries))
                return entries
            except Exception as exc:
                logger.warning("AnimeTosho [%s] attempt %d error: %s", source, attempt + 1, exc)
                if attempt < 2:
                    await asyncio.sleep(10)
        return []



    async def _fetch_subsplease_rss(self) -> list:
        """Fetch SubsPlease 1080p torrent+magnet RSS feeds from subsplease.org."""
        torrent_url_rss = "https://subsplease.org/rss/?t&r=1080"
        magnet_url_rss  = "https://subsplease.org/rss/?r=1080"

        async def _get(url):
            for attempt in range(3):
                try:
                    r = await self.client.get(url)
                    if r.status_code == 200:
                        return r.text
                    logger.warning("SubsPlease RSS %s status %d", url, r.status_code)
                    if attempt < 2:
                        await asyncio.sleep(10)
                except Exception as exc:
                    logger.warning("SubsPlease RSS fetch attempt %d error: %s", attempt + 1, exc)
                    if attempt < 2:
                        await asyncio.sleep(10)
            return None

        torrent_xml, magnet_xml = await asyncio.gather(
            _get(torrent_url_rss), _get(magnet_url_rss)
        )

        from email.utils import parsedate_to_datetime
        entries = []
        magnet_map = {}

        # Parse magnet feed first to build title→magnet map
        if magnet_xml:
            try:
                try:
                    soup = BeautifulSoup(magnet_xml, "xml")
                    if not soup.find("item"):
                        raise ValueError
                except Exception:
                    soup = BeautifulSoup(magnet_xml, "html.parser")
                for item in soup.find_all("item"):
                    t = item.find("title")
                    l = item.find("link")
                    if t and l:
                        mag = l.get("href") or l.get_text(strip=True) or ""
                        if mag:
                            magnet_map[t.get_text(strip=True)] = mag
            except Exception as exc:
                logger.warning("SubsPlease magnet RSS parse error: %s", exc)

        # Parse torrent feed for main entries
        if torrent_xml:
            try:
                try:
                    soup = BeautifulSoup(torrent_xml, "xml")
                    if not soup.find("item"):
                        raise ValueError
                except Exception:
                    soup = BeautifulSoup(torrent_xml, "html.parser")
                for item in soup.find_all("item"):
                    title_tag = item.find("title")
                    if not title_tag:
                        continue
                    title = title_tag.get_text(strip=True)
                    pub_tag = item.find("pubDate")
                    if not pub_tag:
                        continue
                    try:
                        pub_dt = parsedate_to_datetime(pub_tag.get_text(strip=True))
                        if pub_dt.tzinfo is None:
                            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                    except Exception:
                        continue
                    enc = item.find("enclosure")
                    torrent_dl = enc["url"] if enc and enc.get("url") else ""
                    if not torrent_dl:
                        lnk = item.find("link")
                        torrent_dl = (lnk.get("href") or lnk.get_text(strip=True)) if lnk else ""
                    guid_tag = item.find("guid")
                    uid  = guid_tag.get_text(strip=True) if guid_tag else torrent_dl
                    size = "~1.4 GiB"
                    if enc and enc.get("length"):
                        try:
                            b = int(enc["length"])
                            if b > 0:
                                size = f"{b / (1024**3):.2f} GiB"
                        except Exception:
                            pass
                    entries.append({
                        "id":          uid,
                        "title":       title,
                        "torrent_url": torrent_dl,
                        "magnet_url":  magnet_map.get(title, ""),
                        "view_url":    "https://subsplease.org/",
                        "size":        size,
                        "uploader":    "subsplease",
                        "pub_dt":      pub_dt,
                    })
            except Exception as exc:
                logger.error("SubsPlease torrent RSS parse error: %s", exc)

        logger.info("SubsPlease RSS fetched %d entries", len(entries))
        return entries

    def format_entry(e: dict) -> str:
        """Format a torrent entry for Telegram (HTML)."""
        icons = {
            "varyg1001":  "🔴",
            "ToonsHub":   "🟡",
            "subsplease": "🟢",
        }
        up_icon  = icons.get(e["uploader"], "⚪")
        pub_str  = e["pub_dt"].strftime("%d %b %Y %I:%M %p UTC") if e.get("pub_dt") else ""
        view_url = e.get("view_url", "")
        uploader = e.get("uploader", "")
        # Build download/view lines
        torrent_url = e.get("torrent_url", "")
        magnet_url  = e.get("magnet_url", "")

        if uploader == "subsplease":
            dl_line  = f"\n⬇️ <a href=\"{torrent_url}\">Download Torrent</a>" if torrent_url else ""
            dl_line += f"\n🔗 <a href=\"{magnet_url}\">Magnet Link</a>" if magnet_url else ""
            view_line = f"\n🌐 <a href=\"{view_url}\">subsplease.org</a>"
        elif view_url:
            # AnimeTosho JSON gives nyaa.si/view/<id> URLs directly
            import re as _re
            nyaa_m = _re.search(r"/view/(\d+)", view_url)
            nyaa_view = f"https://nyaa.si/view/{nyaa_m.group(1)}" if nyaa_m else view_url
            dl_line  = f"\n⬇️ <a href=\"{torrent_url}\">Download Torrent</a>" if torrent_url else ""
            view_line = f"\n🔗 <a href=\"{nyaa_view}\">View on Nyaa</a>"
        else:
            dl_line  = f"\n⬇️ <a href=\"{torrent_url}\">Download Torrent</a>" if torrent_url else ""
            view_line = ""

        return (
            f"🎌 <b>{e['title']}</b>\n"
            f"{up_icon} <code>{uploader}</code>"
            + (f" · {pub_str}" if pub_str else "")
            + f"\n📦 Size: {e['size']}"
            + dl_line
            + view_line
        )


class AnimeScheduleScraper:
    """
    Async data source for animeschedule.net schedule data.

    AUTHENTICATION
    ─────────────
    Every v3 API request requires a Bearer token.  The token is FREE — create
    an account at animeschedule.net, then go to:
        animeschedule.net/users/<your_username>/settings/api
    Copy the application token and set it as the ANIMESCHEDULE_TOKEN env var.

    DATA MODEL
    ──────────
    The /api/v3/timetables endpoint returns ONE entry per (show, airType).
    The same show appears up to three times (raw, sub, dub) each with its own
    episodeDate.  _coerce_entry() maps the single date to the correct slot and
    ScheduleProcessor.process() merges entries by (route, day) into one block.

    CACHING
    ───────
    Results cached 6 h.  On fetch failure the stale cache (ignoring TTL) is
    returned so the bot never crashes.
    """

    _API_URL = "https://animeschedule.net/api/v3/timetables"

    # Base headers — Authorization is added dynamically in the client property
    _BASE_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://animeschedule.net/",
    }

    def __init__(self, db: Database):
        self.db = db
        self._client: Optional[httpx.AsyncClient] = None

    # ── HTTP client ───────────────────────────────────────────────────────
    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            headers = dict(self._BASE_HEADERS)
            if AS_TOKEN:
                headers["Authorization"] = f"Bearer {AS_TOKEN}"
            else:
                logger.warning(
                    "ANIMESCHEDULE_TOKEN is not set. "
                    "All API requests will fail with 401. "
                    "Get a free token at: animeschedule.net/users/<username>/settings/api"
                )
            self._client = httpx.AsyncClient(
                timeout=45,
                headers=headers,
                follow_redirects=True,
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── Public interface ──────────────────────────────────────────────────
    async def get_timetable(self, year: int, week: int,
                            flt: Optional["ChatFilter"] = None) -> list:
        """
        Return a list of normalised entry dicts for the given ISO year/week.
        flt — if provided, server-side params (media-types, streams) are sent.
              The unfiltered base timetable is still cached; filtered views
              use a filter-specific cache key so each unique filter combo caches
              independently.
        Results cached 6 h.  On failure, stale cache is returned.
        """
        # Cache key v2 — includes donghua field in stored entries
        # Cache key: base (no filter) or with filter fingerprint
        if flt is None or flt.is_default():
            cache_key = f"scrape:v2:timetable:{year}:{week}"
        else:
            import hashlib
            flt_hash = hashlib.md5(
                json.dumps(flt.to_dict(), sort_keys=True).encode()
            ).hexdigest()[:8]
            cache_key = f"scrape:v2:timetable:{year}:{week}:{flt_hash}"

        cached = await self.db.get_cache(cache_key)
        if cached is not None:
            await self.db.inc_stat("cache_hits")
            logger.debug("Cache hit for %s", cache_key)
            return cached

        entries = await self._fetch_timetable(year, week, flt=flt)

        if entries:
            await self.db.set_cache(cache_key, entries, ttl=21600)
            await self.db.inc_stat("scrape_calls")
            logger.info("Scraped %d entries for week %d/%d", len(entries), week, year)
            return entries

        stale = await self.db.get_cache_ignore_ttl(cache_key)
        if stale:
            logger.warning(
                "Scrape failed for week %d/%d — using stale cache (%d entries)",
                week, year, len(stale),
            )
            await self.db.inc_stat("scrape_fallbacks")
            return stale

        logger.error("Scrape failed and no stale cache for week %d/%d", week, year)
        return []

    # ── Fetch strategies ──────────────────────────────────────────────────
    async def _fetch_timetable(self, year: int, week: int,
                               flt: Optional["ChatFilter"] = None) -> list:
        entries = await self._fetch_via_api(year, week, flt=flt)
        if entries:
            return entries
        return await self._fetch_via_html(year, week)

    async def _fetch_via_api(self, year: int, week: int,
                              flt: Optional["ChatFilter"] = None) -> list:
        """
        Call GET /api/v3/timetables (authenticated with Bearer token).

        flt — if provided, media-type/stream filters are sent as API params
              so the server filters at source (reduces response size).
              Air-type filtering is done post-fetch in ScheduleProcessor.process().

        Authentication: ANIMESCHEDULE_TOKEN env var must be set.
        """
        if not AS_TOKEN:
            logger.error(
                "ANIMESCHEDULE_TOKEN is not set — cannot fetch schedule data. "
                "Get a free token at animeschedule.net/users/<username>/settings/api"
            )
            return []

        params = {"week": week, "year": year, "tz": "UTC"}
        if flt is not None:
            params.update(flt.as_api_params())
        try:
            r = await self.client.get(self._API_URL, params=params)
            if r.status_code == 200:
                data = r.json()
                raw  = data if isinstance(data, list) else data.get("entries", [])
                if not isinstance(raw, list):
                    logger.warning("Unexpected API response shape: %s", type(raw))
                    return []
                logger.debug("API returned %d raw entries for week %d/%d", len(raw), week, year)
                entries = self._normalise_entries(raw)
                if entries:
                    return entries
                logger.warning("API returned %d items but 0 normalised — check token/response", len(raw))
            elif r.status_code == 401:
                logger.error(
                    "API returned 401 Unauthorized. "
                    "Check that ANIMESCHEDULE_TOKEN is correct and not expired. "
                    "Renew at animeschedule.net/users/<username>/settings/api"
                )
            elif r.status_code == 429:
                logger.warning("API rate-limited (429). Will retry on next cache miss.")
            else:
                logger.warning("API returned unexpected status %d", r.status_code)
        except Exception as exc:
            logger.warning("API fetch error: %s", exc)
        return []

    async def _fetch_via_html(self, year: int, week: int) -> list:
        """
        Fetch the timetable HTML page and extract JSON from either:
          • <script id="__NEXT_DATA__"> (Next.js SSR payload), or
          • Inline window.__* variable assignments.
        The parsing is offloaded to a thread pool to avoid blocking the
        event loop.
        """
        url = f"{AS_BASE_URL}/?year={year}&week={week}"
        try:
            r = await self.client.get(url)
            if r.status_code != 200:
                logger.warning("HTML page returned %d for %s", r.status_code, url)
                return []
            html = r.text
            has_next = "__NEXT_DATA__" in html
            logger.debug("HTML fetched: %d bytes, __NEXT_DATA__=%s", len(html), has_next)
            if not has_next:
                logger.warning(
                    "HTML page has no __NEXT_DATA__ — site is likely a pure SPA. "
                    "HTML fallback will return empty; stale cache will be used."
                )
            return await asyncio.get_event_loop().run_in_executor(
                None, self._parse_html, html
            )
        except Exception as exc:
            logger.warning("HTML fetch error: %s", exc)
            return []

    # ── HTML parsing (runs in executor) ──────────────────────────────────
    @staticmethod
    def _parse_html(html: str) -> list:
        entries = AnimeScheduleScraper._extract_next_data(html)
        if entries:
            return entries
        return AnimeScheduleScraper._extract_inline_json(html)

    @staticmethod
    def _extract_next_data(html: str) -> list:
        """Parse the Next.js hydration payload embedded in every SSR page."""
        try:
            m = re.search(
                r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
                html,
                re.DOTALL,
            )
            if not m:
                return []
            payload    = json.loads(m.group(1))
            page_props = payload.get("props", {}).get("pageProps", {})

            # Try flat list keys
            for key in ("timetable", "schedule", "anime", "animes", "entries", "data"):
                val = page_props.get(key)
                if isinstance(val, list) and val:
                    logger.debug(
                        "Found %d entries via __NEXT_DATA__[%s]", len(val), key
                    )
                    return AnimeScheduleScraper._normalise_entries(val)
                # Might be a day-keyed mapping {monday: [...], ...}
                if isinstance(val, dict):
                    flat: list = []
                    for day_list in val.values():
                        if isinstance(day_list, list):
                            flat.extend(day_list)
                    if flat:
                        logger.debug(
                            "Found %d entries via __NEXT_DATA__[%s] (day-keyed)",
                            len(flat), key,
                        )
                        return AnimeScheduleScraper._normalise_entries(flat)
        except Exception as exc:
            logger.debug("__NEXT_DATA__ parse error: %s", exc)
        return []

    @staticmethod
    def _extract_inline_json(html: str) -> list:
        """
        Last resort: scan all <script> blocks for JSON arrays that look like
        timetable data (objects containing 'route' or 'episodeDate').
        """
        try:
            blocks = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
            for block in blocks:
                block = block.strip()
                m = re.search(
                    r"(?:window|self)\.__\w+\s*=\s*(\[.*?\]);?$",
                    block,
                    re.DOTALL,
                )
                if not m:
                    continue
                try:
                    data = json.loads(m.group(1))
                    if isinstance(data, list) and data:
                        sample = data[0] if isinstance(data[0], dict) else {}
                        if "route" in sample or "episodeDate" in sample:
                            logger.debug(
                                "Found %d entries via inline JSON scan", len(data)
                            )
                            return AnimeScheduleScraper._normalise_entries(data)
                except json.JSONDecodeError:
                    continue
        except Exception as exc:
            logger.debug("Inline JSON scan error: %s", exc)
        return []

    # ── Normalisation ─────────────────────────────────────────────────────
    @staticmethod
    def _normalise_entries(raw: list) -> list:
        result = []
        for e in raw:
            if not isinstance(e, dict):
                continue
            entry = AnimeScheduleScraper._coerce_entry(e)
            if entry:
                result.append(entry)
        return result

    @staticmethod
    def _coerce_entry(e: dict) -> Optional[dict]:
        """
        Produce a uniform internal entry dict from a raw API or HTML payload.

        IMPORTANT — animeschedule.net v3 API schema:
          Each timetable entry represents ONE (show, airType) pair.
          Fields (PascalCase from the API):
            Route, Title, AirType ("raw"|"sub"|"dub"),
            EpisodeDate (the single air datetime for this airType),
            EpisodeNumber, Episodes, ImageVersionRoute, Likes.
          There are NO SubDate / DubDate fields on timetable entries.

        We map the single EpisodeDate to the correct version slot based on
        AirType so that ScheduleProcessor.process() can merge multiple entries
        that share the same Route into one unified anime block.

        Returns None if the entry lacks both route and title.
        """

        def pick(*keys):
            for k in keys:
                for variant in (k, k.lower(), k[0].upper() + k[1:]):
                    v = e.get(variant)
                    if v is not None:
                        return v
            return None

        route = pick("route", "Route", "slug")

        # AS API v3 provides separate English / Romaji / Title fields.
        # Priority: English (official EN) → Romaji → Title (display fallback) → route slug.
        english_title = (pick("english", "English") or "").strip()
        romaji_title  = (pick("romaji",  "Romaji")  or "").strip()
        display_title = (pick("title",   "Title")   or "").strip()

        # Best display name: English > Romaji > Title field > route
        title = english_title or romaji_title or display_title or route
        # Canonical romaji for search/matching (used by TitleResolver fallback)
        romaji = romaji_title or display_title or english_title or route

        if not route and not title:
            return None

        slug = route or re.sub(r"[^a-z0-9-]", "-", (title or "").lower()).strip("-")

        # Determine airType — normalise to lowercase "raw"/"sub"/"dub"
        air_type = str(pick("airType", "AirType", "air_type") or "raw").lower()
        if air_type not in ("raw", "sub", "dub"):
            air_type = "raw"

        # The single EpisodeDate for this entry
        ep_date_raw = pick("episodeDate", "EpisodeDate", "airingAt", "AiringAt") or ""
        if ep_date_raw == _AS_NULL_DT:
            ep_date_raw = ""

        # Map the air datetime to the correct version slot
        raw_date = ep_date_raw if air_type == "raw" else ""
        sub_date = ep_date_raw if air_type == "sub" else ""
        dub_date = ep_date_raw if air_type == "dub" else ""

        image_route = pick("imageVersionRoute", "ImageVersionRoute", "thumbnail") or ""
        if image_route and not image_route.startswith("http"):
            image_route = AS_CDN_BASE + image_route

        # Preserve donghua flag — animeschedule.net uses "Donghua" boolean or
        # a "mediaCategoryId" / "mediaCategory" of "donghua".
        # We also check genres list and title heuristics as fallback.
        raw_genres = pick("genres", "Genres", "genre", "Genre") or []
        if isinstance(raw_genres, str):
            raw_genres = [raw_genres]
        genres_lower = [g.lower() for g in raw_genres if isinstance(g, str)]

        is_donghua = bool(
            pick("donghua", "Donghua", "isDonghua", "IsDonghua",
                 "is_donghua", "Chinese", "chinese")
            or pick("mediaCategoryId", "MediaCategoryId") == 3   # animeschedule uses 3 for donghua
            or (pick("mediaCategory", "MediaCategory") or "").lower() == "donghua"
            or "chinese animation" in genres_lower
            or "donghua" in genres_lower
        )

        return {
            "route":             slug,
            "title":             title,
            "romaji":            romaji,          # canonical romaji for TitleResolver fallback
            "airType":           air_type,
            "episodeNumber":     pick("episodeNumber", "EpisodeNumber", "episode") or 0,
            "episodes":          pick("episodes", "Episodes", "totalEpisodes")     or 0,
            "episodeDate":       raw_date,
            "subDate":           sub_date,
            "dubDate":           dub_date,
            "imageVersionRoute": image_route,
            "likes":             pick("likes", "Likes", "popularity", "Popularity") or 0,
            "genres":            raw_genres,
            "donghua":           is_donghua,
        }


# ── Season Scraper ───────────────────────────────────────────────────────────
class SeasonScraper:
    """
    Async scraper for MyAnimeList seasonal TV (New) titles, enriched with
    English names from AniList.

    Usage:
        scraper = SeasonScraper(db)
        titles  = await scraper.get_season(2025, "spring")
        # → ["Solo Leveling | ソロ・レベリング", ...]

    Results are cached in MongoDB for 24 h (season data rarely changes).
    Each title is returned as:
        "<English or Romaji> | <MAL title>"
    """

    VALID_SEASONS = ("winter", "spring", "summer", "fall")
    CACHE_TTL     = 86400   # 24 hours

    _MAL_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    _ANILIST_Q = """
    query ($search: String) {
      Media(search: $search, type: ANIME) {
        title { romaji english }
      }
    }
    """

    def __init__(self, db: "Database"):
        self.db = db
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=20,
                headers=self._MAL_HEADERS,
                follow_redirects=True,
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── Public ────────────────────────────────────────────────────────────
    async def get_season(self, year: int, season: str) -> list:
        """
        Return list of "English Title | MAL Title" strings for TV (New) anime.
        Cached for 24 h.  Returns [] on failure.
        """
        season = season.lower().strip()
        if season not in self.VALID_SEASONS:
            return []

        cache_key = f"season:{year}:{season}"
        cached = await self.db.get_cache(cache_key)
        if cached is not None:
            await self.db.inc_stat("season_cache_hits")
            logger.debug("Season cache hit: %s", cache_key)
            return cached

        mal_titles = await self._fetch_mal_titles(year, season)
        if not mal_titles:
            return []

        # Enrich with English names from AniList concurrently (batched)
        enriched = await self._enrich_titles(mal_titles)

        await self.db.set_cache(cache_key, enriched, ttl=self.CACHE_TTL)
        await self.db.inc_stat("season_scrape_calls")
        logger.info("Season scraped: %d titles for %s %d", len(enriched), season, year)
        return enriched

    # ── MAL scrape ────────────────────────────────────────────────────────
    async def _fetch_mal_titles(self, year: int, season: str) -> list:
        url = f"https://myanimelist.net/anime/season/{year}/{season}"
        try:
            r = await self.client.get(url)
            if r.status_code != 200:
                logger.warning("MAL season page returned %d", r.status_code)
                return []
            return await asyncio.get_event_loop().run_in_executor(
                None, self._parse_mal_html, r.text
            )
        except Exception as exc:
            logger.warning("MAL fetch error: %s", exc)
            return []

    @staticmethod
    def _parse_mal_html(html: str) -> list:
        """Extract TV (New) anime titles from MAL season page."""
        try:
            soup = BeautifulSoup(html, "html.parser")
            seasonal = soup.find("div", class_="seasonal-anime-list")
            if not seasonal:
                return []

            tv_new_header = seasonal.find(
                "div", class_="anime-header",
                string=re.compile(r"TV .New.", re.I)
            )
            if not tv_new_header:
                return []

            titles = []
            for anime in tv_new_header.find_all_next("div", class_="seasonal-anime", limit=200):
                prev_hdr = anime.find_previous_sibling("div", class_="anime-header")
                if prev_hdr and not re.search(r"TV \(New\)", prev_hdr.text, re.I):
                    break
                tag = anime.select_one("h2.h2_anime_title a.link-title")
                if tag:
                    titles.append(tag.text.strip())
            return titles
        except Exception as exc:
            logger.error("MAL parse error: %s", exc)
            return []

    # ── AniList enrichment ────────────────────────────────────────────────
    async def _enrich_titles(self, mal_titles: list) -> list:
        """
        Fetch English titles from AniList for each MAL title.
        Uses asyncio.gather with a semaphore to limit concurrency to 5.
        Adds a small delay between batches to avoid rate-limiting.
        """
        sem = asyncio.Semaphore(5)

        async def fetch_one(mal_title: str) -> str:
            async with sem:
                eng = await self._anilist_english(mal_title)
                await asyncio.sleep(0.3)   # gentle rate-limit
                if eng and eng.lower() != mal_title.lower():
                    return f"{eng} | {mal_title}"
                return mal_title

        results = await asyncio.gather(*[fetch_one(t) for t in mal_titles])
        return list(results)

    async def _anilist_english(self, search: str) -> Optional[str]:
        try:
            r = await self.client.post(
                "https://graphql.anilist.co",
                json={"query": self._ANILIST_Q, "variables": {"search": search}},
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            if r.status_code == 200:
                data = r.json()
                t = data.get("data", {}).get("Media", {}).get("title", {})
                return t.get("english") or t.get("romaji")
        except Exception:
            pass
        return None

    # ── Pagination helper ─────────────────────────────────────────────────
    @staticmethod
    def paginate(titles: list, year: int, season: str, page: int = 0,
                 per_page: int = 15) -> tuple:
        """
        Return (text, total_pages) for the given page (0-indexed).
        Each page shows up to per_page titles as a numbered list.
        """
        total       = len(titles)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page        = max(0, min(page, total_pages - 1))

        start  = page * per_page
        chunk  = titles[start : start + per_page]

        header     = f"\U0001f4cb <b>{season.capitalize()} {year} \u2014 TV New Anime</b>"
        sub_header = f"<i>{total} titles total</i>"

        blocks = []
        for i, title in enumerate(chunk, start + 1):
            display = title.split(" | ")[0] if " | " in title else title
            blocks.append(f"<blockquote>{i}. {display}</blockquote>")

        text = header + "\n" + sub_header + "\n\n" + "\n".join(blocks)
        return text, total_pages



# ── AniList API ───────────────────────────────────────────────────────────────
class AniListAPI:
    URL = ANILIST_API

    # Single best-match query (used for detailed card)
    _ANIME_Q = """query($s:String){Media(search:$s,type:ANIME,sort:SEARCH_MATCH){
        id title{romaji english native}
        description(asHtml:false) episodes duration status format source
        averageScore genres
        coverImage{extraLarge large}
        bannerImage siteUrl
        season seasonYear
        startDate{year month day}
        endDate{year month day}
        trailer{site id}
        characters(sort:ROLE,perPage:6){nodes{name{full}image{medium}}}
        studios(isMain:true){nodes{name}}}}"""
    _MANGA_Q = """query($s:String){Media(search:$s,type:MANGA,sort:SEARCH_MATCH){
        id title{romaji english native}
        description(asHtml:false) chapters volumes status format source
        averageScore genres
        coverImage{extraLarge large}
        siteUrl
        startDate{year month day}
        endDate{year month day}
        trailer{site id}
        characters(sort:ROLE,perPage:6){nodes{name{full}image{medium}}}
        staff(sort:RELEVANCE){nodes{name{full}}}}}"""

    # Multi-result search — returns up to 8 matches with id, title, format, status
    _ANIME_SEARCH_Q = """query($s:String){Page(perPage:8){
        media(search:$s,type:ANIME,sort:SEARCH_MATCH){
            id title{romaji english} format status episodes
            coverImage{extraLarge large medium} siteUrl startDate{year}}}}"""
    _MANGA_SEARCH_Q = """query($s:String){Page(perPage:8){
        media(search:$s,type:MANGA,sort:SEARCH_MATCH){
            id title{romaji english} format status chapters volumes
            coverImage{extraLarge large medium} siteUrl startDate{year}}}}"""

    # Detail by ID (for when user picks from inline list)
    _ANIME_BY_ID = """query($id:Int){Media(id:$id,type:ANIME){
        id title{romaji english native}
        description(asHtml:false) episodes duration status format source
        averageScore genres
        coverImage{extraLarge large}
        bannerImage siteUrl
        season seasonYear
        startDate{year month day}
        endDate{year month day}
        trailer{site id}
        characters(sort:ROLE,perPage:6){nodes{name{full}image{medium}}}
        studios(isMain:true){nodes{name}}}}"""
    _MANGA_BY_ID = """query($id:Int){Media(id:$id,type:MANGA){
        id title{romaji english native}
        description(asHtml:false) chapters volumes status format source
        averageScore genres
        coverImage{extraLarge large}
        siteUrl
        startDate{year month day}
        endDate{year month day}
        trailer{site id}
        characters(sort:ROLE,perPage:6){nodes{name{full}image{medium}}}
        staff(sort:RELEVANCE){nodes{name{full}}}}}"""

    def __init__(self, db: Database):
        self.db = db
        self._client: Optional[httpx.AsyncClient] = None
        self._cache: dict = {}   # in-memory short-TTL cache {key: (value, expires)}

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=30,
                headers={
                    "Content-Type": "application/json",
                    "Accept":       "application/json",
                },
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    def _mem_get(self, key: str) -> Any:
        entry = self._cache.get(key)
        if entry and datetime.now(timezone.utc) < entry[1]:
            return entry[0]
        return None

    def _mem_set(self, key: str, value: Any, ttl: int = 600):
        self._cache[key] = (value, datetime.now(timezone.utc) + timedelta(seconds=ttl))

    async def _gql(self, query: str, search: str) -> Optional[dict]:
        cache_key = f"al:gql:{hash(query+search)}"
        cached = self._mem_get(cache_key)
        if cached is not None:
            return cached
        try:
            r = await self.client.post(
                self.URL,
                json={"query": query, "variables": {"s": search}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            result = data.get("data", {}).get("Media") if "errors" not in data else None
            if result:
                self._mem_set(cache_key, result)
            return result
        except Exception as exc:
            logger.error("AniList error: %s", exc)
            return None

    async def search_anime(self, q: str):
        return await self._gql(self._ANIME_Q, q)

    async def search_manga(self, q: str):
        return await self._gql(self._MANGA_Q, q)

    async def search_anime_list(self, q: str) -> list:
        """Return up to 8 search results for inline buttons."""
        cache_key = f"al:list:anime:{q}"
        cached = self._mem_get(cache_key)
        if cached is not None:
            return cached
        try:
            r = await self.client.post(
                self.URL,
                json={"query": self._ANIME_SEARCH_Q, "variables": {"s": q}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            result = (data.get("data", {}).get("Page", {}).get("media") or []) if "errors" not in data else []
            self._mem_set(cache_key, result)
            return result
        except Exception as exc:
            logger.error("AniList search list error: %s", exc)
            return []

    async def search_manga_list(self, q: str) -> list:
        """Return up to 8 manga search results for inline buttons."""
        cache_key = f"al:list:manga:{q}"
        cached = self._mem_get(cache_key)
        if cached is not None:
            return cached
        try:
            r = await self.client.post(
                self.URL,
                json={"query": self._MANGA_SEARCH_Q, "variables": {"s": q}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            result = (data.get("data", {}).get("Page", {}).get("media") or []) if "errors" not in data else []
            self._mem_set(cache_key, result)
            return result
        except Exception as exc:
            logger.error("AniList manga search list error: %s", exc)
            return []

    async def get_anime_by_id(self, media_id: int) -> Optional[dict]:
        """Fetch full anime detail by AniList media ID."""
        cache_key = f"al:id:anime:{media_id}"
        cached = self._mem_get(cache_key)
        if cached is not None:
            return cached
        try:
            r = await self.client.post(
                self.URL,
                json={"query": self._ANIME_BY_ID, "variables": {"id": media_id}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            result = data.get("data", {}).get("Media") if "errors" not in data else None
            if result:
                self._mem_set(cache_key, result, ttl=300)
            return result
        except Exception as exc:
            logger.error("AniList get_anime_by_id error: %s", exc)
            return None

    async def get_manga_by_id(self, media_id: int) -> Optional[dict]:
        """Fetch full manga detail by AniList media ID."""
        cache_key = f"al:id:manga:{media_id}"
        cached = self._mem_get(cache_key)
        if cached is not None:
            return cached
        try:
            r = await self.client.post(
                self.URL,
                json={"query": self._MANGA_BY_ID, "variables": {"id": media_id}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            result = data.get("data", {}).get("Media") if "errors" not in data else None
            if result:
                self._mem_set(cache_key, result, ttl=300)
            return result
        except Exception as exc:
            logger.error("AniList get_manga_by_id error: %s", exc)
            return None

    # ── Relations ─────────────────────────────────────────────────────────
    _RELATIONS_Q = """query($id:Int){Media(id:$id){
        id title{romaji english}
        relations{edges{relationType node{
            id type title{romaji english} format status episodes
            averageScore startDate{year} siteUrl
        }}}}}"""

    async def _get_relations_raw(self, media_id: int) -> Optional[dict]:
        """Fetch a single media entry with its direct relations."""
        try:
            r = await self.client.post(
                self.URL,
                json={"query": self._RELATIONS_Q, "variables": {"id": media_id}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            return data.get("data", {}).get("Media") if "errors" not in data else None
        except Exception as exc:
            logger.error("AniList relations error: %s", exc)
            return None

    async def get_full_relations(self, media_id: int) -> dict:
        """
        Walk the PREQUEL/SEQUEL chain recursively and collect all other relations.
        Returns:
          {
            "root":     {id, title, ...},
            "timeline": [ordered list from oldest prequel to newest sequel],
            "other":    {relationType: [nodes], ...}  (side stories, adaptations, etc.)
          }
        """
        visited: set = set()
        timeline: list = []
        other: dict = {}

        async def walk(mid: int):
            if mid in visited:
                return
            visited.add(mid)
            data = await self._get_relations_raw(mid)
            if not data:
                return
            edges = (data.get("relations") or {}).get("edges") or []
            for edge in edges:
                rel_type = edge.get("relationType", "")
                node     = edge.get("node") or {}
                nid      = node.get("id")
                if not nid or node.get("type") not in (None, "ANIME", "MANGA"):
                    continue
                if rel_type in ("PREQUEL", "SEQUEL"):
                    if nid not in visited:
                        timeline.append((rel_type, node))
                        await walk(nid)
                elif rel_type in ("SIDE_STORY", "SPIN_OFF", "ADAPTATION",
                                  "ALTERNATIVE", "SUMMARY", "PARENT", "CHARACTER"):
                    other.setdefault(rel_type, [])
                    if not any(n.get("id") == nid for n in other[rel_type]):
                        other[rel_type].append(node)

        # Seed with the root entry
        root_data = await self._get_relations_raw(media_id)
        if not root_data:
            return {"root": None, "timeline": [], "other": {}}

        visited.add(media_id)
        edges = (root_data.get("relations") or {}).get("edges") or []
        for edge in edges:
            rel_type = edge.get("relationType", "")
            node     = edge.get("node") or {}
            nid      = node.get("id")
            if not nid:
                continue
            if rel_type in ("PREQUEL", "SEQUEL"):
                if nid not in visited:
                    timeline.append((rel_type, node))
                    await walk(nid)
            elif rel_type in ("SIDE_STORY", "SPIN_OFF", "ADAPTATION",
                              "ALTERNATIVE", "SUMMARY", "PARENT", "CHARACTER"):
                other.setdefault(rel_type, [])
                if not any(n.get("id") == nid for n in other[rel_type]):
                    other[rel_type].append(node)

        return {"root": root_data, "timeline": timeline, "other": other}


# ── Title Resolver ────────────────────────────────────────────────────────────
class TitleResolver:
    """
    Resolves anime romaji/display titles → official English titles via AniList.

    animeschedule.net already supplies English titles directly in its API
    response (English field).  This resolver is a fallback-only layer used
    when the AS API returns no English title for an entry.

    Resolution order:
      1. In-memory dict  (instant, process lifetime)
      2. MongoDB cache   (30-day TTL — survives restarts)
      3. AniList GraphQL (free, no key, best EN title coverage)
      4. Original title  (last resort — never raises)

    Jikan/MAL is intentionally excluded — it returns HTTP 500 for many
    valid queries and adds latency without reliability.
    """

    ANILIST_URL  = "https://graphql.anilist.co"
    CACHE_TTL    = 60 * 60 * 24 * 30   # 30 days
    CACHE_PREFIX = "title_en:"

    _ANILIST_Q = """
    query ($search: String) {
      Media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
        title { romaji english }
      }
    }
    """

    def __init__(self, db: "Database"):
        self.db      = db
        self._mem:   dict = {}
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def _http(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=10,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                follow_redirects=True,
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── Public ────────────────────────────────────────────────────────────
    async def resolve(self, title: str) -> str:
        """Return English title for *title*, falling back to *title* itself."""
        if not title or not title.strip():
            return title
        title = title.strip()
        key   = title.lower()

        if key in self._mem:
            return self._mem[key]

        cached = await self.db.get_cache(self.CACHE_PREFIX + key)
        if cached:
            self._mem[key] = cached
            return cached

        english = await self._from_anilist(title)
        result  = english or title
        self._mem[key] = result
        await self.db.set_cache(self.CACHE_PREFIX + key, result, ttl=self.CACHE_TTL)
        logger.debug(
            "TitleResolver: '%s' → '%s' [%s]",
            title, result, "AniList" if english else "fallback",
        )
        return result

    async def resolve_many(self, titles: list) -> dict:
        """Concurrently resolve a list. Returns {title: english_or_title}."""
        # Limit concurrency to avoid hammering AniList
        sem = asyncio.Semaphore(5)
        async def _one(t):
            async with sem:
                r = await self.resolve(t)
                await asyncio.sleep(0.15)   # gentle pacing
                return r
        results = await asyncio.gather(*[_one(t) for t in titles])
        return dict(zip(titles, results))

    # ── AniList ───────────────────────────────────────────────────────────
    @staticmethod
    def _norm(text: str) -> str:
        return re.sub(r"[^a-z0-9]", "", text.lower())

    @staticmethod
    def _loose_match(a: str, b: str, threshold: float = 0.72) -> bool:
        na, nb = TitleResolver._norm(a), TitleResolver._norm(b)
        if not na or not nb:
            return False
        if na in nb or nb in na:
            return True
        def bigrams(s):
            return {s[i:i+2] for i in range(len(s) - 1)} if len(s) >= 2 else set()
        ba, bb = bigrams(na), bigrams(nb)
        if not ba or not bb:
            return False
        return len(ba & bb) / len(ba | bb) >= threshold

    async def _from_anilist(self, title: str) -> Optional[str]:
        try:
            r = await self._http.post(
                self.ANILIST_URL,
                json={"query": self._ANILIST_Q, "variables": {"search": title}},
            )
            r.raise_for_status()
            data    = r.json()
            media   = (data.get("data") or {}).get("Media") or {}
            titles  = media.get("title") or {}
            english = (titles.get("english") or "").strip()
            cand_r  = (titles.get("romaji")  or "").strip()
            if not english:
                return None
            if cand_r and not self._loose_match(title, cand_r):
                logger.debug(
                    "TitleResolver [AniList] match rejected: '%s' vs '%s'",
                    title, cand_r,
                )
                return None
            return english
        except Exception as exc:
            logger.debug("TitleResolver [AniList] error for '%s': %s", title, exc)
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_dt(s: str) -> Optional[datetime]:
    """
    Parse an ISO-8601 string into a TZ-aware datetime in the configured TZ.
    Returns None for empty strings or the animeschedule.net null sentinel.
    """
    if not s or s == _AS_NULL_DT:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(TZ)
    except Exception:
        return None


def fmt_time(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.strftime("%I:%M %p")


def current_week_year():
    iso = datetime.now(TZ).isocalendar()
    return iso.year, iso.week


def clean_html(text: str, limit: int = 900) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    for old, new in [
        ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
        ("&#039;", "'"), ("&quot;", '"'),
    ]:
        text = text.replace(old, new)
    text = text.strip()
    return text[: limit - 1] + "\u2026" if len(text) > limit else text


def al_status(s: str) -> str:
    return {
        "FINISHED":         "Finished",
        "RELEASING":        "\U0001f7e2 Releasing",
        "NOT_YET_RELEASED": "Not Yet Released",
        "CANCELLED":        "Cancelled",
        "HIATUS":           "On Hiatus",
    }.get(s or "", s or "Unknown")


def al_date(d: Optional[dict]) -> str:
    """Format AniList date dict {year, month, day} as YYYY-M-D (no leading zeros)."""
    if not d or not d.get("year"):
        return "N/A"
    y = d.get("year") or 0
    m = d.get("month") or 0
    day = d.get("day") or 0
    if y and m and day:
        return f"{y}-{m}-{day}"
    elif y and m:
        return f"{y}-{m}"
    return str(y) if y else "N/A"


def al_cover(r: dict) -> Optional[str]:
    """Return best available cover image URL from AniList media dict."""
    cover = r.get("coverImage") or {}
    return cover.get("extraLarge") or cover.get("large") or cover.get("medium")


def al_trailer_url(r: dict) -> Optional[str]:
    """Build trailer URL from AniList trailer dict."""
    t = r.get("trailer") or {}
    site = t.get("site", "")
    tid  = t.get("id", "")
    if not site or not tid:
        return None
    if site == "youtube":
        return f"https://www.youtube.com/watch?v={tid}"
    if site == "dailymotion":
        return f"https://www.dailymotion.com/video/{tid}"
    return None


def strip_html(text: str) -> str:
    """Remove all HTML tags and decode common entities."""
    text = re.sub(r"<[^>]+>", "", text or "")
    for old, new in [
        ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
        ("&#039;", "'"), ("&quot;", '"'), ("&nbsp;", " "),
    ]:
        text = text.replace(old, new)
    return text.strip()


def truncate_synopsis(text: str, media_id: int, media_type: str,
                      limit: int = TG_CAPTION_LIMIT) -> str:
    """
    Truncate synopsis to fit in caption.  If truncated, append a 'read more'
    hyperlink pointing to the AniList page.  The truncation headroom accounts
    for the hyperlink text itself so the final string never exceeds *limit*.
    """
    if not text:
        return "N/A"
    url        = f"https://anilist.co/{media_type}/{media_id}"
    read_more  = f'...<a href="{url}">read more</a>'
    headroom   = len(read_more) + 20   # padding for surrounding caption text
    if len(text) <= limit - headroom:
        return text
    return text[:limit - headroom] + read_more


# ── Schedule Processor ────────────────────────────────────────────────────────
class ScheduleProcessor:
    """
    Converts a flat list of AnimeScheduleScraper entry dicts into a
    {DayName: [anime_dict, ...]} mapping ready for display.

    KEY DESIGN DECISION — per-version independence:
    ───────────────────────────────────────────────
    animeschedule.net returns one entry per (show, airType).  Raw, Sub, and
    Dub are often on DIFFERENT days with DIFFERENT episode numbers.
    e.g. Sub ep 8 airs Wednesday, Dub ep 6 airs Saturday.

    Each (slug, airType) pair is placed on its OWN day independently.
    When the same show has Raw and Sub on the SAME day, those two ARE merged
    into one block (key = slug+day, only same-day same-show entries combine).
    Dub on a different day appears as a separate entry on that day, showing
    only the Dub line with its own episode number and date.

    This means a show can appear on multiple days — once for Raw/Sub and
    again (later in the week) for Dub — which is correct and expected.
    """

    @staticmethod
    def process(entries: list, flt: Optional["ChatFilter"] = None) -> dict:
        """
        Build a {DayName: [anime_dict, ...]} map.

        flt — optional ChatFilter; if provided, entries for disabled air types
              are skipped entirely so they don't appear in the schedule or
              reminder jobs for this chat.

        Merge rule: entries are grouped by (slug, day, airType-group).
        Raw and Sub that fall on the same day are merged into one block.
        Dub is always kept separate per its own air day.
        """
        merged: dict = {}

        for e in entries:
            slug  = e.get("route") or e.get("slug") or e.get("title", "")
            title = e.get("title") or slug
            if not slug:
                continue

            air_type = e.get("airType", "raw").lower()

            # ── Apply chat filter: air type ──────────────────────────────
            if flt is not None and not flt.allows_air_type(air_type):
                continue

            # ── Apply chat filter: hide donghua ───────────────────────────
            is_donghua = bool(e.get("donghua"))
            if flt is not None and flt.hide_donghua and is_donghua:
                continue

            # Determine the datetime for this specific version
            ep_date_str = (
                e.get("episodeDate") or
                e.get("subDate")     or
                e.get("dubDate")     or ""
            )
            dt = parse_dt(ep_date_str)
            if not dt:
                continue

            day = dt.strftime("%A")
            if day not in DAYS_OF_WEEK:
                continue

            ep = e.get("episodeNumber") or e.get("episode") or 0
            try:
                ep_num = float(ep)
            except (TypeError, ValueError):
                ep_num = 0.0

            total_eps  = e.get("episodes") or e.get("totalEpisodes") or "?"
            popularity = e.get("likes") or e.get("popularity") or 0
            thumbnail  = e.get("imageVersionRoute") or ""

            # Dub always gets its own day-slot; raw and sub share a slot per day
            version_group = "dub" if air_type == "dub" else "raw_sub"
            key = (slug, day, version_group)

            existing = merged.get(key)
            if existing is None:
                merged[key] = {
                    "slug":        slug,
                    "title":       title,
                    "popularity":  popularity,
                    "thumbnail":   thumbnail,
                    # Raw version slot
                    "raw_time":    dt    if air_type == "raw" else None,
                    "raw_ep":      ep    if air_type == "raw" else None,
                    "raw_date":    day   if air_type == "raw" else None,
                    # Sub version slot
                    "sub_time":    dt    if air_type == "sub" else None,
                    "sub_ep":      ep    if air_type == "sub" else None,
                    "sub_date":    day   if air_type == "sub" else None,
                    # Dub version slot
                    "dub_time":    dt    if air_type == "dub" else None,
                    "dub_ep":      ep    if air_type == "dub" else None,
                    "dub_date":    day   if air_type == "dub" else None,
                    # Shared display fields — taken from whichever version is present
                    "total_eps":   total_eps,
                    "ep_num":      ep_num,   # for dedup comparisons only
                }
            else:
                # Same show, same day, same version_group — fill in any missing slot
                if air_type == "raw" and existing["raw_time"] is None:
                    existing["raw_time"] = dt
                    existing["raw_ep"]   = ep
                    existing["raw_date"] = day
                elif air_type == "sub" and existing["sub_time"] is None:
                    existing["sub_time"] = dt
                    existing["sub_ep"]   = ep
                    existing["sub_date"] = day
                elif air_type == "dub" and existing["dub_time"] is None:
                    existing["dub_time"] = dt
                    existing["dub_ep"]   = ep
                    existing["dub_date"] = day
                if ep_num > existing["ep_num"]:
                    existing["ep_num"] = ep_num
                if total_eps and total_eps != "?":
                    existing["total_eps"] = total_eps

        day_map: dict = {d: [] for d in DAYS_OF_WEEK}
        for (slug, day, _vgroup), anime in merged.items():
            anime.pop("ep_num", None)
            day_map[day].append(anime)

        for d in day_map:
            day_map[d].sort(key=lambda x: -(x.get("popularity") or 0))

        return day_map

    @staticmethod
    def anime_block_rawsub(a: dict) -> str:
        """Render a Raw/Sub entry block (shown in the Raw & Sub section)."""
        has_raw = a.get("raw_time") is not None
        has_sub = a.get("sub_time") is not None
        if not (has_raw or has_sub):
            return ""
        lines = [f"🎬 <b>{a['title']}</b>"]
        if has_raw:
            raw_ep = a.get("raw_ep")
            ep_str = f" Ep {raw_ep}" if raw_ep else ""
            lines.append(f"  🔴 Raw{ep_str}: {fmt_time(a['raw_time'])}")
        if has_sub:
            sub_ep = a.get("sub_ep")
            ep_str = f" Ep {sub_ep}" if sub_ep else ""
            lines.append(f"  🔵 Sub{ep_str}: {fmt_time(a['sub_time'])}")
        total = a.get("total_eps", "?")
        if total and total != "?":
            lines.append(f"  📺 {total} eps total")
        return "\n".join(lines)

    @staticmethod
    def anime_block_dub(a: dict) -> str:
        """Render a Dub-only entry block (shown in the Dub section)."""
        if a.get("dub_time") is None:
            return ""
        dub_ep = a.get("dub_ep")
        ep_str = f" Ep {dub_ep}" if dub_ep else ""
        total  = a.get("total_eps", "?")
        lines  = [f"🎬 <b>{a['title']}</b>"]
        lines.append(f"  🟢 Dub{ep_str}: {fmt_time(a['dub_time'])}")
        if total and total != "?":
            lines.append(f"  📺 {total} eps total")
        return "\n".join(lines)

    @staticmethod
    def day_pages(day: str, anime_list: list, per_page: int = 15,
                  flt: Optional["ChatFilter"] = None,
                  year: Optional[int] = None, week: Optional[int] = None) -> list:
        """
        Build paginated schedule for a day.
        Format matches the reference screenshot:
          - Day header as plain bold text with actual date
          - Section headers (Raw & Sub / Dubbed) as plain text with count
          - Each show entry wrapped in its own <blockquote>
          - Divider line between sections
        """
        import datetime as _dt

        # Compute the actual calendar date for this day name
        day_index = DAYS_OF_WEEK.index(day) + 1  # ISO: Mon=1 … Sun=7
        if year and week:
            try:
                day_date = _dt.date.fromisocalendar(year, week, day_index)
                date_str = day_date.strftime("%d %b %Y")   # e.g. "25 Mar 2026"
            except Exception:
                date_str = ""
        else:
            date_str = ""

        header_date = f" {date_str}" if date_str else ""

        if not anime_list:
            return [f"\U0001f4c5 <b>{day}{header_date}</b>\n\nNo anime scheduled."]

        show_rawsub   = flt is None or "raw" in flt.air_types or "sub" in flt.air_types
        show_dub      = flt is None or "dub" in flt.air_types
        hide_donghua  = flt is not None and flt.hide_donghua

        def _visible(a: dict) -> bool:
            """Return False if this anime should be hidden by the current filter."""
            return not (hide_donghua and bool(a.get("donghua")))

        rawsub_entries = []
        if show_rawsub:
            for a in anime_list:
                if not _visible(a):
                    continue
                block = ScheduleProcessor.anime_block_rawsub(a)
                if block:
                    rawsub_entries.append(block)

        dub_entries = []
        if show_dub:
            for a in anime_list:
                if not _visible(a):
                    continue
                block = ScheduleProcessor.anime_block_dub(a)
                if block:
                    dub_entries.append(block)

        if not rawsub_entries and not dub_entries:
            return [f"\U0001f4c5 <b>{day}{header_date}</b>\n\nNo anime scheduled."]

        total_shows = len(rawsub_entries) + len(dub_entries)
        DIVIDER = "\u2500" * 16

        # Build flat item list: ("section_rawsub"), ("section_dub"), ("divider"), ("entry", text)
        items: list = []
        if rawsub_entries:
            items.append(("section_rawsub", len(rawsub_entries)))
            for b in rawsub_entries:
                items.append(("entry", b))
        if dub_entries:
            if rawsub_entries:
                items.append(("divider", DIVIDER))
            items.append(("section_dub", len(dub_entries)))
            for b in dub_entries:
                items.append(("entry", b))

        # Paginate — only "entry" items count toward per_page
        pages: list = []
        current: list = []
        entry_count = 0

        for item in items:
            kind = item[0]
            if kind == "entry":
                if entry_count >= per_page and current:
                    pages.append(current)
                    current = []
                    entry_count = 0
                current.append(item)
                entry_count += 1
            else:
                # Non-entry items: flush page if we hit limit, then attach header to new page
                if entry_count >= per_page and current:
                    pages.append(current)
                    current = []
                    entry_count = 0
                current.append(item)

        if current:
            pages.append(current)

        total_pages = len(pages)
        result = []
        for i, page_items in enumerate(pages):
            day_header = f"\U0001f4c5 <b>{day}{header_date}</b> \u2014 {total_shows} show(s)"
            parts = [day_header, ""]
            for item in page_items:
                kind = item[0]
                if kind == "section_rawsub":
                    count = item[1]
                    parts.append(f"\U0001f4fa <b>Raw &amp; Sub \u2014 {count} show(s)</b>")
                    parts.append("")
                elif kind == "section_dub":
                    count = item[1]
                    parts.append(f"\U0001f3ac <b>Dubbed \u2014 {count} show(s)</b>")
                    parts.append("")
                elif kind == "divider":
                    parts.append(item[1])
                    parts.append("")
                elif kind == "entry":
                    parts.append(f"<blockquote>{item[1]}</blockquote>")
                    parts.append("")  # blank line after every entry
            result.append("\n".join(parts))

        return result

# ── Reminder Scheduler ────────────────────────────────────────────────────────
class ReminderScheduler:
    def __init__(self, db: Database, scraper: AnimeScheduleScraper):
        self.db        = db
        self.scraper   = scraper
        self.app_ref   = None
        self.scheduler = AsyncIOScheduler(timezone=TZ_NAME)

    def start(self):
        self.scheduler.start()
        logger.info("Reminder scheduler started")

    def shutdown(self):
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    async def build_all_jobs(self):
        """Scrape current + next week and register APScheduler date jobs."""
        await self.db.clear_jobs()
        # Remove only date-based reminder jobs, NOT the interval refresh job
        for job in self.scheduler.get_jobs():
            if job.id != "refresh_schedule":
                job.remove()

        year, week = current_week_year()
        import datetime as _dt
        next_week_dt = _dt.date.today() + _dt.timedelta(weeks=1)
        nw_iso       = next_week_dt.isocalendar()
        week_pairs   = [(year, week), (nw_iso.year, nw_iso.week)]

        all_jobs: list = []
        for wy, w in week_pairs:
            entries   = await self.scraper.get_timetable(wy, w)
            processed = ScheduleProcessor.process(entries)
            for day_list in processed.values():
                for anime in day_list:
                    all_jobs.extend(self._make_jobs(anime))

        await self.db.bulk_save_jobs(all_jobs)

        now   = datetime.now(TZ)
        added = 0
        for j in all_jobs:
            dt = datetime.fromisoformat(j["air_time"]).astimezone(TZ)
            if dt <= now:
                continue
            jid = f"{j['anime_slug']}:{j['episode']}:{j['version']}"
            self.scheduler.add_job(
                self._send_reminder,
                "date",
                run_date=dt,
                id=jid,
                args=[j],
                replace_existing=True,
                misfire_grace_time=600,
            )
            added += 1

        # Register the 24h auto-refresh only once — skip if already scheduled
        if not self.scheduler.get_job("refresh_schedule"):
            self.scheduler.add_job(
                self.build_all_jobs,
                "interval",
                hours=24,
                id="refresh_schedule",
            )
            logger.info("Auto-reload job registered (every 24h)")
        logger.info("Built %d reminder jobs", added)

    def _make_jobs(self, anime: dict) -> list:
        jobs = []
        is_donghua = bool(anime.get("donghua"))
        for v in ("raw", "sub", "dub"):
            dt = anime.get(f"{v}_time")
            if dt is None:
                continue
            # Use per-version episode number; fall back to shared total_eps
            ep       = anime.get(f"{v}_ep") or "?"
            total_ep = anime.get("total_eps") or "?"
            jobs.append({
                "anime_slug":  anime["slug"],
                "anime_title": anime["title"],
                "episode":     ep,
                "total_eps":   total_ep,
                "version":     v,
                "air_time":    dt.isoformat(),
                "donghua":     is_donghua,   # needed for per-chat hide_donghua filter
            })
        return jobs

    async def _send_reminder(self, job: dict):
        if not self.app_ref:
            return
        air_dt  = datetime.fromisoformat(job["air_time"]).astimezone(TZ)
        version = job["version"].lower()
        text    = (
            f"\U0001f514 <b>Episode Alert!</b>\n\n"
            f"\U0001f3ac <b>{job['anime_title']}</b>\n"
            f"\U0001f4cc Type: {version.capitalize()}\n"
            f"\U0001f4fa Episode: {job['episode']}/{job['total_eps']}\n"
            f"\U0001f550 Time: {fmt_time(air_dt)}"
        )
        is_donghua = bool(job.get("donghua"))

        chats_with_filters = await self.db.all_chats_with_rem_topic()
        for cid, topic_id, flt in chats_with_filters:
            if topic_id:
                # Per-topic: use the topic-level filter for donghua (and air types below)
                topic_flt = await self.db.get_topic_filter(cid, topic_id)
                if topic_flt.hide_donghua and is_donghua:
                    continue
            else:
                # Chat-level donghua filter for plain subscribers
                if flt.hide_donghua and is_donghua:
                    continue

            if topic_id:
                # Topic is set — use ONLY per-topic rem config for air type filtering.
                # The shared /filter air types do NOT apply here so each topic
                # can independently choose raw/sub/dub.
                cfg     = await self.db.get_topic_cfg(cid, topic_id)
                # Merge over full defaults so any missing key (partial save)
                # never silently falls back to "always send".
                _rem_defaults = {"show_raw": True, "show_sub": True, "show_dub": True}
                rem_cfg = {**_rem_defaults, **cfg.get("rem", {})}
                logger.debug(
                    "Reminder filter: chat=%s topic=%s version=%s rem_cfg=%s",
                    cid, topic_id, version, rem_cfg
                )
                if not rem_cfg[f"show_{version}"]:
                    logger.info(
                        "Skipping %s reminder for %s (topic=%s) — show_%s=False",
                        version, job.get("anime_title"), topic_id, version
                    )
                    continue
            else:
                # No topic bound — fall back to chat-level filter
                if not flt.allows(version):
                    continue
            try:
                # message_thread_id delivers INTO a forum topic.
                # reply_to_message_id replies to a specific message — not the same.
                send_kwargs: dict = {"parse_mode": enums.ParseMode.HTML}
                if topic_id:
                    send_kwargs["reply_to_message_id"] = topic_id
                await self.app_ref.send_message(cid, text, **send_kwargs)
            except Exception as exc:
                logger.warning("Reminder to %s (topic=%s) failed: %s", cid, topic_id, exc)




# ── Gemini AI ─────────────────────────────────────────────────────────────────
class GeminiAI:
    """
    Per-user Gemini chat with persistent conversation history stored in MongoDB.
    Uses the Gemini REST API directly (no SDK dependency).

    History is capped at GEMINI_MAX_HIST messages per user to keep context
    windows small and responses fast.
    """

    SYSTEM_PROMPT = (
        "You are Naruto Uzumaki — the enthusiastic, never-give-up ninja from the Hidden Leaf Village. "
        "You love anime, ramen, and helping people. Answer questions with energy and keep replies "
        "concise (under 300 words unless more is truly needed). "
        "When discussing anime or manga you are knowledgeable but always stay in character. Dattebayo!"
    )

    def __init__(self, db: "Database"):
        self.db = db
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── History helpers ───────────────────────────────────────────────────
    async def _get_history(self, user_id: int) -> list:
        doc = await self.db.db["gemini_hist"].find_one({"_id": user_id})
        return doc.get("history", []) if doc else []

    async def _save_history(self, user_id: int, history: list):
        trimmed = history[-GEMINI_MAX_HIST:] if len(history) > GEMINI_MAX_HIST else history
        await self.db.db["gemini_hist"].update_one(
            {"_id": user_id},
            {"$set": {"history": trimmed, "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )

    async def clear_history(self, user_id: int):
        await self.db.db["gemini_hist"].delete_one({"_id": user_id})

    # ── Main chat method ──────────────────────────────────────────────────
    async def chat(self, user_id: int, prompt: str) -> str:
        """Send prompt to Gemini with history context, return reply text."""
        if not GEMINI_API_KEY:
            return "⚠️ Gemini API key not configured. Set GEMINI_API_KEY env var."

        history = await self._get_history(user_id)

        # Build contents array: system + history + new user message
        contents = []

        # Prepend system prompt as first user/model exchange if history is empty
        if not history:
            contents.append({"role": "user",   "parts": [{"text": self.SYSTEM_PROMPT}]})
            contents.append({"role": "model",  "parts": [{"text": "Believe it! I'm ready to help, dattebayo! 🍥"}]})

        # Append stored history
        for msg in history:
            contents.append(msg)

        # Add new user message
        contents.append({"role": "user", "parts": [{"text": prompt}]})

        payload = {
            "contents": contents,
            "generationConfig": {
                "temperature": 0.85,
                "maxOutputTokens": 800,
                "topP": 0.95,
            },
        }

        try:
            url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
            r   = await self.client.post(url, json=payload)
            if r.status_code == 429:
                return "⏳ Gemini rate limit hit — please wait a moment and try again."
            r.raise_for_status()
            data  = r.json()
            reply = (
                data.get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                    .strip()
            )
            if not reply:
                return "🤔 Hmm, I got an empty response from Gemini. Try again!"

            # Persist updated history
            history.append({"role": "user",  "parts": [{"text": prompt}]})
            history.append({"role": "model", "parts": [{"text": reply}]})
            await self._save_history(user_id, history)

            return reply

        except httpx.HTTPStatusError as exc:
            logger.error("Gemini API HTTP error: %s — %s", exc.response.status_code, exc.response.text[:200])
            return f"❌ Gemini API error ({exc.response.status_code}). Check your API key."
        except Exception as exc:
            logger.error("Gemini chat error: %s", exc)
            return "❌ Something went wrong talking to Gemini. Try again!"


# ── Banner Database ───────────────────────────────────────────────────────────
class BannerDB:
    """
    Banner storage with category support (ongoing / completed / rename).

    MongoDB schema: {name, category, msg_id, message_id, file_type}
    Telegram DB channel stores the raw media (photo or document).

    ✅ NEVER uses get_chat_history / iter_history.
    ✅ Uses only send_photo/send_document + get_messages — fully bot-compatible.
    """

    BANNERS_PER_PAGE = 6
    VALID_CATEGORIES = ("ongoing", "completed", "rename")

    def __init__(self, db: "Database"):
        self.db = db
        # In-memory pending-save state for multi-step /save_banner flow
        # {user_id: {"name": str, "category": str, "file_id": str, "file_type": str}}
        self._pending: dict = {}

    # ── Pending-save state helpers ────────────────────────────────────────
    def set_pending(self, user_id: int, data: dict):
        """Store pending save data for a user (category picker not yet clicked)."""
        self._pending[user_id] = data

    def get_pending(self, user_id: int) -> Optional[dict]:
        return self._pending.get(user_id)

    def clear_pending(self, user_id: int):
        self._pending.pop(user_id, None)

    # ── Save ──────────────────────────────────────────────────────────────
    async def save(self, app: "Client", name: str, category: str,
                   file_id: str, file_type: str) -> dict:
        """
        Upload media to DB channel, upsert {name, category, msg_id, file_type} into MongoDB.

        file_type : "photo" | "document"
        category  : "ongoing" | "completed" | "rename"
        Returns {"ok": True, "updated": bool} or {"ok": False, "error": str}.
        """
        if not BANNER_DB_CHAT:
            return {"ok": False, "error": "BANNER_DB_CHAT not configured"}
        try:
            name_lc = name.lower().strip()
            caption = f"[{category}] {name_lc}"

            # Forward to DB channel using the correct method for the file type
            if file_type == "document":
                sent = await app.send_document(
                    BANNER_DB_CHAT, document=file_id, caption=caption
                )
            else:
                sent = await app.send_photo(
                    BANNER_DB_CHAT, photo=file_id, caption=caption
                )

            # Upsert keyed on (name, category) — same name can exist in multiple categories
            existing = await self.db.banners.find_one(
                {"name": name_lc, "category": category}
            )
            await self.db.banners.update_one(
                {"name": name_lc, "category": category},
                {"$set": {
                    "name":       name_lc,
                    "category":   category,
                    "msg_id":     sent.id,
                    "message_id": sent.id,   # backward-compat alias
                    "file_type":  file_type,
                }},
                upsert=True,
            )
            was_updated = existing is not None
            logger.info(
                "BannerDB: saved '%s' [%s] msg_id=%d file_type=%s updated=%s",
                name_lc, category, sent.id, file_type, was_updated,
            )
            return {"ok": True, "updated": was_updated}
        except Exception as exc:
            logger.error("BannerDB.save error: %s", exc)
            return {"ok": False, "error": str(exc)}

    # ── Fetch single media message ─────────────────────────────────────────
    async def fetch_media(self, app: "Client", msg_id: int):
        """Fetch a media message from DB channel by msg_id. Fully bot-safe."""
        if not BANNER_DB_CHAT or not msg_id:
            return None
        try:
            msg = await app.get_messages(BANNER_DB_CHAT, msg_id)
            if not msg or getattr(msg, "empty", False):
                return None
            return msg
        except Exception as exc:
            logger.error("BannerDB.fetch_media error msg_id=%d: %s", msg_id, exc)
            return None

    # ── Search (MongoDB only) ─────────────────────────────────────────────
    async def search(self, query: str = "", category: str = "") -> list:
        """
        Case-insensitive substring search in MongoDB.
        Optionally filter by category ("ongoing"|"completed"|"rename").
        Empty query + no category returns all banners sorted A→Z.
        """
        filt: dict = {}
        if query:
            filt["name"] = {"$regex": re.escape(query.lower()), "$options": "i"}
        if category and category in self.VALID_CATEGORIES:
            filt["category"] = category
        cursor = self.db.banners.find(filt).sort("name", 1)
        return [doc async for doc in cursor]

    # ── Pagination ────────────────────────────────────────────────────────
    @staticmethod
    def paginate(banners: list, page: int = 0) -> tuple:
        n     = BannerDB.BANNERS_PER_PAGE
        total = max(1, (len(banners) + n - 1) // n)
        page  = max(0, min(page, total - 1))
        return banners[page * n : (page + 1) * n], total

    # ── List text builder ─────────────────────────────────────────────────
    @staticmethod
    def banner_list_text(page_banners: list, page: int, total_all: int,
                         query: str = "", total_pages: int = 1,
                         category: str = "") -> str:
        """
        Build the numbered list shown above the keyboard.

        Example:
            🖼 Banners — Ongoing
            6 result(s) · Page 1/2

            1. naruto banner [ongoing]
            2. one piece banner [ongoing]
        """
        hdr = "🖼 <b>Banners</b>"
        if category:
            hdr += f" — <i>{category.capitalize()}</i>"
        if query:
            hdr += f" — <i>{query}</i>"
        hdr += f"\n<i>{total_all} result(s)"
        if total_pages > 1:
            hdr += f" · Page {page + 1}/{total_pages}"
        hdr += "</i>"
        lines = [hdr]
        for i, b in enumerate(page_banners, 1):
            # Show category tag only if not already filtering by category
            cat_tag = f" <code>[{b.get('category','?')}]</code>" if not category else ""
            lines.append(f"\n{i}. {b['name']}{cat_tag}")
        return "\n".join(lines)

    # ── Category picker keyboard ──────────────────────────────────────────
    @staticmethod
    def category_picker_kb(action: str = "save") -> InlineKeyboardMarkup:
        """
        Shown by /save_banner (action="save") and /banner (action="browse")
        so the user picks which category to save into / browse.
        """
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🟡 Ongoing",    callback_data=f"bnr_cat:{action}:ongoing"),
                InlineKeyboardButton("✅ Completed",   callback_data=f"bnr_cat:{action}:completed"),
                InlineKeyboardButton("✏️ Rename File", callback_data=f"bnr_cat:{action}:rename"),
            ],
            [InlineKeyboardButton("✖ Cancel", callback_data="banner:close")],
        ])

    # ── Paginated list keyboard ───────────────────────────────────────────
    @staticmethod
    def banner_keyboard(banners: list, page: int, total_pages: int,
                        query: str = "", category: str = "") -> InlineKeyboardMarkup:
        """
        Number buttons [1][2]...[N] (3 per row) matching the list above,
        followed by [⬅️ Prev][➡️ Next] navigation row.

        Callback formats:
          banner_select:{cat}:{q}:{page}:{idx}  — retrieve banner at idx (0-based)
          banner_page:{cat}:{q}:{page}           — paginate
          banner:close                           — delete message
        """
        q_enc   = query[:20]
        cat_enc = category[:12]
        rows    = []

        num_btns = [
            InlineKeyboardButton(
                str(idx + 1),
                callback_data=f"banner_select:{cat_enc}:{q_enc}:{page}:{idx}",
            )
            for idx in range(len(banners))
        ]
        for i in range(0, len(num_btns), 3):
            rows.append(num_btns[i : i + 3])

        if total_pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton(
                    "⬅️ Prev",
                    callback_data=f"banner_page:{cat_enc}:{q_enc}:{page - 1}"
                ))
            if page < total_pages - 1:
                nav.append(InlineKeyboardButton(
                    "➡️ Next",
                    callback_data=f"banner_page:{cat_enc}:{q_enc}:{page + 1}"
                ))
            if nav:
                rows.append(nav)

        rows.append([InlineKeyboardButton("✖ Close", callback_data="banner:close")])
        return InlineKeyboardMarkup(rows)


class AnimeBot:
    def __init__(self):
        self.db             = Database(MONGO_URI)
        self.scraper        = AnimeScheduleScraper(self.db)
        self.al_api         = AniListAPI(self.db)
        self.season         = SeasonScraper(self.db)
        self.sched          = ReminderScheduler(self.db, self.scraper)
        self.nyaa           = NyaaScraper(self.db)
        self.title_resolver = TitleResolver(self.db)   # romaji → English
        self.gemini         = GeminiAI(self.db)         # NEW: Gemini AI chat
        self.banner_db      = BannerDB(self.db)         # NEW: Banner channel DB
        self.app: Optional[Client] = None
        self._health = None
        # Per-user airing page state {user_id: page_index}
        self._airing_pages: dict = {}

    # ── Topic ID extraction ───────────────────────────────────────────────
    @staticmethod
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

    # ── Guards ────────────────────────────────────────────────────────────
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

    async def _is_admin_of_chat(self, user_id: int, chat_id: int) -> bool:
        """
        Check if user_id is an admin/owner of chat_id.
        Used by topic-settings callbacks where the panel may be open in a DM
        but the target chat is a group.
        Always returns True for env-level bot admins.
        """
        if user_id in ADMIN_IDS:
            return True
        if await self.db.is_dynamic_admin(user_id):
            return True
        try:
            m = await self.app.get_chat_member(chat_id, user_id)
            return m.status.value in ("administrator", "owner", "creator")
        except Exception:
            return False

    async def _is_bot_admin(self, msg: Message) -> bool:
        if msg.from_user and msg.from_user.id in ADMIN_IDS:
            return True
        if msg.from_user and await self.db.is_dynamic_admin(msg.from_user.id):
            return True
        await msg.reply("⛔ Bot admin only.")
        return False

    # ── Static keyboards ──────────────────────────────────────────────────
    @staticmethod
    def _kb_main() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f4c5 View Schedule",     callback_data="menu:schedule")],
            [InlineKeyboardButton("⏰ Airing Now",               callback_data="menu:airing")],
            [InlineKeyboardButton("\U0001f514 Reminder Settings", callback_data="menu:reminders")],
            [InlineKeyboardButton("\U0001f4ca Weekly Overview",   callback_data="menu:weekly")],
            [InlineKeyboardButton("\U0001f39b Filter Settings",   callback_data="menu:filter")],
            [InlineKeyboardButton("\U0001f4cb Animes Assigned",   callback_data="menu:assigned")],
            [InlineKeyboardButton("\U0001f4d6 Help",              callback_data="menu:help")],
            [InlineKeyboardButton("\u274c Close",                 callback_data="menu:close")],
        ])

    @staticmethod
    def _kb_days(year: Optional[int] = None, week: Optional[int] = None) -> InlineKeyboardMarkup:
        """
        Day-picker keyboard for a given year/week (defaults to current week).
        Includes Prev Week / Next Week navigation and a week label.
        """
        import datetime as _dt
        if year is None or week is None:
            iso = datetime.now(TZ).isocalendar()
            year, week = iso.year, iso.week

        # Compute prev/next week using ISO calendar arithmetic
        # Find Monday of the target week
        monday = _dt.date.fromisocalendar(year, week, 1)
        prev_mon = monday - _dt.timedelta(weeks=1)
        next_mon = monday + _dt.timedelta(weeks=1)
        prev_iso = prev_mon.isocalendar()
        next_iso = next_mon.isocalendar()

        rows = []
        for i in range(0, len(DAYS_OF_WEEK), 2):
            rows.append([
                InlineKeyboardButton(d, callback_data=f"day:{d}:{year}:{week}:0")
                for d in DAYS_OF_WEEK[i : i + 2]
            ])

        # Week navigation row
        rows.append([
            InlineKeyboardButton(
                "\u25c4 Prev Week",
                callback_data=f"sched:week:{prev_iso.year}:{prev_iso.week}",
            ),
            InlineKeyboardButton(
                f"W{week} / {year}",
                callback_data="noop",
            ),
            InlineKeyboardButton(
                "Next Week \u25ba",
                callback_data=f"sched:week:{next_iso.year}:{next_iso.week}",
            ),
        ])
        rows.append([InlineKeyboardButton("\u25c0 Back", callback_data="menu:back")])
        return InlineKeyboardMarkup(rows)

    @staticmethod
    def _kb_reminders() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("\u2705 Subscribe",   callback_data="rem:on")],
            [InlineKeyboardButton("\u274c Unsubscribe", callback_data="rem:off")],
            [InlineKeyboardButton("\U0001f4cb Status",  callback_data="rem:status")],
            [InlineKeyboardButton("\u25c0 Back",        callback_data="menu:back")],
        ])

    # ── Schedule helper ───────────────────────────────────────────────────
    async def _get_schedule(self, chat_id: Optional[int] = None,
                            year: Optional[int] = None,
                            week: Optional[int] = None) -> dict:
        """
        Fetch and process the schedule for a given year/week.
        Defaults to the current week.  Applies chat filter if chat_id provided.

        Title resolution strategy:
          1. AS API already provides English/Romaji/Title — _coerce_entry picks
             English first, so most titles come out correctly with zero extra calls.
          2. For entries where the AS API had no English field (title == romaji),
             TitleResolver queries AniList as a fallback (cached 30 days in MongoDB).
        """
        if year is None or week is None:
            year, week = current_week_year()
        flt      = await self.db.get_filter(chat_id) if chat_id is not None else None
        entries  = await self.scraper.get_timetable(year, week, flt=flt)
        schedule = ScheduleProcessor.process(entries, flt=flt)

        # Collect titles where AS provided no English (title == romaji field)
        # and resolve them via AniList as a fallback.
        missing: set = set()
        for day_list in schedule.values():
            for anime in day_list:
                t = anime.get("title", "")
                r = anime.get("romaji", "")
                # If title equals romaji, AS had no English — try AniList fallback
                if t and r and t == r:
                    missing.add(r)

        if missing:
            title_map = await self.title_resolver.resolve_many(list(missing))
            for day_list in schedule.values():
                for anime in day_list:
                    romaji_key = anime.get("romaji", "")
                    if romaji_key in title_map:
                        english = title_map[romaji_key]
                        if english and english != romaji_key:
                            anime["title"] = english

        return schedule

    # ── /start ────────────────────────────────────────────────────────────
    async def cmd_start(self, _, msg: Message):
        if not await self._auth(msg):
            return
        await self.db.track_user(msg)  # track private users for /users count
        name = msg.from_user.first_name if msg.from_user else "Shinobi"
        text = (
            f"\U0001f343 <b>Naruto Timekeeper</b> \U0001f343\n\n"
            f"Yo, <b>{name}</b>!\n\n"
            "I track every anime episode that airs \u2014 raw, sub, and dub \u2014 "
            "so you never miss a moment.\n\n"
            "\U0001f4dc <b>What I can do:</b>\n"
            "  \u2022 Weekly anime schedule by day\n"
            "  \u2022 Episode reminders the moment they air\n"
            "  \u2022 Search any anime or manga via AniList\n\n"
            "Hit the button below to get started \u2193"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f4c5 View Schedule",   callback_data="menu:schedule")],
            [InlineKeyboardButton("\U0001f514 Reminders",       callback_data="menu:reminders")],
            [InlineKeyboardButton("\U0001f4ca Weekly Overview", callback_data="menu:weekly")],
            [InlineKeyboardButton("\U0001f4d6 Help",            callback_data="menu:help")],
        ])
        if BOT_IMAGE:
            try:
                await msg.reply_photo(
                    photo=BOT_IMAGE, caption=text,
                    reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                )
                return
            except Exception:
                pass
        await msg.reply(text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    # ── /help ─────────────────────────────────────────────────────────────
    async def cmd_help(self, _, msg: Message):
        if not await self._auth(msg):
            return
        text = (
            "🍃 <b>Naruto Timekeeper — Help</b>\n\n"
            "<b>📌 User Commands</b>\n"
            "/start — Welcome screen\n"
            "/settings — Full settings panel\n"
            "/anime &lt;name&gt; — Search anime (AniList)\n"
            "/manga &lt;name&gt; — Search manga (AniList)\n"
            "/airing — Episode countdowns for this week\n"
            "/filter — Schedule filters\n"
            "/season &lt;year&gt; &lt;season&gt; — Seasonal anime list\n"
            "/help — Show this message\n\n"
            "<b>🤖 AI Chat</b>\n"
            "/naruto &lt;prompt&gt; — Chat with Gemini AI (Naruto persona)\n"
            "naruto &lt;prompt&gt; — Auto-trigger (no slash needed)\n"
            "/naruto_clear — Clear your AI chat history\n\n"
            "<b>🖼 Banner System</b>\n"
            "/save_banner &lt;n&gt; — Reply to image to save banner\n"
            "/banner [name] — Search &amp; view saved banners\n"
            "<i>Inline:</i> @bot &lt;name&gt; — Search banners inline\n\n"
            "<b>🛡 Group Admin Commands</b>\n"
            "/auth — Authorize group\n"
            "/deauth — Remove group authorization\n"
            "/mode — Open topic settings panel\n"
            "/mode &lt;chat_id&gt;|&lt;topic_id&gt; — Topic settings by ID\n\n"
            "<b>🔧 Bot Admin Commands</b>\n"
            "/reload — Force schedule refresh\n"
            "/stats — Bot usage statistics\n"
            "/broadcast &lt;msg&gt; — Send to all subscribed chats\n"
            "/addadmin &lt;user_id&gt; — Add a bot admin\n"
            "/remadmin &lt;user_id&gt; — Remove a bot admin\n"
            "/admins — List all bot admins\n"
            "/grouplist — List authorized groups\n"
            "/users — User &amp; group counts\n"
            "/restart — Restart the bot\n\n"
            f"🕐 All times in <b>{TZ_NAME}</b>"
        )
        if BOT_IMAGE:
            try:
                await msg.reply_photo(
                    photo=BOT_IMAGE, caption=text, parse_mode=enums.ParseMode.HTML,
                )
                return
            except Exception:
                pass
        await msg.reply(text, parse_mode=enums.ParseMode.HTML)

    # ── /settings ─────────────────────────────────────────────────────────
    async def cmd_settings(self, _, msg: Message):
        if not await self._auth(msg):
            return
        await msg.reply(
            "\U0001f343 <b>Naruto Timekeeper</b>\n\nChoose an option from the panel below:",
            reply_markup=self._kb_main(),
            parse_mode=enums.ParseMode.HTML,
        )

    # ── /auth /deauth ─────────────────────────────────────────────────────
    async def cmd_auth(self, _, msg: Message):
        """
        /auth — authorize this group to use the bot.
        Restricted to: bot owner (OWNER_ID) or chat admins/owner.
        """
        if msg.chat.type == ChatType.PRIVATE:
            await msg.reply("This command is for groups only.")
            return
        uid = msg.from_user.id if msg.from_user else None
        if not uid:
            return
        is_owner = (uid == OWNER_ID) or (uid in ADMIN_IDS)
        is_chat_admin = await self._is_group_admin(msg)
        if not is_owner and not is_chat_admin:
            await msg.reply(
                "❌ You are not authorized to use this command.",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        await self.db.authorize_group(msg.chat.id, msg.from_user.id)
        await msg.reply(
            "✅ <b>Group authorized!</b>\n"
            "This group can now use Naruto Timekeeper.\n\n"
            "<b>Tip:</b> Use <code>/mode</code> inside a topic to assign "
            "it a mode (Reminders or Nyaa).",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_mode(self, _, msg: Message):
        """
        /mode                          — open topic settings for current topic
        /mode <chat_id>|<topic_id>     — open topic settings by ID from anywhere
        Example: /mode -1003739341690|46
        """
        uid  = msg.from_user.id if msg.from_user else None
        if not uid:
            return

        args = msg.command[1:] if len(msg.command) > 1 else []
        target_str = args[0] if args else None

        target_chat_id  = None
        target_topic_id = None

        if target_str and "|" in target_str:
            # Explicit: /mode -1003739341690|46
            parts = target_str.split("|", 1)
            try:
                target_chat_id  = int(parts[0])
                target_topic_id = int(parts[1])
            except ValueError:
                await msg.reply(
                    "❌ Invalid format. Use: <code>/mode -1001234567890|46</code>",
                    parse_mode=enums.ParseMode.HTML,
                )
                return
            # Verify the sender is an admin of the TARGET chat
            if not await self._is_admin_of_chat(uid, target_chat_id):
                await msg.reply("⛔ You must be an admin of that group to use this.")
                return
        else:
            # Auto-detect from current message
            if msg.chat.type == ChatType.PRIVATE:
                await msg.reply(
                    "Use: <code>/mode &lt;chat_id&gt;|&lt;topic_id&gt;</code>\n"
                    "Example: <code>/mode -1003739341690|46</code>",
                    parse_mode=enums.ParseMode.HTML,
                )
                return
            # Verify admin of current group
            if not await self._is_group_admin(msg):
                await msg.reply("⛔ Only group admins can run this.")
                return
            target_chat_id = msg.chat.id

            # If a plain topic ID was given as argument (e.g. /mode 46), use it directly
            if target_str and target_str.lstrip("-").isdigit():
                try:
                    target_topic_id = int(target_str)
                except ValueError:
                    target_topic_id = None
            else:
                target_topic_id = self._get_topic_id(msg)

            # Debug: log all relevant attrs so we can see what Pyrogram provides
            logger.info(
                "cmd_mode topic detection: chat=%s topic=%s | "
                "topic_obj=%s reply_to_top=%s is_topic=%s reply_to=%s thread=%s",
                msg.chat.id,
                target_topic_id,
                getattr(getattr(msg, 'topic', None), 'id', None),
                getattr(msg, 'reply_to_top_message_id', None),
                getattr(msg, 'is_topic_message', None),
                getattr(msg, 'reply_to_message_id', None),
                getattr(msg, 'message_thread_id', None),
            )

            if not target_topic_id:
                await msg.reply(
                    "⚠️ Could not detect topic ID automatically.\n\n"
                    "<b>Option 1</b> — Pass the topic ID directly (run inside the topic):\n"
                    "<code>/mode &lt;topic_id&gt;</code>\n"
                    "Example: <code>/mode 46</code>\n\n"
                    "<b>Option 2</b> — Use the full format from anywhere:\n"
                    "<code>/mode &lt;chat_id&gt;|&lt;topic_id&gt;</code>\n"
                    "Example: <code>/mode -1003739341690|46</code>\n\n"
                    "Your topic ID is in its link — e.g. <code>t.me/c/3739341690/<b>46</b></code> → ID is <b>46</b>",
                    parse_mode=enums.ParseMode.HTML,
                )
                return

        current = await self.db.get_topic_mode(target_chat_id, target_topic_id)
        text, kb = self._topic_settings_panel(target_topic_id, current, target_chat_id)
        # Always use send_message (not msg.reply) so we can pass message_thread_id.
        # Pyrogram's Message.reply() does not accept message_thread_id.
        # This also ensures the panel lands in the correct topic even when
        # /mode is invoked from a DM using the chat_id|topic_id format.
        await self.app.send_message(
            target_chat_id,
            text,
            reply_markup=kb,
            parse_mode=enums.ParseMode.HTML,
            reply_to_message_id=target_topic_id,
        )

    # ── Topic settings panel helpers ──────────────────────────────────────
    @staticmethod
    def _topic_settings_panel(topic_id: int, current_mode: Optional[str], target_chat_id: int = 0) -> tuple:
        """Return (text, InlineKeyboardMarkup) for the topic settings main panel."""
        mode_label = {
            "rem":  "📅 Reminders",
            "nyaa": "🌐 Nyaa Scraper",
        }.get(current_mode or "", "None")

        cid = target_chat_id  # embed in callback data so handler knows which chat
        text = (
            f"⚙️ <b>Topic Settings</b>\n\n"
            f"Chat: <code>{cid}</code>\n"
            f"Topic ID: <code>{topic_id}</code>\n"
            f"Current mode: <b>{mode_label}</b>\n\n"
            "Select a mode to assign this topic, or configure the current mode below."
        )
        rows = [
            [
                InlineKeyboardButton(
                    "📅 Reminder" + (" ✅" if current_mode == "rem"  else ""),
                    callback_data=f"ts:set:rem:{cid}:{topic_id}",
                ),
                InlineKeyboardButton(
                    "🌐 Nyaa"     + (" ✅" if current_mode == "nyaa" else ""),
                    callback_data=f"ts:set:nyaa:{cid}:{topic_id}",
                ),
            ],
        ]
        if current_mode == "rem":
            rows.append([InlineKeyboardButton(
                "🔧 Reminder Settings", callback_data=f"ts:cfg:rem:{cid}:{topic_id}"
            )])
        elif current_mode == "nyaa":
            rows.append([InlineKeyboardButton(
                "🔧 Nyaa Settings", callback_data=f"ts:cfg:nyaa:{cid}:{topic_id}"
            )])
        rows.append([InlineKeyboardButton(
            "🚫 Clear Mode", callback_data=f"ts:clear:{cid}:{topic_id}"
        )])
        rows.append([InlineKeyboardButton("✖ Close", callback_data="ts:close")])
        return text, InlineKeyboardMarkup(rows)

    @staticmethod
    def _rem_settings_kb(topic_id: int, rem_cfg: dict, target_chat_id: int = 0) -> InlineKeyboardMarkup:
        """Keyboard for reminder-topic sub-settings (air types + schedule filter)."""
        def tick(v): return "✅" if v else "⬜"
        show_raw = rem_cfg.get("show_raw", True)
        show_sub = rem_cfg.get("show_sub", True)
        show_dub = rem_cfg.get("show_dub", True)
        cid = target_chat_id
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{tick(show_raw)} Raw", callback_data=f"ts:rem:raw:{cid}:{topic_id}")],
            [InlineKeyboardButton(f"{tick(show_sub)} Sub", callback_data=f"ts:rem:sub:{cid}:{topic_id}")],
            [InlineKeyboardButton(f"{tick(show_dub)} Dub", callback_data=f"ts:rem:dub:{cid}:{topic_id}")],
            [InlineKeyboardButton("🎛 Schedule Filter",    callback_data=f"ts:flt:main:{cid}:{topic_id}")],
            [InlineKeyboardButton("◀ Back",               callback_data=f"ts:back:{cid}:{topic_id}")],
        ])

    @staticmethod
    def _nyaa_settings_kb(topic_id: int, nyaa_cfg: dict, target_chat_id: int = 0) -> InlineKeyboardMarkup:
        """Keyboard for nyaa-topic sub-settings."""
        def tick(v): return "✅" if v else "⬜"
        varyg      = nyaa_cfg.get("varyg",      True)
        toonshub   = nyaa_cfg.get("toonshub",   True)
        subsplease = nyaa_cfg.get("subsplease", True)
        cid = target_chat_id
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🔴 {tick(varyg)}    varyg1001",    callback_data=f"ts:nyaa:varyg:{cid}:{topic_id}")],
            [InlineKeyboardButton(f"🟡 {tick(toonshub)} ToonsHub",     callback_data=f"ts:nyaa:toonshub:{cid}:{topic_id}")],
            [InlineKeyboardButton(f"🟢 {tick(subsplease)} subsplease", callback_data=f"ts:nyaa:subsplease:{cid}:{topic_id}")],
            [InlineKeyboardButton("◀ Back", callback_data=f"ts:back:{cid}:{topic_id}")],
        ])

    async def cmd_deauth(self, _, msg: Message):
        """
        /deauth            — deauthorize current group
        /deauth <chat_id>  — deauthorize a specific group by ID (bot admin only)
        """
        args           = msg.command[1:] if len(msg.command) > 1 else []
        target_str     = args[0] if args else None

        if target_str and target_str.lstrip("-").isdigit():
            # Bot admin deauthorizing another chat by ID
            if not await self._is_bot_admin(msg):
                return
            target_chat_id = int(target_str)
        else:
            # Deauthorize current group — requires group admin
            if msg.chat.type == ChatType.PRIVATE:
                await msg.reply("This command is for groups only.")
                return
            if not await self._is_group_admin(msg):
                await msg.reply("⛔ Only group admins can run this.")
                return
            target_chat_id = msg.chat.id

        await self.db.deauthorize_group(target_chat_id)
        await msg.reply(
            f"❌ Chat <code>{target_chat_id}</code> deauthorized.",
            parse_mode=enums.ParseMode.HTML,
        )

    # ── AniList search helpers ───────────────────────────────────────────
    @staticmethod
    def _al_search_rows(results: list, al_type: str) -> list:
        """Build InlineKeyboardButton rows for an AniList search result list."""
        rows = []
        for item in results:
            item_id   = item.get("id")
            t         = item.get("title", {})
            label     = t.get("english") or t.get("romaji") or "Unknown"
            year      = (item.get("startDate") or {}).get("year") or ""
            fmt       = (item.get("format") or "").replace("_", " ").title()
            btn_label = f"{label} ({fmt}{', ' + str(year) if year else ''})"
            rows.append([InlineKeyboardButton(
                btn_label[:60],
                callback_data=f"al:{al_type}:{item_id}",
            )])
        rows.append([InlineKeyboardButton("✖ Close", callback_data="al:close")])
        return rows

    # ── /anime ────────────────────────────────────────────────────────────
    async def cmd_anime(self, _, msg: Message):
        if not await self._auth(msg):
            return
        q = " ".join(msg.command[1:]).strip()
        if not q:
            await msg.reply("Usage: /anime &lt;name&gt;", parse_mode=enums.ParseMode.HTML)
            return

        wait = await msg.reply(
            f"Searching anime: <i>{q}</i>\u2026", parse_mode=enums.ParseMode.HTML
        )
        results = await self.al_api.search_anime_list(q)
        try:
            await wait.delete()
        except Exception:
            pass

        if not results:
            await msg.reply("\u274c No anime found.", parse_mode=enums.ParseMode.HTML)
            return

        # Cache results so the Back button can rebuild the list
        cache_key = f"al:search:anime:{msg.chat.id}"
        await self.db.set_cache(cache_key, {"q": q, "results": results[:8]}, ttl=1800)

        rows = self._al_search_rows(results[:8], "anime")
        await msg.reply(
            f"\U0001f50d <b>Results for \"{q}\"</b>\nTap a title to view details:",
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=enums.ParseMode.HTML,
        )

    async def _send_anime_card(self, target, r: dict, search_msg=None, al_type: str = "anime"):
        """Send anime detail card — AniList image + exact specified format."""
        t          = r.get("title", {})
        eng_title  = (t.get("english") or "").strip()
        rom_title  = (t.get("romaji") or "").strip()
        display_en = eng_title or rom_title      # fallback romaji if english null
        display_rom = rom_title or eng_title

        media_id = r.get("id", 0)
        fmt      = (r.get("format") or "").replace("_", " ").title()
        score    = r.get("averageScore") or "N/A"
        status   = al_status(r.get("status", ""))
        episodes = r.get("episodes") or "?"
        duration = r.get("duration") or "?"
        genres   = ", ".join(r.get("genres") or []) or "N/A"
        source   = (r.get("source") or "").replace("_", " ").title() or "N/A"
        season   = r.get("season") or ""
        sea_year = r.get("seasonYear") or ""
        start_dt = al_date(r.get("startDate"))
        end_dt   = al_date(r.get("endDate"))

        # Synopsis — strip HTML, truncate with read-more link
        raw_desc = strip_html(r.get("description") or "")
        synopsis = truncate_synopsis(raw_desc, media_id, "anime")

        # Optional AniList-style header
        header_parts = []
        if season:
            header_parts.append(season.capitalize())
        if sea_year:
            header_parts.append(str(sea_year))
        if fmt:
            header_parts.append(fmt)
        if episodes and episodes != "?":
            header_parts.append(f"{episodes} Episodes")
        header_line = (" • ".join(header_parts) + "\n") if header_parts else ""

        cap = (
            f"<b>{display_en}</b> | <b>{display_rom}</b>\n\n"
            f"‣ <b>Genres</b> : {genres}\n"
            f"‣ <b>Type</b> : {fmt or 'N/A'}\n"
            f"‣ <b>Average Rating</b> : {score}\n"
            f"‣ <b>Status</b> : {status}\n"
            f"‣ <b>First aired</b> : {start_dt}\n"
            f"‣ <b>Last aired</b> : {end_dt}\n"
            f"‣ <b>Runtime</b> : {duration} minutes\n"
            f"‣ <b>No of episodes</b> : {episodes}\n\n"
            f"‣ <b>Synopsis</b> : {synopsis}\n\n"
            f"<i>(Source: {source})</i>"
        )

        # Build buttons — layout:
        #   [ℹ️ Information] [▶️ Trailer]
        #   [🔗 Relations]   [👤 Character]
        #   [◀ Close and go back]
        trailer_url = al_trailer_url(r)
        anilist_url = r.get("siteUrl") or f"https://anilist.co/anime/{media_id}"

        row1 = [InlineKeyboardButton("ℹ️ Information", url=anilist_url)]
        if trailer_url:
            row1.append(InlineKeyboardButton("▶️ Trailer", url=trailer_url))
        row2 = [
            InlineKeyboardButton("🔗 Relations",  callback_data=f"al:relations:{media_id}"),
            InlineKeyboardButton("👤 Character",  callback_data=f"al:characters:anime:{media_id}"),
        ]
        row3 = [InlineKeyboardButton("◀ Close and go back", callback_data=f"al:back:anime:{media_id}")]

        kb = InlineKeyboardMarkup([row1, row2, row3])

        # Cover image — prefer extraLarge → large
        thumb = al_cover(r)

        await self._deliver_media_card(target, search_msg, thumb, cap, kb)

    async def _deliver_media_card(self, target, search_msg, thumb: Optional[str],
                                   cap: str, kb: InlineKeyboardMarkup):
        """
        Shared delivery logic for anime/manga cards.
        Delivery strategy (EXACTLY ONE message sent, never duplicated):
          1. Photo with caption (if cap ≤ 1024 chars and thumb exists)
          2. Photo alone + text message with caption (if cap > 1024 and thumb exists)
          3. Text-only (if no thumb or photo send fails)
        Always deletes search_msg first if provided.
        """
        if search_msg is not None:
            try:
                await search_msg.delete()
            except Exception:
                pass

        sent = False

        if thumb:
            if len(cap) <= TG_CAPTION_LIMIT:
                # Strategy 1: single photo message with caption
                try:
                    await target.reply_photo(
                        photo=thumb, caption=cap,
                        reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                    )
                    return   # ← done, exit immediately
                except Exception as exc:
                    logger.warning("send_photo+caption failed: %s — falling back to text", exc)
                    # Fall through to text-only below
            else:
                # Strategy 2: photo first, then text with buttons
                try:
                    await target.reply_photo(photo=thumb)
                    sent = True
                except Exception as exc:
                    logger.warning("send_photo (no cap) failed: %s", exc)
                try:
                    await target.reply(
                        cap, reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                    return   # ← done
                except Exception as exc:
                    logger.warning("text after photo failed: %s", exc)
                    return   # already sent photo; don't send text again

        # Strategy 3: text-only (no thumb, or photo failed before sending)
        if not sent:
            try:
                await target.reply(
                    cap, reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except Exception as exc:
                logger.error("_deliver_media_card final fallback failed: %s", exc)

    async def _send_manga_card(self, target, r: dict, search_msg=None, al_type: str = "manga"):
        """Send manga detail card — AniList image + exact specified format."""
        t          = r.get("title", {})
        eng_title  = (t.get("english") or "").strip() or "N/A"
        rom_title  = (t.get("romaji") or "").strip()

        media_id = r.get("id", 0)
        fmt      = (r.get("format") or "").replace("_", " ").title() or "N/A"
        score    = r.get("averageScore") or "N/A"
        status   = al_status(r.get("status", ""))
        chapters = r.get("chapters") or "?"
        volumes  = r.get("volumes") or "N/A"
        genres   = ", ".join(r.get("genres") or []) or "N/A"
        source   = (r.get("source") or "").replace("_", " ").title() or "N/A"

        # Start/end years (manga uses year only)
        sd = r.get("startDate") or {}
        ed = r.get("endDate") or {}
        start_year = sd.get("year") or "N/A"
        end_year   = ed.get("year") or "N/A"

        # Synopsis
        raw_desc = strip_html(r.get("description") or "")
        synopsis = truncate_synopsis(raw_desc, media_id, "manga")

        cap = (
            f"<b>{eng_title}</b> | <b>{rom_title}</b>\n\n"
            f"‣ <b>Type</b> : {fmt}\n"
            f"‣ <b>Average Rating</b> : {score}\n"
            f"‣ <b>Status</b> : {status}\n"
            f"‣ <b>First release</b> : {start_year}\n"
            f"‣ <b>Last release</b> : {end_year}\n"
            f"‣ <b>Volume count</b> : {volumes}\n"
            f"‣ <b>No of chapters</b> : {chapters}\n"
            f"‣ <b>Genres</b> : {genres}\n\n"
            f"‣ <b>Synopsis</b> : {synopsis}\n\n"
            f"<i>(Source: {source})</i>"
        )

        # Build buttons — layout:
        #   [ℹ️ Information] [▶️ Trailer]
        #   [🔗 Relations]   [👤 Character]
        #   [◀ Close and go back]
        trailer_url = al_trailer_url(r)
        anilist_url = r.get("siteUrl") or f"https://anilist.co/manga/{media_id}"

        row1 = [InlineKeyboardButton("ℹ️ Information", url=anilist_url)]
        if trailer_url:
            row1.append(InlineKeyboardButton("▶️ Trailer", url=trailer_url))
        row2 = [
            InlineKeyboardButton("🔗 Relations",  callback_data=f"al:relations:{media_id}"),
            InlineKeyboardButton("👤 Character",  callback_data=f"al:characters:manga:{media_id}"),
        ]
        row3 = [InlineKeyboardButton("◀ Close and go back", callback_data=f"al:back:manga:{media_id}")]

        kb = InlineKeyboardMarkup([row1, row2, row3])

        thumb = al_cover(r)
        await self._deliver_media_card(target, search_msg, thumb, cap, kb)

    # ── /manga ────────────────────────────────────────────────────────────
    async def cmd_manga(self, _, msg: Message):
        if not await self._auth(msg):
            return
        q = " ".join(msg.command[1:]).strip()
        if not q:
            await msg.reply("Usage: /manga &lt;name&gt;", parse_mode=enums.ParseMode.HTML)
            return

        wait = await msg.reply(
            f"Searching manga: <i>{q}</i>\u2026", parse_mode=enums.ParseMode.HTML
        )
        results = await self.al_api.search_manga_list(q)
        try:
            await wait.delete()
        except Exception:
            pass

        if not results:
            await msg.reply("\u274c No manga found.", parse_mode=enums.ParseMode.HTML)
            return

        # Cache results so the Back button can rebuild the list
        cache_key = f"al:search:manga:{msg.chat.id}"
        await self.db.set_cache(cache_key, {"q": q, "results": results[:8]}, ttl=1800)

        rows = self._al_search_rows(results[:8], "manga")
        await msg.reply(
            f"\U0001f50d <b>Results for \"{q}\"</b>\nTap a title to view details:",
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=enums.ParseMode.HTML,
        )

        # ── Admin commands ────────────────────────────────────────────────────
    async def cmd_reload(self, _, msg: Message):
        if not await self._is_bot_admin(msg):
            return
        await msg.reply("\U0001f504 Reloading schedule\u2026")
        # Drop only schedule/timetable cache — preserve nyaa_seen and nyaa_watermark
        # to prevent re-sending old torrents after reload
        await self.db.cache.delete_many({
            "key": {"$not": {"$regex": "^nyaa"}}
        })
        await self.sched.build_all_jobs()
        await msg.reply("\u2705 Done.")

    async def cmd_stats(self, _, msg: Message):
        if not await self._is_bot_admin(msg):
            return
        s = await self.db.get_stats()
        await msg.reply(
            "<b>\U0001f4ca Bot Stats</b>\n\n"
            f"Token Set:        {'Yes' if AS_TOKEN else 'NO — set ANIMESCHEDULE_TOKEN!'}\n"
            f"Scrape Calls:     {s.get('scrape_calls', 0)}\n"
            f"Scrape Fallbacks: {s.get('scrape_fallbacks', 0)}\n"
            f"AL API Calls:     {s.get('al_api_calls', 0)}\n"
            f"Cache Hits:       {s.get('cache_hits', 0)}\n"
            f"Season Scrapes:   {s.get('season_scrape_calls', 0)}\n"
            f"Season Cache:     {s.get('season_cache_hits', 0)}\n"
            f"Sched Jobs:       {len(self.sched.scheduler.get_jobs())}\n"
            f"Auth Groups:      {await self.db.count_auth()}\n"
            f"Subscribed:       {await self.db.count('chats')}\n"
            f"Job Docs:         {await self.db.count('jobs')}\n"
            f"Cache Docs:       {await self.db.count('cache')}",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_broadcast(self, _, msg: Message):
        if not await self._is_bot_admin(msg):
            return
        text = " ".join(msg.command[1:]).strip()
        if not text:
            await msg.reply("Usage: /broadcast &lt;message&gt;", parse_mode=enums.ParseMode.HTML)
            return
        chats = await self.db.all_subscribed_chats()
        sent  = 0
        for cid in chats:
            try:
                await self.app.send_message(cid, text)
                sent += 1
            except Exception:
                pass
        await msg.reply(f"\U0001f4e2 Sent to {sent}/{len(chats)} chats.")

    async def cmd_addadmin(self, _, msg: Message):
        """
        /addadmin <user_id> — add a dynamic bot admin (env ADMIN_IDS only)
        """
        if not (msg.from_user and msg.from_user.id in ADMIN_IDS):
            await msg.reply("⛔ Only env-level admins can add new admins.")
            return
        args = msg.command[1:]
        if not args or not args[0].lstrip("-").isdigit():
            await msg.reply("Usage: <code>/addadmin &lt;user_id&gt;</code>", parse_mode=enums.ParseMode.HTML)
            return
        uid = int(args[0])
        await self.db.add_admin(uid)
        await msg.reply(f"✅ <code>{uid}</code> added as bot admin.", parse_mode=enums.ParseMode.HTML)

    async def cmd_remadmin(self, _, msg: Message):
        """
        /remadmin <user_id> — remove a dynamic bot admin
        """
        if not (msg.from_user and msg.from_user.id in ADMIN_IDS):
            await msg.reply("⛔ Only env-level admins can remove admins.")
            return
        args = msg.command[1:]
        if not args or not args[0].lstrip("-").isdigit():
            await msg.reply("Usage: <code>/remadmin &lt;user_id&gt;</code>", parse_mode=enums.ParseMode.HTML)
            return
        uid = int(args[0])
        await self.db.remove_admin(uid)
        await msg.reply(f"✅ <code>{uid}</code> removed from bot admins.", parse_mode=enums.ParseMode.HTML)

    async def cmd_admins(self, _, msg: Message):
        """
        /admins — list all bot admins (env + dynamic DB)
        """
        if not await self._is_bot_admin(msg):
            return
        env_admins = ADMIN_IDS
        db_admins  = await self.db.get_dynamic_admins()
        # Merge, marking source
        all_ids    = {uid: "env" for uid in env_admins}
        for uid in db_admins:
            if uid not in all_ids:
                all_ids[uid] = "db"
            else:
                all_ids[uid] = "env+db"

        if not all_ids:
            await msg.reply("No admins configured.")
            return

        lines = [f"<code>{uid}</code> — {src}" for uid, src in all_ids.items()]
        await msg.reply(
            f"🛡 <b>Bot Admins ({len(all_ids)})</b>\n\n" + "\n".join(lines),
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_grouplist(self, _, msg: Message):
        """
        /grouplist — list all authorized groups with their chat IDs
        """
        if not await self._is_bot_admin(msg):
            return
        groups = await self.db.all_authorized_groups()
        if not groups:
            await msg.reply("No authorized groups.")
            return
        lines = [f"<code>{g['chat_id']}</code>" for g in groups]
        await msg.reply(
            f"👥 <b>Authorized Groups ({len(groups)})</b>\n\n" + "\n".join(lines),
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_users(self, _, msg: Message):
        """
        /users — show total user and group counts
        """
        if not await self._is_bot_admin(msg):
            return
        total_chats  = await self.db.count("chats")
        subscribed   = len(await self.db.all_subscribed_chats())
        auth_groups  = len(await self.db.all_authorized_groups())
        private_users = await self.db.count_private_users()
        await msg.reply(
            "👤 <b>Users &amp; Groups</b>\n\n"
            f"Private users:     <b>{private_users}</b>\n"
            f"Auth groups:       <b>{auth_groups}</b>\n"
            f"Subscribed chats:  <b>{subscribed}</b>\n"
            f"Total chat docs:   <b>{total_chats}</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_restart(self, _, msg: Message):
        """
        /restart — gracefully restart the bot process
        """
        if not await self._is_bot_admin(msg):
            return
        await msg.reply("🔄 Restarting…")
        import os, sys
        # Re-exec the current process
        os.execv(sys.executable, [sys.executable] + sys.argv)

    # ── Callback handler ──────────────────────────────────────────────────
    @staticmethod
    async def safe_edit(query, text: str, reply_markup=None, parse_mode=None):
        """Edit a message text. Falls back to delete+resend for photo messages.
        Handles FloodWait by sleeping and retrying once."""
        import re as _re
        for attempt in range(2):
            try:
                await query.edit_message_text(
                    text, reply_markup=reply_markup, parse_mode=parse_mode
                )
                return
            except Exception as exc:
                err = str(exc)
                if "MESSAGE_NOT_MODIFIED" in err or "message is not modified" in err.lower():
                    return
                if "FLOOD_WAIT" in err:
                    m = _re.search(r"wait of (\d+) seconds", err)
                    wait = int(m.group(1)) + 1 if m else 5
                    logger.info("safe_edit FloodWait %ds, retrying…", wait)
                    await asyncio.sleep(wait)
                    continue
                if "MEDIA_CAPTION_TOO_LONG" in err or "caption" in err.lower():
                    try:
                        chat_id = query.message.chat.id
                        await query.message.delete()
                        await query.message._client.send_message(
                            chat_id, text,
                            reply_markup=reply_markup,
                            parse_mode=parse_mode,
                        )
                        return
                    except Exception:
                        pass
                raise

    async def handle_callback(self, _, query: CallbackQuery):
        data    = query.data
        chat_id = query.message.chat.id
        ctype   = query.message.chat.type

        if not await self.db.is_authorized(chat_id, ctype):
            await query.answer("\U0001f512 Group not authorized.", show_alert=True)
            return

        if data == "menu:back":
            await self.safe_edit(
                query,
                "\U0001f343 <b>Naruto Timekeeper</b>\n\nChoose an option:",
                reply_markup=self._kb_main(),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:close":
            await query.message.delete()

        elif data == "menu:help":
            text = (
                "\U0001f343 <b>Naruto Timekeeper \u2014 Help</b>\n\n"
                "/start \u2014 Welcome screen\n"
                "/settings \u2014 Full settings panel\n"
                "/anime &lt;name&gt; \u2014 Search anime (AniList)\n"
                "/manga &lt;name&gt; \u2014 Search manga (AniList)\n"
                "/auth \u2014 Authorize group (admin)\n\n"
                f"\U0001f550 Times in <b>{TZ_NAME}</b>"
            )
            await self.safe_edit(
                query, text,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u25c0 Back", callback_data="menu:back")]]
                ),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:schedule":
            y, w = current_week_year()
            await self.safe_edit(
                query,
                f"\U0001f4c5 <b>Select a day</b> \u2014 Week {w}, {y}:",
                reply_markup=self._kb_days(y, w),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data.startswith("menu:airing"):
            # Supports pagination: data may be "menu:airing" (page 0) or
            # "menu:airing:N" (page N via refresh/nav)
            parts_air = data.split(":")
            air_page  = int(parts_air[2]) if len(parts_air) > 2 and parts_air[2].lstrip("-").isdigit() else 0
            uid_air   = query.from_user.id if query.from_user else chat_id
            await query.answer("Loading airing schedule…")
            now          = datetime.now(TZ)
            year, week   = current_week_year()
            schedule     = await self._get_schedule(chat_id, year, week)

            # Build deduplicated, sorted entry list
            seen_air: set = set()
            upcoming: list = []
            for day_list in schedule.values():
                for anime in day_list:
                    title = anime.get("title", "?")
                    for ver in ("raw", "sub", "dub"):
                        dt = anime.get(f"{ver}_time")
                        if dt is None:
                            continue
                        ep  = anime.get(f"{ver}_ep") or "?"
                        key = (title, ver, str(ep))
                        if key in seen_air:
                            continue
                        seen_air.add(key)
                        upcoming.append((dt, title, ver, ep))

            upcoming.sort(key=lambda x: x[0])
            cutoff_past   = now - timedelta(hours=6)
            cutoff_future = now + timedelta(hours=72)
            visible = [i for i in upcoming if cutoff_past <= i[0] <= cutoff_future]
            if not visible:
                visible = [i for i in upcoming if i[0] >= now][:30]

            def _air_icon(ver: str, dt: datetime) -> str:
                delta = (dt - now).total_seconds()
                if abs(delta) <= 300:           # airing right now (±5 min)
                    return "🔴"
                if -3600 < delta <= 0:          # recently aired (<1h ago)
                    return "🟢"
                return {"raw": "🔴", "sub": "🔵", "dub": "🟢"}.get(ver, "⚪")

            # Paginate
            total_pages = max(1, (len(visible) + AIRING_PAGE_SIZE - 1) // AIRING_PAGE_SIZE)
            air_page    = max(0, min(air_page, total_pages - 1))
            page_items  = visible[air_page * AIRING_PAGE_SIZE:(air_page + 1) * AIRING_PAGE_SIZE]

            if not visible:
                airing_text = "✅ <b>No episodes in the next 72 hours.</b>"
            else:
                lines = [f"⏰ <b>Airing — Week {week}, {year}</b>\n"]
                for (air_dt, title, ver, ep) in page_items:
                    delta   = air_dt - now
                    seconds = int(delta.total_seconds())
                    icon    = _air_icon(ver, air_dt)
                    if seconds < 0:
                        ago = abs(seconds) // 60
                        time_str = f"aired {ago}m ago" if ago < 60 else f"aired {ago // 60}h ago"
                    else:
                        h, rem = divmod(seconds, 3600)
                        m_val  = rem // 60
                        time_str = f"in {h}h {m_val:02d}m" if h > 0 else f"in {m_val}m"
                    ep_str   = f" Ep {ep}" if ep != "?" else ""
                    air_time = air_dt.strftime("%a %d %b %I:%M %p")
                    lines.append(
                        f"<blockquote>{icon} <b>{title}</b>{ep_str}\n"
                        f"  🕐 {air_time} — <i>{time_str}</i></blockquote>\n"
                    )
                airing_text = "\n".join(lines)

            if len(airing_text) > 3800:
                airing_text = airing_text[:3797] + "…"

            # Build pagination keyboard
            nav_row = []
            if air_page > 0:
                nav_row.append(InlineKeyboardButton(
                    "◀ Back", callback_data=f"menu:airing:{air_page - 1}"
                ))
            nav_row.append(InlineKeyboardButton(
                f"{air_page + 1}/{total_pages}", callback_data="noop"
            ))
            if air_page < total_pages - 1:
                nav_row.append(InlineKeyboardButton(
                    "Next ▶", callback_data=f"menu:airing:{air_page + 1}"
                ))
            kb_air = InlineKeyboardMarkup([
                nav_row,
                [InlineKeyboardButton("🔄 Refresh", callback_data="menu:airing:0")],
                [InlineKeyboardButton("◀ Back",     callback_data="menu:back")],
            ])
            await self.safe_edit(
                query, airing_text,
                reply_markup=kb_air,
                parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:reminders":
            await self.safe_edit(
                query,
                "\U0001f514 <b>Reminder Settings</b>\n\n"
                "Subscribe to get notified when episodes air:",
                reply_markup=self._kb_reminders(),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:filter":
            flt = await self.db.get_filter(chat_id)
            await self.safe_edit(
                query,
                self._filter_text(flt),
                reply_markup=self._kb_filter(flt, section="main"),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:assigned":
            await self.safe_edit(
                query,
                "📋 <b>Animes Assigned</b>\n\nSelect a day to view its assigned anime list:",
                reply_markup=self._kb_assigned(),
                parse_mode=enums.ParseMode.HTML,
            )
            await query.answer()

        elif data.startswith("assigned:pg:"):
            # assigned:pg:<day>:<page>
            parts_ = data.split(":")
            day  = parts_[2]
            pg   = int(parts_[3]) if len(parts_) > 3 else 0
            await self._show_assigned_day(query, chat_id, day, pg)
            await query.answer()

        elif data.startswith("assigned:"):
            day = data.split(":", 1)[1]
            await self._show_assigned_day(query, chat_id, day, 0)
            await query.answer()

        elif data == "menu:weekly":
            await query.answer("Loading\u2026")
            schedule = await self._get_schedule(chat_id)
            lines    = ["\U0001f4ca <b>Weekly Anime Overview</b>\n"]
            for d in DAYS_OF_WEEK:
                lines.append(f"  <b>{d}:</b> {len(schedule.get(d, []))} anime")
            await self.safe_edit(
                query,
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u25c0 Back", callback_data="menu:back")]]
                ),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data.startswith("day:"):
            # Format: day:{day}:{year}:{week}:{pg}
            parts  = data.split(":")
            day    = parts[1]
            d_year = int(parts[2]) if len(parts) > 2 else None
            d_week = int(parts[3]) if len(parts) > 3 else None
            pg     = int(parts[4]) if len(parts) > 4 else 0

            await query.answer(f"Loading {day}\u2026")
            schedule    = await self._get_schedule(chat_id, year=d_year, week=d_week)
            flt         = await self.db.get_filter(chat_id)
            pages       = ScheduleProcessor.day_pages(day, schedule.get(day, []), flt=flt, year=d_year, week=d_week)
            total_pages = len(pages)
            pg          = max(0, min(pg, total_pages - 1))
            text        = pages[pg]

            # Back button returns to week view for the same year/week
            back_data = (f"sched:week:{d_year}:{d_week}"
                         if d_year and d_week else "menu:schedule")

            # Always show pagination row (season-style)
            nav: list = []
            page_row: list = []
            if pg > 0:
                page_row.append(InlineKeyboardButton(
                    "\u25c4 Prev",
                    callback_data=f"day:{day}:{d_year}:{d_week}:{pg - 1}",
                ))
            page_row.append(InlineKeyboardButton(
                f"{pg + 1}/{total_pages}", callback_data="noop"
            ))
            if pg < total_pages - 1:
                page_row.append(InlineKeyboardButton(
                    "Next \u25ba",
                    callback_data=f"day:{day}:{d_year}:{d_week}:{pg + 1}",
                ))
            nav.append(page_row)
            nav.append([InlineKeyboardButton("\u25c0 Back", callback_data=back_data)])

            await self.safe_edit(
                query, text,
                reply_markup=InlineKeyboardMarkup(nav),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data == "rem:on":
            await self.db.subscribe(chat_id)
            await query.answer("\u2705 Subscribed to episode alerts!", show_alert=True)

        elif data == "rem:off":
            await self.db.unsubscribe(chat_id)
            await query.answer("\u274c Unsubscribed from alerts.", show_alert=True)

        elif data == "rem:status":
            sub = await self.db.is_subscribed(chat_id)
            await query.answer(
                "\u2705 Currently subscribed" if sub else "\u274c Not subscribed",
                show_alert=True,
            )

        elif data.startswith("sched:week:"):
            # sched:week:<year>:<week>
            parts  = data.split(":")
            s_year = int(parts[2])
            s_week = int(parts[3])
            await query.answer(f"Week {s_week}, {s_year}")
            await self.safe_edit(
                query,
                f"\U0001f4c5 <b>Select a day</b> \u2014 Week {s_week}, {s_year}:",
                reply_markup=self._kb_days(s_year, s_week),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data.startswith("flt:"):
            # Routing table:
            #   flt:main               — show main filter menu
            #   flt:section:<name>     — open sub-panel (airtype/mediatype/streams/other)
            #   flt:air:<key>          — toggle air type (raw/sub/dub)
            #   flt:mt:<slug>          — toggle media type slug (or "reset")
            #   flt:st:<slug>          — toggle stream slug (or "reset")
            #   flt:other:<key>        — toggle other flags (donghua)
            #   flt:reset              — reset entire filter to defaults
            #   flt:close              — delete the filter message
            parts  = data.split(":")
            action = parts[1] if len(parts) > 1 else ""

            async def _refresh(section: str = "main"):
                flt = await self.db.get_filter(chat_id)
                await self.safe_edit(
                    query,
                    self._filter_text(flt),
                    reply_markup=self._kb_filter(flt, section=section),
                    parse_mode=enums.ParseMode.HTML,
                )

            if action == "main":
                await _refresh("main")
                await query.answer()

            elif action == "section":
                section = parts[2] if len(parts) > 2 else "main"
                await _refresh(section)
                await query.answer()

            elif action == "air":
                key = parts[2] if len(parts) > 2 else ""
                if key in ("raw", "sub", "dub"):
                    flt = await self.db.get_filter(chat_id)
                    if key in flt.air_types:
                        flt.air_types.discard(key)
                    else:
                        flt.air_types.add(key)
                    # At least one air type must stay on
                    if not flt.air_types:
                        flt.air_types = {"raw", "sub", "dub"}
                        await query.answer(
                            "⚠️ At least one air type must be enabled.",
                            show_alert=True,
                        )
                    else:
                        await self.db.set_filter(chat_id, flt)
                        await query.answer(
                            f"{'Enabled' if key in flt.air_types else 'Disabled'} "
                            f"{key.capitalize()}"
                        )
                    await self.safe_edit(
                        query,
                        self._filter_text(flt),
                        reply_markup=self._kb_filter(flt, section="airtype"),
                        parse_mode=enums.ParseMode.HTML,
                    )

            elif action == "mt":
                slug = parts[2] if len(parts) > 2 else ""
                flt  = await self.db.get_filter(chat_id)
                if slug == "reset":
                    flt.media_types = set()
                    await query.answer("✅ Showing all media types")
                elif slug in MEDIA_TYPES:
                    if slug in flt.media_types:
                        flt.media_types.discard(slug)
                        await query.answer(f"Removed: {MEDIA_TYPES[slug]}")
                    else:
                        flt.media_types.add(slug)
                        await query.answer(f"Added: {MEDIA_TYPES[slug]}")
                await self.db.set_filter(chat_id, flt)
                await self.safe_edit(
                    query,
                    self._filter_text(flt),
                    reply_markup=self._kb_filter(flt, section="mediatype"),
                    parse_mode=enums.ParseMode.HTML,
                )

            elif action == "st":
                slug = parts[2] if len(parts) > 2 else ""
                flt  = await self.db.get_filter(chat_id)
                if slug == "reset":
                    flt.streams = set()
                    await query.answer("✅ Showing all platforms")
                elif slug in STREAMS:
                    if slug in flt.streams:
                        flt.streams.discard(slug)
                        await query.answer(f"Removed: {STREAMS[slug]}")
                    else:
                        flt.streams.add(slug)
                        await query.answer(f"Added: {STREAMS[slug]}")
                await self.db.set_filter(chat_id, flt)
                await self.safe_edit(
                    query,
                    self._filter_text(flt),
                    reply_markup=self._kb_filter(flt, section="streams"),
                    parse_mode=enums.ParseMode.HTML,
                )

            elif action == "other":
                key = parts[2] if len(parts) > 2 else ""
                flt = await self.db.get_filter(chat_id)
                if key == "donghua":
                    flt.hide_donghua = not flt.hide_donghua
                    state = "hidden" if flt.hide_donghua else "shown"
                    await query.answer(f"Donghua: {state}")
                await self.db.set_filter(chat_id, flt)
                await self.safe_edit(
                    query,
                    self._filter_text(flt),
                    reply_markup=self._kb_filter(flt, section="other"),
                    parse_mode=enums.ParseMode.HTML,
                )

            elif action == "reset":
                await self.db.reset_filter(chat_id)
                flt = ChatFilter()
                await self.safe_edit(
                    query,
                    self._filter_text(flt),
                    reply_markup=self._kb_filter(flt, section="main"),
                    parse_mode=enums.ParseMode.HTML,
                )
                await query.answer("✅ All filters reset to default")

            elif action == "close":
                await query.message.delete()

            else:
                await query.answer()

        elif data.startswith("al:"):
            # al:close                     — delete the current message
            # al:anime:<id>                — show anime card (delete search list)
            # al:manga:<id>                — show manga card (delete search list)
            # al:relations:<id>            — show full relation chain
            # al:back:anime:<id>           — re-show search list, delete card
            # al:back:manga:<id>           — re-show search list, delete card
            parts   = data.split(":")
            al_type = parts[1] if len(parts) > 1 else ""

            if al_type == "close":
                try:
                    await query.message.delete()
                except Exception:
                    pass
                return

            if al_type == "characters":
                # al:characters:<media_type>:<id>
                m_type   = parts[2] if len(parts) > 2 else "anime"
                media_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None
                if not media_id:
                    await query.answer("Invalid ID.", show_alert=True)
                    return
                await query.answer("Loading characters…")
                if m_type == "anime":
                    r = await self.al_api.get_anime_by_id(media_id)
                else:
                    r = await self.al_api.get_manga_by_id(media_id)
                if not r:
                    await query.answer("❌ Not found.", show_alert=True)
                    return
                chars = (r.get("characters") or {}).get("nodes") or []
                if not chars:
                    await query.answer("No characters data available.", show_alert=True)
                    return
                t      = r.get("title", {})
                title  = t.get("english") or t.get("romaji") or "Unknown"
                lines  = [f"👤 <b>Characters — {title}</b>\n"]
                for c in chars[:6]:
                    n  = c.get("name") or {}
                    lines.append(f"• {n.get('full') or 'Unknown'}")
                text = "\n".join(lines)
                kb   = InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀ Back", callback_data=f"al:{m_type}:{media_id}"),
                ]])
                try:
                    await query.message.edit_text(
                        text, reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                    )
                except Exception:
                    await query.message.reply(
                        text, reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                    )
                return

            if al_type == "relations":
                # al:relations:<id>
                media_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
                if not media_id:
                    await query.answer("Invalid ID.", show_alert=True)
                    return
                await query.answer("Loading relations…")
                data = await self.al_api.get_full_relations(media_id)
                text = self._format_relations(data)
                # Send as a new message — relations can be long
                try:
                    if len(text) <= 4096:
                        await query.message.reply(
                            text, parse_mode=enums.ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    else:
                        chunks = []
                        current = ""
                        for line in text.split("\n"):
                            if len(current) + len(line) + 1 > 4000:
                                chunks.append(current)
                                current = line + "\n"
                            else:
                                current += line + "\n"
                        if current.strip():
                            chunks.append(current)
                        for chunk in chunks:
                            await query.message.reply(
                                chunk.strip(), parse_mode=enums.ParseMode.HTML,
                                disable_web_page_preview=True,
                            )
                except Exception as exc:
                    logger.warning("Relations send error: %s", exc)
                return

            if al_type in ("anime", "manga"):
                al_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
                if not al_id:
                    await query.answer("Invalid ID.", show_alert=True)
                    return
                await query.answer("Loading…")
                # query.message is the search-list message — delete it, send card as new msg
                search_msg = query.message
                if al_type == "anime":
                    r = await self.al_api.get_anime_by_id(al_id)
                    if r:
                        await self._send_anime_card(search_msg, r, search_msg=search_msg, al_type="anime")
                    else:
                        await query.answer("❌ Not found.", show_alert=True)
                else:
                    r = await self.al_api.get_manga_by_id(al_id)
                    if r:
                        await self._send_manga_card(search_msg, r, search_msg=search_msg, al_type="manga")
                    else:
                        await query.answer("❌ Not found.", show_alert=True)
                return

            if al_type == "back":
                # al:back:<media_type>:<id>
                # Restore the search list from cache (populated when /anime or /manga ran).
                await query.answer()
                sub = parts[2] if len(parts) > 2 else "anime"   # "anime" or "manga"
                cache_key = f"al:search:{sub}:{chat_id}"
                cached = await self.db.get_cache(cache_key)
                if cached:
                    q       = cached.get("q", "")
                    results = cached.get("results", [])
                    rows    = self._al_search_rows(results, sub)
                    try:
                        await query.message.edit_text(
                            f"\U0001f50d <b>Results for \"{q}\"</b>\nTap a title to view details:",
                            reply_markup=InlineKeyboardMarkup(rows),
                            parse_mode=enums.ParseMode.HTML,
                        )
                        return
                    except Exception:
                        pass
                # Fallback: just delete the card if cache is gone
                try:
                    await query.message.delete()
                except Exception:
                    pass
                return

        elif data.startswith("season:"):
            # season:<year>:<season>:<page>
            parts = data.split(":")
            if len(parts) == 4:
                s_year   = int(parts[1])
                s_season = parts[2]
                s_page   = int(parts[3])
                await query.answer(f"Loading page {s_page + 1}…")
                titles = await self.season.get_season(s_year, s_season)
                if not titles:
                    await query.answer("No data available.", show_alert=True)
                    return
                text, total_pages = SeasonScraper.paginate(titles, s_year, s_season, page=s_page)
                kb = self._kb_season(s_year, s_season, s_page, total_pages)
                await self.safe_edit(
                    query, text, reply_markup=kb, parse_mode=enums.ParseMode.HTML
                )
            else:
                await query.answer()

        elif data.startswith("ts:"):
            # Topic settings callbacks
            # ts:set:<mode>:<cid>:<tid>      — assign mode to topic
            # ts:clear:<cid>:<tid>           — clear topic mode
            # ts:cfg:rem:<cid>:<tid>         — open rem sub-settings
            # ts:cfg:nyaa:<cid>:<tid>        — open nyaa sub-settings
            # ts:rem:<key>:<cid>:<tid>       — toggle rem sub-setting
            # ts:nyaa:<key>:<cid>:<tid>      — toggle nyaa sub-setting
            # ts:back:<cid>:<tid>            — back to topic main panel
            # ts:close                       — delete the panel

            parts  = data.split(":")
            action = parts[1] if len(parts) > 1 else ""

            if action == "close":
                try:
                    await query.message.delete()
                except Exception:
                    pass
                await query.answer()
                return

            # All actions (except close) encode <cid> and <tid> as the last two parts
            try:
                t_cid = int(parts[-2])
                t_tid = int(parts[-1])
            except (ValueError, IndexError):
                await query.answer("Invalid callback data.", show_alert=True)
                return

            # Verify the clicking user is admin of the TARGET group (t_cid).
            # Must check AFTER extracting t_cid so we verify the right chat.
            # query.from_user is the person who pressed the button (not the bot).
            cb_uid = query.from_user.id if query.from_user else None
            if not cb_uid or not await self._is_admin_of_chat(cb_uid, t_cid):
                await query.answer("⛔ You must be an admin of that group.", show_alert=True)
                return

            if action == "set":
                t_mode = parts[2] if len(parts) > 2 else ""
                if t_mode in ("rem", "nyaa"):
                    await self.db.set_topic_mode(t_cid, t_tid, t_mode)
                    await query.answer(f"✅ Mode set to {'Reminders' if t_mode == 'rem' else 'Nyaa'}")
                # Re-read mode from DB so panel always reflects stored state
                saved_mode = await self.db.get_topic_mode(t_cid, t_tid)
                text, kb = self._topic_settings_panel(t_tid, saved_mode, t_cid)
                await self.safe_edit(query, text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

            elif action == "clear":
                # Unset the topic key entirely rather than storing empty string
                await self.db.chats.update_one(
                    {"chat_id": t_cid},
                    {"$unset": {f"topics.{t_tid}": ""}},
                )
                await query.answer("🚫 Topic mode cleared.")
                text, kb = self._topic_settings_panel(t_tid, None, t_cid)
                await self.safe_edit(query, text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

            elif action == "cfg":
                sub = parts[2] if len(parts) > 2 else ""
                cfg = await self.db.get_topic_cfg(t_cid, t_tid)
                if sub == "rem":
                    _rem_defaults = {"show_raw": True, "show_sub": True, "show_dub": True}
                    rem_cfg = {**_rem_defaults, **cfg.get("rem", {})}
                    await self.safe_edit(
                        query,
                        f"🔧 <b>Reminder Settings</b> — Topic <code>{t_tid}</code>\n\n"
                        "<b>Air Types</b> — toggle which versions this topic receives:\n"
                        "<i>Schedule Filter</i> — filter shows by media type, stream, donghua:",
                        reply_markup=self._rem_settings_kb(t_tid, rem_cfg, t_cid),
                        parse_mode=enums.ParseMode.HTML,
                    )
                elif sub == "nyaa":
                    nyaa_cfg = cfg.get("nyaa", {"varyg": True, "toonshub": True, "subsplease": True})
                    await self.safe_edit(
                        query,
                        f"🔧 <b>Nyaa Settings</b> — Topic <code>{t_tid}</code>\n\n"
                        "Toggle which uploaders this topic receives:",
                        reply_markup=self._nyaa_settings_kb(t_tid, nyaa_cfg, t_cid),
                        parse_mode=enums.ParseMode.HTML,
                    )
                await query.answer()

            elif action == "rem":
                key = parts[2] if len(parts) > 2 else ""
                if key in ("raw", "sub", "dub"):
                    cfg = await self.db.get_topic_cfg(t_cid, t_tid)
                    # Always initialise ALL three keys so partial DB saves
                    # (e.g. only "show_sub" present) don't leave raw/dub
                    # defaulting to True and bypassing the filter.
                    _defaults = {"show_raw": True, "show_sub": True, "show_dub": True}
                    rem_cfg = {**_defaults, **cfg.get("rem", {})}
                    field = f"show_{key}"
                    rem_cfg[field] = not rem_cfg[field]
                    cfg["rem"] = rem_cfg
                    await self.db.set_topic_cfg(t_cid, t_tid, cfg)
                    await query.answer(f"{'✅' if rem_cfg[field] else '⬜'} {key.capitalize()} {'on' if rem_cfg[field] else 'off'}")
                    await self.safe_edit(
                        query,
                        f"🔧 <b>Reminder Settings</b> — Topic <code>{t_tid}</code>\n\n"
                        "Toggle which air types this topic receives:",
                        reply_markup=self._rem_settings_kb(t_tid, rem_cfg, t_cid),
                        parse_mode=enums.ParseMode.HTML,
                    )

            elif action == "nyaa":
                key = parts[2] if len(parts) > 2 else ""
                if key in ("varyg", "toonshub", "subsplease"):
                    cfg = await self.db.get_topic_cfg(t_cid, t_tid)
                    _nyaa_defaults = {"varyg": True, "toonshub": True, "subsplease": True}
                    nyaa_cfg = {**_nyaa_defaults, **cfg.get("nyaa", {})}
                    nyaa_cfg[key] = not nyaa_cfg[key]
                    cfg["nyaa"] = nyaa_cfg
                    await self.db.set_topic_cfg(t_cid, t_tid, cfg)
                    lbl = {"varyg": "varyg1001", "toonshub": "ToonsHub", "subsplease": "subsplease"}.get(key, key)
                    await query.answer(f"{'✅' if nyaa_cfg[key] else '⬜'} {lbl} {'on' if nyaa_cfg[key] else 'off'}")
                    await self.safe_edit(
                        query,
                        f"🔧 <b>Nyaa Settings</b> — Topic <code>{t_tid}</code>\n\n"
                        "Toggle which uploaders this topic receives:",
                        reply_markup=self._nyaa_settings_kb(t_tid, nyaa_cfg, t_cid),
                        parse_mode=enums.ParseMode.HTML,
                    )

            elif action == "back":
                current_mode = await self.db.get_topic_mode(t_cid, t_tid)
                text, kb = self._topic_settings_panel(t_tid, current_mode, t_cid)
                await self.safe_edit(query, text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)
                await query.answer()

            elif action == "flt":
                # ts:flt:<sub>:...:<cid>:<tid>
                # sub: main, section, air, mt, st, other, reset
                # cid and tid are always the last two parts
                sub = parts[2] if len(parts) > 2 else "main"

                async def _flt_refresh(sec: str):
                    flt = await self.db.get_topic_filter(t_cid, t_tid)
                    await self.safe_edit(
                        query,
                        f"🎛 <b>Schedule Filter</b> — Topic <code>{t_tid}</code>\n\n"
                        + AnimeBot._filter_text(flt),
                        reply_markup=self._kb_topic_filter(flt, sec, t_cid, t_tid),
                        parse_mode=enums.ParseMode.HTML,
                    )

                if sub == "main":
                    await _flt_refresh("main")
                    await query.answer()

                elif sub == "section":
                    sec = parts[3] if len(parts) > 3 else "main"
                    await _flt_refresh(sec)
                    await query.answer()

                elif sub == "air":
                    key = parts[3] if len(parts) > 3 else ""
                    if key in ("raw", "sub", "dub"):
                        flt = await self.db.get_topic_filter(t_cid, t_tid)
                        if key in flt.air_types:
                            flt.air_types.discard(key)
                        else:
                            flt.air_types.add(key)
                        if not flt.air_types:
                            flt.air_types = {"raw", "sub", "dub"}
                            await query.answer("⚠️ At least one air type must be enabled.", show_alert=True)
                        else:
                            await self.db.set_topic_filter(t_cid, t_tid, flt)
                            await query.answer(f"{'Enabled' if key in flt.air_types else 'Disabled'} {key.capitalize()}")
                        await _flt_refresh("airtype")

                elif sub == "mt":
                    slug = parts[3] if len(parts) > 3 else ""
                    flt  = await self.db.get_topic_filter(t_cid, t_tid)
                    if slug == "reset":
                        flt.media_types = set()
                        await query.answer("✅ Showing all media types")
                    elif slug in MEDIA_TYPES:
                        if slug in flt.media_types:
                            flt.media_types.discard(slug)
                            await query.answer(f"Removed: {MEDIA_TYPES[slug]}")
                        else:
                            flt.media_types.add(slug)
                            await query.answer(f"Added: {MEDIA_TYPES[slug]}")
                    await self.db.set_topic_filter(t_cid, t_tid, flt)
                    await _flt_refresh("mediatype")

                elif sub == "st":
                    slug = parts[3] if len(parts) > 3 else ""
                    flt  = await self.db.get_topic_filter(t_cid, t_tid)
                    if slug == "reset":
                        flt.streams = set()
                        await query.answer("✅ Showing all platforms")
                    elif slug in STREAMS:
                        if slug in flt.streams:
                            flt.streams.discard(slug)
                            await query.answer(f"Removed: {STREAMS[slug]}")
                        else:
                            flt.streams.add(slug)
                            await query.answer(f"Added: {STREAMS[slug]}")
                    await self.db.set_topic_filter(t_cid, t_tid, flt)
                    await _flt_refresh("streams")

                elif sub == "other":
                    key = parts[3] if len(parts) > 3 else ""
                    flt = await self.db.get_topic_filter(t_cid, t_tid)
                    if key == "donghua":
                        flt.hide_donghua = not flt.hide_donghua
                        await query.answer(f"Donghua: {'hidden' if flt.hide_donghua else 'shown'}")
                    await self.db.set_topic_filter(t_cid, t_tid, flt)
                    await _flt_refresh("other")

                elif sub == "reset":
                    await self.db.reset_topic_filter(t_cid, t_tid)
                    await query.answer("✅ Topic filter reset to defaults")
                    await _flt_refresh("main")

                else:
                    await query.answer()

            else:
                await query.answer()

        elif data == "noop":
            await query.answer()

        # ── bnr_cat:{action}:{category} — category picker tapped ─────────────
        elif data.startswith("bnr_cat:"):
            # Emitted by BannerDB.category_picker_kb()
            # action = "save"   → complete the pending /save_banner flow
            # action = "browse" → show banner list for chosen category
            parts  = data.split(":", 2)
            action = parts[1] if len(parts) > 1 else ""
            cat    = parts[2] if len(parts) > 2 else ""
            uid    = query.from_user.id if query.from_user else query.message.chat.id

            if action == "save":
                pending = self.banner_db.get_pending(uid)
                if not pending:
                    await query.answer(
                        "⏱ Session expired — please run /save_banner again.",
                        show_alert=True,
                    )
                    try:
                        await query.message.delete()
                    except Exception:
                        pass
                    return

                await query.answer(f"💾 Saving in {cat.capitalize()}…")
                result = await self.banner_db.save(
                    self.app,
                    pending["name"],
                    cat,
                    pending["file_id"],
                    pending["file_type"],
                )
                self.banner_db.clear_pending(uid)
                try:
                    await query.message.delete()
                except Exception:
                    pass

                if result.get("ok"):
                    verb = "updated" if result.get("updated") else "saved"
                    await query.message.reply(
                        f'✅ Banner {verb}: "<b>{pending["name"]}</b>" '
                        f'in <b>{cat.capitalize()}</b>',
                        parse_mode=enums.ParseMode.HTML,
                    )
                else:
                    await query.message.reply(
                        f"❌ Failed to save banner: {result.get('error', 'unknown error')}",
                        parse_mode=enums.ParseMode.HTML,
                    )

            elif action == "browse":
                await query.answer()
                try:
                    await query.message.delete()
                except Exception:
                    pass
                banners = await self.banner_db.search("", cat)
                if not banners:
                    await query.message.reply(
                        f"❌ No banners in <b>{cat.capitalize()}</b> yet.",
                        parse_mode=enums.ParseMode.HTML,
                    )
                    return
                page_banners, total_pages = self.banner_db.paginate(banners, 0)
                kb  = self.banner_db.banner_keyboard(page_banners, 0, total_pages, "", cat)
                txt = self.banner_db.banner_list_text(
                    page_banners, 0, len(banners), "", total_pages, cat
                )
                await query.message.reply(
                    txt, reply_markup=kb, parse_mode=enums.ParseMode.HTML
                )
            else:
                await query.answer()

        # ── banner:close — delete the banner list message ─────────────────────
        elif data == "banner:close":
            try:
                await query.message.delete()
            except Exception:
                pass
            await query.answer()

        # ── banner:show:<msg_id> — legacy backward compat ─────────────────────
        elif data.startswith("banner:"):
            bparts  = data.split(":", 3)
            baction = bparts[1] if len(bparts) > 1 else ""
            if baction == "show":
                msg_id = int(bparts[2]) if len(bparts) > 2 and bparts[2].isdigit() else None
                if not msg_id or not BANNER_DB_CHAT:
                    await query.answer("❌ Banner not found.", show_alert=True)
                    return
                await query.answer("📤 Sending…")
                try:
                    tg_msg = await self.banner_db.fetch_media(self.app, msg_id)
                    if tg_msg and tg_msg.photo:
                        await query.message.reply_photo(
                            photo=tg_msg.photo.file_id,
                            caption=f"🖼 {tg_msg.caption or ''}",
                            parse_mode=enums.ParseMode.HTML,
                        )
                    elif tg_msg and tg_msg.document:
                        await query.message.reply_document(
                            document=tg_msg.document.file_id,
                            caption=f"🖼 {tg_msg.caption or ''}",
                            parse_mode=enums.ParseMode.HTML,
                        )
                    else:
                        await query.answer("❌ Banner media not found.", show_alert=True)
                except Exception as exc:
                    logger.error("Banner show error: %s", exc)
                    await query.answer("❌ Failed to retrieve banner.", show_alert=True)
            else:
                await query.answer()

        # ── banner_select:{cat}:{q}:{page}:{idx} — user tapped a number ──────
        elif data.startswith("bnr_cat:"):
            # ── Category picker: bnr_cat:{action}:{category} ──────────────
            # action = "save"   → complete the pending save_banner flow
            # action = "browse" → show banner list for that category
            parts  = data.split(":", 2)
            action = parts[1] if len(parts) > 1 else ""
            cat    = parts[2] if len(parts) > 2 else ""
            uid    = query.from_user.id if query.from_user else query.message.chat.id

            if action == "save":
                # Retrieve the pending file data set by cmd_save_banner
                pending = self.banner_db.get_pending(uid)
                if not pending:
                    await query.answer("⏰ Session expired. Run /save_banner again.", show_alert=True)
                    try:
                        await query.message.delete()
                    except Exception:
                        pass
                    return
                await query.answer(f"💾 Saving in {cat.capitalize()}…")
                result = await self.banner_db.save(
                    self.app,
                    pending["name"],
                    cat,
                    pending["file_id"],
                    pending["file_type"],
                )
                self.banner_db.clear_pending(uid)
                try:
                    await query.message.delete()
                except Exception:
                    pass
                if result.get("ok"):
                    verb = "updated" if result.get("updated") else "saved"
                    await query.message.reply(
                        f'✅ Banner {verb}: "<b>{pending["name"]}</b>" '
                        f'in <b>{cat.capitalize()}</b>',
                        parse_mode=enums.ParseMode.HTML,
                    )
                else:
                    await query.message.reply(
                        f"❌ Save failed: {result.get('error', 'unknown error')}",
                        parse_mode=enums.ParseMode.HTML,
                    )

            elif action == "browse":
                await query.answer()
                try:
                    await query.message.delete()
                except Exception:
                    pass
                # Fetch + show banners in that category
                banners = await self.banner_db.search("", cat)
                if not banners:
                    await query.message.reply(
                        f"❌ No banners in <b>{cat.capitalize()}</b> yet.\n\n"
                        f"Save one with <code>/save_banner</code>.",
                        parse_mode=enums.ParseMode.HTML,
                    )
                    return
                page_banners, total_pages = self.banner_db.paginate(banners, 0)
                kb  = self.banner_db.banner_keyboard(page_banners, 0, total_pages, "", cat)
                txt = self.banner_db.banner_list_text(
                    page_banners, 0, len(banners), "", total_pages, cat
                )
                await query.message.reply(txt, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

            else:
                await query.answer()

        elif data.startswith("banner_select:"):
            # ── User tapped a number → retrieve + send that banner ─────────
            # Format: banner_select:{cat}:{q}:{page}:{idx}
            parts = data.split(":", 4)
            cat_enc = parts[1] if len(parts) > 1 else ""
            q_enc   = parts[2] if len(parts) > 2 else ""
            try:
                bpage = int(parts[3]) if len(parts) > 3 else 0
                bidx  = int(parts[4]) if len(parts) > 4 else 0
            except (ValueError, IndexError):
                await query.answer("❌ Invalid selection.", show_alert=True)
                return

            await query.answer("📤 Sending banner…")

            # Re-fetch the same page from MongoDB to find the correct banner
            banners = await self.banner_db.search(q_enc, cat_enc)
            if not banners:
                await query.answer("❌ No banners found.", show_alert=True)
                return

            page_banners, _ = self.banner_db.paginate(banners, bpage)
            if bidx < 0 or bidx >= len(page_banners):
                await query.answer("❌ Invalid banner index.", show_alert=True)
                return

            banner_doc = page_banners[bidx]
            msg_id     = banner_doc.get("msg_id") or banner_doc.get("message_id")
            name       = banner_doc.get("name", "banner")
            cat_label  = banner_doc.get("category", "")
            ftype      = banner_doc.get("file_type", "photo")

            if not msg_id or not BANNER_DB_CHAT:
                await query.answer("❌ Banner DB not configured or no msg_id.", show_alert=True)
                return

            # Fetch the stored media message from DB channel
            tg_msg = await self.banner_db.fetch_media(self.app, msg_id)
            if not tg_msg:
                await query.message.reply(
                    f"❌ Banner <b>{name}</b> media not found in DB channel.",
                    parse_mode=enums.ParseMode.HTML,
                )
                return

            caption = f"🖼 <b>{name}</b>"
            if cat_label:
                caption += f"\n📂 {cat_label.capitalize()}"

            try:
                if tg_msg.photo:
                    await query.message.reply_photo(
                        photo=tg_msg.photo.file_id,
                        caption=caption,
                        parse_mode=enums.ParseMode.HTML,
                    )
                elif tg_msg.document:
                    await query.message.reply_document(
                        document=tg_msg.document.file_id,
                        caption=caption,
                        parse_mode=enums.ParseMode.HTML,
                    )
                else:
                    await query.message.reply(
                        f"❌ Banner <b>{name}</b> has no photo or document.",
                        parse_mode=enums.ParseMode.HTML,
                    )
                # Delete the banner list message after successfully sending the banner
                try:
                    await query.message.delete()
                except Exception:
                    pass
            except Exception as exc:
                logger.error("banner_select send error: %s", exc)
                await query.answer("❌ Failed to send banner.", show_alert=True)

        elif data.startswith("banner_page:"):
            # ── Pagination: banner_page:{cat}:{q}:{page} ─────────────────
            parts = data.split(":", 3)
            cat_enc = parts[1] if len(parts) > 1 else ""
            q_enc   = parts[2] if len(parts) > 2 else ""
            try:
                bpage = int(parts[3]) if len(parts) > 3 else 0
            except ValueError:
                bpage = 0

            await query.answer(f"Page {bpage + 1}…")
            banners = await self.banner_db.search(q_enc, cat_enc)
            if not banners:
                await query.answer("No banners found.", show_alert=True)
                return

            page_banners, total_pages = self.banner_db.paginate(banners, bpage)
            kb  = self.banner_db.banner_keyboard(
                page_banners, bpage, total_pages, q_enc, cat_enc
            )
            txt = self.banner_db.banner_list_text(
                page_banners, bpage, len(banners), q_enc, total_pages, cat_enc
            )
            await self.safe_edit(query, txt, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

        elif data == "banner:close":
            # ── Close / delete the banner list message ────────────────────
            try:
                await query.message.delete()
            except Exception:
                pass
            await query.answer()

        elif data.startswith("srch:"):
            # Formats:
            #   srch:cancel                          — dismiss picker
            #   srch:{source}:{query}                — first search (page 0)
            #   srch:{source}:{query}:{page}         — paginated search
            parts    = data.split(":", 3)
            s_action = parts[1] if len(parts) > 1 else ""
            s_query  = parts[2] if len(parts) > 2 else ""
            # Page is the optional 4th segment
            try:
                s_page = int(parts[3]) if len(parts) > 3 else 0
            except (ValueError, IndexError):
                s_page = 0

            if s_action == "cancel":
                try:
                    await query.message.delete()
                except Exception:
                    pass
                await query.answer()

            elif s_action in ("varyg", "toons", "subsp", "all"):
                if not s_query:
                    await query.answer("No query provided.", show_alert=True)
                    return
                try:
                    await self._do_torrent_search(query, s_action, s_query, page=s_page)
                except Exception as exc:
                    logger.error("srch callback error: %s", exc)
                    await query.answer("❌ Search failed.", show_alert=True)
            else:
                await query.answer()

        else:
            await query.answer("Unknown action.")

    # ── /filter ───────────────────────────────────────────────────────────
    # ── /set<day> commands ───────────────────────────────────────────────
    async def cmd_set_day(self, _, msg: Message):
        """
        /setmonday <text>   — save a custom anime list message for Monday
        /settuesday <text>  — etc.
        Used by /settings → Animes Assigned panel.

        All Telegram formatting (bold, italic, underline, code, links,
        blockquote, strikethrough, spoiler) is preserved — entities are
        converted to HTML before saving so they render in the /assigned panel.
        """
        if not await self._auth(msg):
            return
        if not await self._is_group_admin(msg) and not await self._is_bot_admin(msg):
            await msg.reply("⛔ Only group admins can use this.")
            return
        cmd = msg.command[0].lower()   # e.g. "setmonday"
        day = cmd[3:]                  # strip "set" → "monday"

        raw = msg.text or msg.caption or ""
        prefix_end = raw.find(" ")
        plain_body = raw[prefix_end:].strip() if prefix_end != -1 else ""

        if not plain_body:
            await msg.reply(
                f"Usage: <code>/{cmd} Your anime list here...</code>\n\n"
                f"✅ <b>Formatting is fully supported.</b>\n"
                f"Use Telegram's built-in formatting toolbar (bold, italic, "
                f"mono, links, blockquote, etc.) — everything is preserved.\n\n"
                f"Example:\n"
                f"<code>/{cmd} </code><b>Isekai:</b>\n"
                f"<code>• Tensura S3\n• Re:Zero S3</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        # Convert Telegram message entities → HTML so all formatting
        # survives storage and renders correctly in _show_assigned_day.
        html_body = self._entities_to_html(msg, prefix_end)

        await self.db.set_day_message(msg.chat.id, day, html_body)
        await msg.reply(
            f"✅ Message saved for <b>{day.capitalize()}</b>.\n"
            f"<i>All formatting has been preserved.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    @staticmethod
    def _entities_to_html(msg: Message, prefix_end: int) -> str:
        """
        Convert a Pyrogram Message's text + entities into an HTML string,
        starting from after the command prefix (offset prefix_end + 1).

        Handles every Telegram entity type that Pyrogram exposes:
          bold, italic, underline, strikethrough, code, pre,
          text_link, text_mention, spoiler, blockquote.
        Plain text characters are HTML-escaped so <, >, & display correctly.
        """
        from pyrogram.enums import MessageEntityType as MET

        full_text = msg.text or msg.caption or ""
        entities  = list(msg.entities or msg.caption_entities or [])

        # Body starts after the command word + space
        body_offset = prefix_end + 1 if prefix_end != -1 else 0
        text_body   = full_text[body_offset:]

        if not entities:
            # No formatting at all — just HTML-escape and return
            return (
                text_body
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )

        # Telegram entity offsets/lengths are in UTF-16 code units
        try:
            utf16 = full_text.encode("utf-16-le")
        except Exception:
            return (
                text_body
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )

        body_u16_start = len(full_text[:body_offset].encode("utf-16-le")) // 2
        total_u16      = len(utf16) // 2

        def u16_slice(s: int, e: int) -> str:
            return utf16[s * 2: e * 2].decode("utf-16-le", errors="replace")

        def esc(s: str) -> str:
            return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        # Build (utf16_pos, tag_html) insertion list
        insertions: list = []

        for ent in entities:
            off = ent.offset
            end = off + ent.length
            # Skip entities entirely before the body
            if end <= body_u16_start:
                continue
            # Clamp start to body boundary
            off = max(off, body_u16_start)

            et = ent.type
            if et == MET.BOLD:
                insertions += [(off, "<b>"), (end, "</b>")]
            elif et == MET.ITALIC:
                insertions += [(off, "<i>"), (end, "</i>")]
            elif et == MET.UNDERLINE:
                insertions += [(off, "<u>"), (end, "</u>")]
            elif et == MET.STRIKETHROUGH:
                insertions += [(off, "<s>"), (end, "</s>")]
            elif et == MET.CODE:
                insertions += [(off, "<code>"), (end, "</code>")]
            elif et == MET.PRE:
                lang = getattr(ent, "language", "") or ""
                open_tag = f'<pre language="{lang}">' if lang else "<pre>"
                insertions += [(off, open_tag), (end, "</pre>")]
            elif et == MET.TEXT_LINK:
                url = getattr(ent, "url", "") or ""
                insertions += [(off, f'<a href="{url}">'), (end, "</a>")]
            elif et == MET.TEXT_MENTION:
                user = getattr(ent, "user", None)
                uid  = getattr(user, "id", None) if user else None
                if uid:
                    insertions += [(off, f'<a href="tg://user?id={uid}">'), (end, "</a>")]
            elif et == MET.SPOILER:
                insertions += [(off, "<spoiler>"), (end, "</spoiler>")]
            elif et == MET.BLOCKQUOTE:
                insertions += [(off, "<blockquote>"), (end, "</blockquote>")]
            # MENTION, HASHTAG, URL, EMAIL, PHONE, CASHTAG → plain text (no wrapping)

        # Sort: by position; at same position opening tags before closing tags
        insertions.sort(key=lambda x: (x[0], x[1].startswith("</")))

        # Walk UTF-16 units, inserting tags at their positions
        result    = []
        ins_queue = list(insertions)
        qi        = 0

        for pos in range(body_u16_start, total_u16):
            # Flush tags positioned here
            while qi < len(ins_queue) and ins_queue[qi][0] == pos:
                result.append(ins_queue[qi][1])
                qi += 1
            result.append(esc(u16_slice(pos, pos + 1)))

        # Flush any trailing closing tags at end-of-text
        while qi < len(ins_queue):
            result.append(ins_queue[qi][1])
            qi += 1

        return "".join(result).strip()

    async def cmd_assigned(self, _, msg: Message):
        """Show the Animes Assigned panel with day buttons."""
        if not await self._auth(msg):
            return
        await msg.reply(
            "📋 <b>Animes Assigned</b>\n\nSelect a day to view its assigned anime list:",
            reply_markup=self._kb_assigned(),
            parse_mode=enums.ParseMode.HTML,
        )

    @staticmethod
    def _kb_assigned(back: str = "menu:back") -> InlineKeyboardMarkup:
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        rows = [
            [InlineKeyboardButton(days[0], callback_data=f"assigned:{days[0].lower()}"),
             InlineKeyboardButton(days[1], callback_data=f"assigned:{days[1].lower()}")],
            [InlineKeyboardButton(days[2], callback_data=f"assigned:{days[2].lower()}"),
             InlineKeyboardButton(days[3], callback_data=f"assigned:{days[3].lower()}")],
            [InlineKeyboardButton(days[4], callback_data=f"assigned:{days[4].lower()}"),
             InlineKeyboardButton(days[5], callback_data=f"assigned:{days[5].lower()}")],
            [InlineKeyboardButton(days[6], callback_data=f"assigned:{days[6].lower()}")],
            [InlineKeyboardButton("◀ Back", callback_data=back)],
        ]
        return InlineKeyboardMarkup(rows)

    # ── Assigned day helpers ─────────────────────────────────────────────
    _ASSIGNED_PAGE_LEN = 3000  # chars per page (Telegram limit ~4096)

    # HTML tags that Telegram's Bot API actually supports in messages
    _HTML_TAG_RE = re.compile(
        r"<(/?)("
        r"b|strong|i|em|u|ins|s|strike|del|"
        r"code|pre|tg-spoiler|blockquote|"
        r"a|span"
        r")(\s[^>]*)?>",
        re.IGNORECASE,
    )

    @staticmethod
    def _contains_html(text: str) -> bool:
        """Return True if text looks like it intentionally uses Telegram HTML tags."""
        return bool(AnimeBot._HTML_TAG_RE.search(text))

    @staticmethod
    def _escape_html(text: str) -> str:
        """Escape plain text so it is safe to send with parse_mode=HTML."""
        return (
            text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    @staticmethod
    def _prepare_body(text: str) -> str:
        """
        Prepare stored day-message body for HTML rendering.

        - If the text already contains valid Telegram HTML tags → use as-is
          (the admin deliberately formatted it with <b>, <i>, <code>, etc.)
        - Otherwise → escape `<`, `>`, `&` so plain-text content renders
          correctly and doesn't trip Telegram's HTML parser.
        """
        if AnimeBot._contains_html(text):
            return text          # trust the admin's HTML
        return AnimeBot._escape_html(text)

    @staticmethod
    def _paginate_assigned(header: str, body: str) -> list:
        """
        Split a prepared (HTML-safe) day message into pages that fit
        Telegram's 4096-character message limit.

        `header` must already be valid HTML (it is generated internally).
        `body`   must have been passed through _prepare_body() first.
        """
        lines = body.split("\n")
        pages = []
        current = header + "\n\n"
        for line in lines:
            candidate = current + line + "\n"
            if len(candidate) > AnimeBot._ASSIGNED_PAGE_LEN and current != header + "\n\n":
                pages.append(current.rstrip())
                current = header + " (cont.)\n\n" + line + "\n"
            else:
                current = candidate
        if current.strip() != header.strip() and current.strip() != (header + " (cont.)").strip():
            pages.append(current.rstrip())
        return pages if pages else [header + "\n\n<i>No list set yet.</i>"]

    async def _show_assigned_day(self, query, chat_id: int, day: str, pg: int):
        """
        Render a paginated assigned-day message panel.

        Stored body is run through _prepare_body() so:
          • Plain text (no HTML tags) → auto-escaped, always renders cleanly.
          • HTML-formatted text (admin used <b>, <i>, etc.) → rendered as-is.
        """
        msg_text = await self.db.get_day_message(chat_id, day)
        header = f"📅 <b>{day.capitalize()} — Animes</b>"
        if not msg_text:
            body = (
                f"{header}\n\n<i>No list set yet.</i>\n\n"
                f"Set one with:\n<code>/set{day} Your list here...</code>\n\n"
                f"<i>Tip: HTML tags like &lt;b&gt;, &lt;i&gt;, &lt;code&gt; are supported.</i>"
            )
            await self.safe_edit(
                query, body,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◀ Back", callback_data="menu:assigned")],
                ]),
                parse_mode=enums.ParseMode.HTML,
            )
            return

        # Prepare body: escape plain text OR pass HTML through unchanged
        prepared = self._prepare_body(msg_text)
        pages = self._paginate_assigned(header, prepared)
        total = len(pages)
        pg    = max(0, min(pg, total - 1))
        nav   = []
        if total > 1:
            row = []
            if pg > 0:
                row.append(InlineKeyboardButton("◀ Prev", callback_data=f"assigned:pg:{day}:{pg - 1}"))
            row.append(InlineKeyboardButton(f"{pg + 1}/{total}", callback_data="noop"))
            if pg < total - 1:
                row.append(InlineKeyboardButton("Next ▶", callback_data=f"assigned:pg:{day}:{pg + 1}"))
            nav.append(row)
        nav.append([InlineKeyboardButton("◀ Back", callback_data="menu:assigned")])

        # Send with HTML; fall back to plain text if Telegram rejects the HTML
        try:
            await self.safe_edit(
                query, pages[pg],
                reply_markup=InlineKeyboardMarkup(nav),
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            # Nuclear fallback: strip all tags and retry as plain text
            plain = re.sub(r"<[^>]+>", "", pages[pg])
            for ent, ch in [("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">")]:
                plain = plain.replace(ent, ch)
            await self.safe_edit(
                query, plain,
                reply_markup=InlineKeyboardMarkup(nav),
            )

    async def cmd_clearassigned(self, _, msg: Message):
        """/clearassigned <day> — remove the assigned anime list for a day."""
        if not await self._auth(msg):
            return
        if not await self._is_group_admin(msg) and not await self._is_bot_admin(msg):
            await msg.reply("⛔ Only group admins can use this.")
            return
        args = msg.command[1:] if len(msg.command) > 1 else []
        if not args:
            await msg.reply(
                "Usage: <code>/clearassigned &lt;day&gt;</code>\n"
                "Example: <code>/clearassigned monday</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        day = args[0].lower()
        valid = {"monday","tuesday","wednesday","thursday","friday","saturday","sunday"}
        if day not in valid:
            await msg.reply(f"❌ Invalid day. Use one of: {', '.join(sorted(valid))}")
            return
        await self.db.clear_day_message(msg.chat.id, day)
        await msg.reply(
            f"🗑 Cleared anime list for <b>{day.capitalize()}</b>.",
            parse_mode=enums.ParseMode.HTML,
        )

    # ── /relations ────────────────────────────────────────────────────────
    async def cmd_relations(self, _, msg: Message):
        """
        /relations <anime name>  — show full sequel/prequel chain + related works
        """
        if not await self._auth(msg):
            return
        q = " ".join(msg.command[1:]).strip()
        if not q:
            await msg.reply("Usage: <code>/relations &lt;anime name&gt;</code>", parse_mode=enums.ParseMode.HTML)
            return

        wait = await msg.reply(f"🔍 Fetching relations for <i>{q}</i>…", parse_mode=enums.ParseMode.HTML)

        # First search for the anime to get its ID
        results = await self.al_api.search_anime_list(q)
        if not results:
            try: await wait.delete()
            except Exception: pass
            await msg.reply("❌ No anime found.", parse_mode=enums.ParseMode.HTML)
            return

        media_id = results[0]["id"]
        data = await self.al_api.get_full_relations(media_id)

        try: await wait.delete()
        except Exception: pass

        text = self._format_relations(data)
        if len(text) <= 4096:
            await msg.reply(text, parse_mode=enums.ParseMode.HTML, disable_web_page_preview=True)
        else:
            # Split into chunks at newlines
            chunks = []
            current = ""
            for line in text.split("\n"):
                if len(current) + len(line) + 1 > 4000:
                    chunks.append(current)
                    current = line + "\n"
                else:
                    current += line + "\n"
            if current.strip():
                chunks.append(current)
            for chunk in chunks:
                await msg.reply(chunk.strip(), parse_mode=enums.ParseMode.HTML, disable_web_page_preview=True)

    @staticmethod
    def _format_relations(data: dict) -> str:
        """Format get_full_relations output into a Telegram HTML message."""
        root     = data.get("root") or {}
        timeline = data.get("timeline") or []
        other    = data.get("other") or {}

        def title_of(node: dict) -> str:
            t = node.get("title") or {}
            return t.get("english") or t.get("romaji") or "Unknown"

        def node_line(node: dict, prefix: str = "") -> str:
            name   = title_of(node)
            year   = (node.get("startDate") or {}).get("year") or ""
            eps    = node.get("episodes") or ""
            score  = node.get("averageScore") or ""
            status = (node.get("status") or "").replace("_", " ").title()
            fmt    = (node.get("format") or "").replace("_", " ").title()
            url    = node.get("siteUrl") or ""
            parts  = []
            if fmt:   parts.append(fmt)
            if eps:   parts.append(f"{eps} eps")
            if year:  parts.append(str(year))
            if score: parts.append(f"⭐{score}/100")
            if status: parts.append(status)
            meta = " · ".join(parts)
            line = f"{prefix}<a href=\"{url}\">{name}</a>"
            if meta:
                line += f"\n    <i>{meta}</i>"
            return line

        root_title = title_of(root)
        root_url   = root.get("siteUrl", "")
        lines = [f"🔗 <b>Relations — <a href=\"{root_url}\">{root_title}</a></b>\n"]

        # ── Timeline (prequel → root → sequel chain) ──────────────────────
        if timeline:
            lines.append("📅 <b>Main Story Timeline</b>")

            # Separate prequels and sequels
            prequels = [(rt, n) for rt, n in timeline if rt == "PREQUEL"]
            sequels  = [(rt, n) for rt, n in timeline if rt == "SEQUEL"]

            # Build ordered: prequels (reversed so oldest first) → root → sequels
            ordered = []
            for _, n in reversed(prequels):
                ordered.append(("PREQUEL", n))
            ordered.append(("ROOT", root))
            for _, n in sequels:
                ordered.append(("SEQUEL", n))

            for i, (kind, node) in enumerate(ordered):
                num = i + 1
                if kind == "ROOT":
                    lines.append(f"  {num}. <b>{root_title}</b> ← <i>this one</i>")
                    yr = (root.get("startDate") or {}).get("year") or ""
                    ep = root.get("episodes") or ""
                    sc = root.get("averageScore") or ""
                    meta_parts = []
                    if ep: meta_parts.append(f"{ep} eps")
                    if yr: meta_parts.append(str(yr))
                    if sc: meta_parts.append(f"⭐{sc}/100")
                    if meta_parts:
                        lines.append(f"    <i>{' · '.join(meta_parts)}</i>")
                else:
                    lines.append(f"  {num}. {node_line(node)}")
            lines.append("")

        # ── Other relations ───────────────────────────────────────────────
        rel_labels = {
            "SIDE_STORY":  "📎 Side Stories",
            "SPIN_OFF":    "🌀 Spin-offs",
            "ADAPTATION":  "📖 Adaptations",
            "ALTERNATIVE": "🔀 Alternatives",
            "PARENT":      "🗂 Parent Story",
            "SUMMARY":     "📝 Summaries",
            "CHARACTER":   "👤 Character Appearances",
        }
        for rel_type, label in rel_labels.items():
            nodes = other.get(rel_type)
            if not nodes:
                continue
            lines.append(f"{label}")
            for node in nodes:
                lines.append(f"  • {node_line(node)}")
            lines.append("")

        if not timeline and not other:
            lines.append("<i>No related works found.</i>")

        return "\n".join(lines)

    # ── /airing ───────────────────────────────────────────────────────────
    async def cmd_airing(self, _, msg: Message):
        """
        /airing <anime name>
        Search this week's schedule for the anime and show its episode
        countdown (Raw / Sub / Dub air times with time-remaining).

        Example:
            /airing One Piece
            /airing Jujutsu Kaisen
        """
        if not await self._auth(msg):
            return

        query_str = " ".join(msg.command[1:]).strip()
        if not query_str:
            await msg.reply(
                "⏰ <b>Usage:</b> <code>/airing &lt;anime name&gt;</code>\n\n"
                "Shows the episode countdown for that anime this week.\n\n"
                "<b>Examples:</b>\n"
                "• <code>/airing One Piece</code>\n"
                "• <code>/airing Jujutsu Kaisen</code>\n"
                "• <code>/airing Tensura</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        wait = await msg.reply(
            f"🔍 Searching schedule for <i>{query_str}</i>…",
            parse_mode=enums.ParseMode.HTML,
        )

        try:
            year, week   = current_week_year()
            flt          = await self.db.get_filter(msg.chat.id)
            entries      = await self.scraper.get_timetable(year, week, flt=flt)
            schedule     = ScheduleProcessor.process(entries, flt=flt)
            now          = datetime.now(TZ)

            # ── fuzzy search through all anime in this week's schedule ──
            def _norm(s: str) -> str:
                return re.sub(r"[^a-z0-9]", "", s.lower())

            q_norm = _norm(query_str)

            # Collect matching anime entries (title + romaji both checked)
            matches: list = []
            seen_slugs: set = set()
            for day_list in schedule.values():
                for anime in day_list:
                    slug = anime.get("route") or anime.get("slug") or ""
                    if slug in seen_slugs:
                        # Already matched this show from another day/version — merge
                        # (handled below via slug keying)
                        pass
                    title  = anime.get("title", "")
                    romaji = anime.get("romaji", title)
                    t_norm = _norm(title)
                    r_norm = _norm(romaji)
                    if q_norm in t_norm or q_norm in r_norm or t_norm in q_norm or r_norm in q_norm:
                        if slug not in seen_slugs:
                            seen_slugs.add(slug)
                            matches.append(anime)
                        else:
                            # Merge new version times into the existing match
                            for m in matches:
                                if (m.get("route") or m.get("slug") or "") == slug:
                                    for ver in ("raw", "sub", "dub"):
                                        if anime.get(f"{ver}_time") and not m.get(f"{ver}_time"):
                                            m[f"{ver}_time"] = anime[f"{ver}_time"]
                                            m[f"{ver}_ep"]   = anime.get(f"{ver}_ep")
                                    break

            try:
                await wait.delete()
            except Exception:
                pass

            if not matches:
                await msg.reply(
                    f"❌ <b>{query_str}</b> not found in this week's schedule.\n\n"
                    f"<i>Try a shorter name or check the spelling.</i>",
                    parse_mode=enums.ParseMode.HTML,
                )
                return

            ver_icons = {"raw": "🔴 Raw", "sub": "🔵 Sub", "dub": "🟢 Dub"}

            def _fmt_countdown(dt: datetime) -> str:
                delta   = dt - now
                seconds = int(delta.total_seconds())
                if seconds < 0:
                    ago = abs(seconds) // 60
                    return f"aired {ago}m ago" if ago < 60 else f"aired {ago // 60}h {ago % 60:02d}m ago"
                h, rem = divmod(seconds, 3600)
                m      = rem // 60
                if h >= 24:
                    d = h // 24
                    hr = h % 24
                    return f"in {d}d {hr}h {m:02d}m"
                return f"in {h}h {m:02d}m" if h > 0 else f"in {m}m"

            lines = [f"⏰ <b>Airing — Week {week}, {year}</b>\n"]

            for anime in matches:
                title      = anime.get("title", "?")
                total_eps  = anime.get("total_eps") or anime.get("episodes") or "?"
                has_any    = any(anime.get(f"{v}_time") for v in ("raw", "sub", "dub"))
                if not has_any:
                    continue

                lines.append(f"🎬 <b>{title}</b>")
                if total_eps and total_eps != "?":
                    lines.append(f"  📺 Total episodes: {total_eps}")

                for ver in ("raw", "sub", "dub"):
                    dt = anime.get(f"{ver}_time")
                    if dt is None:
                        continue
                    ep      = anime.get(f"{ver}_ep") or "?"
                    ep_str  = f" Ep {ep}" if ep != "?" else ""
                    day_str = dt.strftime("%A, %d %b")
                    t_str   = dt.strftime("%I:%M %p")
                    cd_str  = _fmt_countdown(dt)
                    label   = ver_icons[ver]
                    lines.append(
                        f"  {label}{ep_str}\n"
                        f"    📅 {day_str} at {t_str}\n"
                        f"    ⏳ {cd_str}"
                    )
                lines.append("")   # blank separator between anime

            if len(lines) <= 2:
                await msg.reply(
                    f"📭 No air times found for <b>{query_str}</b> this week.",
                    parse_mode=enums.ParseMode.HTML,
                )
                return

            text = "\n".join(lines).rstrip()

            # Split if too long
            if len(text) > 4000:
                chunks, cur = [], ""
                for line in text.split("\n"):
                    if len(cur) + len(line) + 1 > 3800 and cur:
                        chunks.append(cur)
                        cur = line + "\n"
                    else:
                        cur += line + "\n"
                if cur.strip():
                    chunks.append(cur)
                for chunk in chunks:
                    await msg.reply(chunk.strip(), parse_mode=enums.ParseMode.HTML)
            else:
                await msg.reply(text, parse_mode=enums.ParseMode.HTML)

        except Exception as exc:
            logger.error("cmd_airing error: %s", exc)
            try:
                await wait.delete()
            except Exception:
                pass
            await msg.reply("❌ Failed to fetch airing data.", parse_mode=enums.ParseMode.HTML)

    async def cmd_filter(self, _, msg: Message):
        if not await self._auth(msg):
            return
        flt = await self.db.get_filter(msg.chat.id)
        await msg.reply(
            self._filter_text(flt),
            reply_markup=self._kb_filter(flt, section="main"),
            parse_mode=enums.ParseMode.HTML,
        )

    # ── /season ───────────────────────────────────────────────────────────
    async def cmd_season(self, _, msg: Message):
        if not await self._auth(msg):
            return
        parts = msg.command[1:]
        if len(parts) < 2:
            await msg.reply(
                "Usage: /season &lt;year&gt; &lt;season&gt;\n"
                "Seasons: winter, spring, summer, fall\n"
                "Example: /season 2025 spring",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        try:
            year = int(parts[0])
        except ValueError:
            await msg.reply("❌ Invalid year.", parse_mode=enums.ParseMode.HTML)
            return
        season = parts[1].lower().strip()
        if season not in SeasonScraper.VALID_SEASONS:
            await msg.reply(
                f"❌ Invalid season. Choose from: {', '.join(SeasonScraper.VALID_SEASONS)}",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        wait = await msg.reply(f"⏳ Fetching {season.capitalize()} {year} anime…")
        titles = await self.season.get_season(year, season)
        try:
            await wait.delete()
        except Exception:
            pass
        if not titles:
            await msg.reply("❌ No data found for that season.", parse_mode=enums.ParseMode.HTML)
            return
        text, total_pages = SeasonScraper.paginate(titles, year, season, page=0)
        kb = self._kb_season(year, season, 0, total_pages)
        await msg.reply(text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    # ── Filter helpers ────────────────────────────────────────────────────
    @staticmethod
    def _filter_text(flt: "ChatFilter") -> str:
        air_on  = ", ".join(sorted(flt.air_types)).upper() or "None"
        mt_on   = ", ".join(MEDIA_TYPES.get(s, s) for s in sorted(flt.media_types)) or "All"
        st_on   = ", ".join(STREAMS.get(s, s) for s in sorted(flt.streams)) or "All"
        donghua = "Hidden" if flt.hide_donghua else "Shown"
        return (
            "🎛 <b>Schedule Filters</b>\n\n"
            f"📡 <b>Air Types:</b> {air_on}\n"
            f"📺 <b>Media Types:</b> {mt_on}\n"
            f"🎞 <b>Streams:</b> {st_on}\n"
            f"🐉 <b>Donghua:</b> {donghua}\n\n"
            "<i>Use the buttons below to adjust filters.</i>"
        )

    @staticmethod
    def _kb_filter(flt: "ChatFilter", section: str = "main") -> InlineKeyboardMarkup:
        def tick(on: bool) -> str:
            return "✅" if on else "⬜"

        if section == "airtype":
            rows = [
                [InlineKeyboardButton(f"{tick('raw' in flt.air_types)} Raw",  callback_data="flt:air:raw")],
                [InlineKeyboardButton(f"{tick('sub' in flt.air_types)} Sub",  callback_data="flt:air:sub")],
                [InlineKeyboardButton(f"{tick('dub' in flt.air_types)} Dub",  callback_data="flt:air:dub")],
                [InlineKeyboardButton("◀ Back", callback_data="flt:main")],
            ]
        elif section == "mediatype":
            rows = []
            for slug, label in MEDIA_TYPES.items():
                rows.append([InlineKeyboardButton(
                    f"{tick(slug in flt.media_types)} {label}",
                    callback_data=f"flt:mt:{slug}",
                )])
            rows.append([InlineKeyboardButton("🔄 Show All", callback_data="flt:mt:reset")])
            rows.append([InlineKeyboardButton("◀ Back", callback_data="flt:main")])
        elif section == "streams":
            rows = []
            for slug, label in STREAMS.items():
                rows.append([InlineKeyboardButton(
                    f"{tick(slug in flt.streams)} {label}",
                    callback_data=f"flt:st:{slug}",
                )])
            rows.append([InlineKeyboardButton("🔄 Show All", callback_data="flt:st:reset")])
            rows.append([InlineKeyboardButton("◀ Back", callback_data="flt:main")])
        elif section == "other":
            rows = [
                [InlineKeyboardButton(
                    f"{tick(flt.hide_donghua)} Hide Donghua",
                    callback_data="flt:other:donghua",
                )],
                [InlineKeyboardButton("◀ Back", callback_data="flt:main")],
            ]
        else:  # main
            rows = [
                [InlineKeyboardButton("📡 Air Types",   callback_data="flt:section:airtype")],
                [InlineKeyboardButton("📺 Media Types", callback_data="flt:section:mediatype")],
                [InlineKeyboardButton("🎞 Streams",     callback_data="flt:section:streams")],
                [InlineKeyboardButton("🐉 Other",       callback_data="flt:section:other")],
                [InlineKeyboardButton("🔄 Reset All",   callback_data="flt:reset")],
                [
                    InlineKeyboardButton("◀ Back", callback_data="menu:back"),
                    InlineKeyboardButton("✖ Close", callback_data="flt:close"),
                ],
            ]
        return InlineKeyboardMarkup(rows)

    @staticmethod
    def _kb_topic_filter(flt: "ChatFilter", section: str, cid: int, tid: int) -> InlineKeyboardMarkup:
        """
        Like _kb_filter but scoped to a specific topic.
        Uses ts:flt:* callback data with cid and tid embedded.
        """
        def tick(on: bool) -> str:
            return "✅" if on else "⬜"

        back_to_rem = f"ts:cfg:rem:{cid}:{tid}"  # back → rem settings

        if section == "airtype":
            rows = [
                [InlineKeyboardButton(f"{tick('raw' in flt.air_types)} Raw", callback_data=f"ts:flt:air:raw:{cid}:{tid}")],
                [InlineKeyboardButton(f"{tick('sub' in flt.air_types)} Sub", callback_data=f"ts:flt:air:sub:{cid}:{tid}")],
                [InlineKeyboardButton(f"{tick('dub' in flt.air_types)} Dub", callback_data=f"ts:flt:air:dub:{cid}:{tid}")],
                [InlineKeyboardButton("◀ Back", callback_data=f"ts:flt:main:{cid}:{tid}")],
            ]
        elif section == "mediatype":
            rows = []
            for slug, label in MEDIA_TYPES.items():
                rows.append([InlineKeyboardButton(
                    f"{tick(slug in flt.media_types)} {label}",
                    callback_data=f"ts:flt:mt:{slug}:{cid}:{tid}",
                )])
            rows.append([InlineKeyboardButton("🔄 Show All", callback_data=f"ts:flt:mt:reset:{cid}:{tid}")])
            rows.append([InlineKeyboardButton("◀ Back",      callback_data=f"ts:flt:main:{cid}:{tid}")])
        elif section == "streams":
            rows = []
            for slug, label in STREAMS.items():
                rows.append([InlineKeyboardButton(
                    f"{tick(slug in flt.streams)} {label}",
                    callback_data=f"ts:flt:st:{slug}:{cid}:{tid}",
                )])
            rows.append([InlineKeyboardButton("🔄 Show All", callback_data=f"ts:flt:st:reset:{cid}:{tid}")])
            rows.append([InlineKeyboardButton("◀ Back",      callback_data=f"ts:flt:main:{cid}:{tid}")])
        elif section == "other":
            rows = [
                [InlineKeyboardButton(
                    f"{tick(flt.hide_donghua)} Hide Donghua",
                    callback_data=f"ts:flt:other:donghua:{cid}:{tid}",
                )],
                [InlineKeyboardButton("◀ Back", callback_data=f"ts:flt:main:{cid}:{tid}")],
            ]
        else:  # main
            rows = [
                [InlineKeyboardButton("📡 Air Types",   callback_data=f"ts:flt:section:airtype:{cid}:{tid}")],
                [InlineKeyboardButton("📺 Media Types", callback_data=f"ts:flt:section:mediatype:{cid}:{tid}")],
                [InlineKeyboardButton("🎞 Streams",     callback_data=f"ts:flt:section:streams:{cid}:{tid}")],
                [InlineKeyboardButton("🐉 Other",       callback_data=f"ts:flt:section:other:{cid}:{tid}")],
                [InlineKeyboardButton("🔄 Reset All",   callback_data=f"ts:flt:reset:{cid}:{tid}")],
                [InlineKeyboardButton("◀ Back",         callback_data=back_to_rem)],
            ]
        return InlineKeyboardMarkup(rows)

    @staticmethod
    def _kb_season(year: int, season: str, page: int, total_pages: int) -> InlineKeyboardMarkup:
        rows: list = []
        nav: list = []
        if page > 0:
            nav.append(InlineKeyboardButton(
                "◀ Prev", callback_data=f"season:{year}:{season}:{page - 1}"
            ))
        if total_pages > 1:
            nav.append(InlineKeyboardButton(
                f"{page + 1}/{total_pages}", callback_data="noop"
            ))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(
                "Next ▶", callback_data=f"season:{year}:{season}:{page + 1}"
            ))
        if nav:
            rows.append(nav)
        return InlineKeyboardMarkup(rows) if rows else None

    # ── /naruto (Gemini AI chat) ─────────────────────────────────────────
    async def cmd_naruto(self, _, msg: Message):
        """
        /naruto <prompt>  — Chat with Gemini AI in Naruto persona.
        Also triggered by plain messages starting with 'naruto '.
        """
        if not await self._auth(msg):
            return
        await self.db.track_user(msg)

        # Extract prompt — works for both /naruto command and plain text trigger
        raw = msg.text or msg.caption or ""
        if msg.command:
            prompt = " ".join(msg.command[1:]).strip()
        else:
            # Plain text trigger: "naruto <prompt>"
            prompt = re.sub(r"(?i)^naruto\s+", "", raw).strip()

        if not prompt:
            await msg.reply(
                "🍥 <b>Naruto AI</b>\n\n"
                "Usage: <code>/naruto &lt;your question&gt;</code>\n"
                "Or just type: <code>naruto &lt;your question&gt;</code>\n\n"
                "<i>Tip: I remember our conversation context!</i>",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        thinking = await msg.reply("🍥 <i>Thinking, dattebayo…</i>", parse_mode=enums.ParseMode.HTML)
        uid    = msg.from_user.id if msg.from_user else msg.chat.id
        reply  = await self.gemini.chat(uid, prompt)
        try:
            await thinking.delete()
        except Exception:
            pass

        # Split if too long
        if len(reply) <= TG_MESSAGE_LIMIT:
            await msg.reply(reply, parse_mode=enums.ParseMode.HTML,
                            disable_web_page_preview=True)
        else:
            chunks, cur = [], ""
            for line in reply.split("\n"):
                if len(cur) + len(line) + 1 > 3800 and cur:
                    chunks.append(cur)
                    cur = line + "\n"
                else:
                    cur += line + "\n"
            if cur.strip():
                chunks.append(cur)
            for chunk in chunks:
                await msg.reply(chunk.strip(), parse_mode=enums.ParseMode.HTML,
                                disable_web_page_preview=True)

    async def cmd_naruto_clear(self, _, msg: Message):
        """/naruto_clear — clear your Gemini chat history."""
        if not await self._auth(msg):
            return
        uid = msg.from_user.id if msg.from_user else msg.chat.id
        await self.gemini.clear_history(uid)
        await msg.reply("🗑 Gemini chat history cleared! Fresh start, dattebayo! 🍥",
                        parse_mode=enums.ParseMode.HTML)

    # ── /mode_clear ──────────────────────────────────────────────────────────
    async def cmd_mode_clear(self, _, msg: Message):
        """
        /mode_clear — wipe all saved topics, modes, per-topic configs,
        day messages and the schedule filter for this chat.
        Resets everything to factory defaults.
        """
        if not await self._auth(msg):
            return
        if not await self._is_group_admin(msg) and not await self._is_bot_admin(msg):
            await msg.reply("⛔ Only group admins can run /mode_clear.")
            return
        await self.db.clear_all_modes_and_topics(msg.chat.id)
        await msg.reply(
            "🗑 <b>Mode cleared!</b>\n\n"
            "All topics, modes, per-topic configs, day messages and the schedule "
            "filter for this chat have been reset to factory defaults.",
            parse_mode=enums.ParseMode.HTML,
        )

    # ── /set_reminder ─────────────────────────────────────────────────────────
    async def cmd_set_reminder(self, _, msg: Message):
        """
        /set_reminder <day> <HH:MM> <message text>
        Example: /set_reminder Monday 10:00 Gym time!

        Stores a weekly reminder for the user (DM only).
        APScheduler sends the reminder at the exact time every week.
        """
        if msg.chat.type != ChatType.PRIVATE:
            await msg.reply(
                "ℹ️ Reminders only work in private (DM) chat with the bot.",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        args = msg.command[1:] if len(msg.command) > 1 else []
        if len(args) < 3:
            await msg.reply(
                "ℹ️ <b>Usage:</b> <code>/set_reminder &lt;day&gt; &lt;HH:MM&gt; &lt;message&gt;</code>\n\n"
                "<b>Example:</b> <code>/set_reminder Monday 10:00 Gym time!</code>\n\n"
                "Valid days: Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        day_arg  = args[0].strip().capitalize()
        time_arg = args[1].strip()
        text_arg = " ".join(args[2:]).strip()

        valid_days = [d.capitalize() for d in DAYS_OF_WEEK]
        if day_arg not in valid_days:
            await msg.reply(
                f"❌ Invalid day: <b>{day_arg}</b>\n"
                f"Valid days: {', '.join(valid_days)}",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        if not re.match(r"^\d{1,2}:\d{2}$", time_arg):
            await msg.reply(
                "❌ Invalid time format. Use <code>HH:MM</code> (e.g. <code>10:00</code>, <code>14:30</code>)",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        try:
            hour, minute = map(int, time_arg.split(":"))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError("out of range")
        except ValueError:
            await msg.reply(
                "❌ Invalid time value. Hours 0–23, minutes 0–59.",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        uid = msg.from_user.id if msg.from_user else msg.chat.id

        # Save to MongoDB
        await self.db.save_reminder(uid, day_arg, f"{hour:02d}:{minute:02d}", text_arg)

        # Register/replace weekly APScheduler job for this user+day
        day_index = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"].index(day_arg)
        day_short = ["mon","tue","wed","thu","fri","sat","sun"][day_index]
        job_id    = f"user_reminder:{uid}:{day_arg}"

        async def _fire_reminder(user_id=uid, reminder_text=text_arg):
            try:
                await self.app.send_message(
                    user_id,
                    f"⏰ <b>Reminder</b>\n\n{reminder_text}",
                    parse_mode=enums.ParseMode.HTML,
                )
            except Exception as exc:
                logger.warning("User reminder send failed uid=%s: %s", user_id, exc)

        self.sched.scheduler.add_job(
            _fire_reminder,
            "cron",
            day_of_week=day_short,
            hour=hour,
            minute=minute,
            timezone=TZ_NAME,
            id=job_id,
            replace_existing=True,
            misfire_grace_time=120,
        )
        logger.info("Reminder registered: uid=%s %s %02d:%02d '%s'", uid, day_arg, hour, minute, text_arg)

        await msg.reply(
            f"✅ <b>Reminder set!</b>\n\n"
            f"📅 Day: <b>{day_arg}</b>\n"
            f"🕐 Time: <b>{hour:02d}:{minute:02d}</b> ({TZ_NAME})\n"
            f"💬 Message: <i>{text_arg}</i>\n\n"
            f"I'll send this every <b>{day_arg}</b> at <b>{hour:02d}:{minute:02d}</b>.",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_list_reminders(self, _, msg: Message):
        """
        /list_reminders — show all your active weekly reminders.
        DM only.
        """
        if msg.chat.type != ChatType.PRIVATE:
            await msg.reply("ℹ️ Only works in private chat.", parse_mode=enums.ParseMode.HTML)
            return
        uid = msg.from_user.id if msg.from_user else msg.chat.id
        reminders = await self.db.get_user_reminders(uid)
        if not reminders:
            await msg.reply(
                "📭 No reminders set.\n\n"
                "Use <code>/set_reminder &lt;day&gt; &lt;HH:MM&gt; &lt;message&gt;</code> to add one.",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        lines = ["⏰ <b>Your Weekly Reminders</b>\n"]
        for r in reminders:
            lines.append(
                f"📅 <b>{r['day_of_week']}</b> at <b>{r['time']}</b>\n"
                f"   💬 {r['message']}"
            )
        await msg.reply("\n\n".join(lines), parse_mode=enums.ParseMode.HTML)

    async def cmd_del_reminder(self, _, msg: Message):
        """
        /del_reminder <day> — delete your reminder for a specific day.
        Example: /del_reminder Monday
        """
        if msg.chat.type != ChatType.PRIVATE:
            await msg.reply("ℹ️ Only works in private chat.", parse_mode=enums.ParseMode.HTML)
            return
        args = msg.command[1:] if len(msg.command) > 1 else []
        if not args:
            await msg.reply(
                "Usage: <code>/del_reminder &lt;day&gt;</code>\n"
                "Example: <code>/del_reminder Monday</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        day_arg = args[0].strip().capitalize()
        uid     = msg.from_user.id if msg.from_user else msg.chat.id
        await self.db.delete_reminder(uid, day_arg)
        job_id = f"user_reminder:{uid}:{day_arg}"
        try:
            if self.sched.scheduler.get_job(job_id):
                self.sched.scheduler.remove_job(job_id)
        except Exception:
            pass
        await msg.reply(
            f"🗑 Reminder for <b>{day_arg}</b> deleted.",
            parse_mode=enums.ParseMode.HTML,
        )

    async def _auto_naruto(self, _, msg: Message):
        """Auto-trigger: plain message starting with 'naruto ' (case-insensitive)."""
        if not await self._auth(msg):
            return
        raw = msg.text or msg.caption or ""
        if re.match(r"(?i)^naruto\s+", raw):
            await self.cmd_naruto(_, msg)


    # NOTE: AI auto-reply on bot-message replies is intentionally DISABLED.
    # AI works ONLY via explicit commands: /naruto, /naruto_clear, etc.
    async def _auto_reply_naruto(self, _, msg: Message):
        """DISABLED — AI auto-reply on bot-message replies has been removed."""
        return  # Intentionally does nothing

    # ── /save_banner ─────────────────────────────────────────────────────
    async def cmd_save_banner(self, _, msg: Message):
        """
        /save_banner [name]

        Step 1: Shows category buttons [Ongoing] [Completed] [Rename File].
        Step 2: User taps category → media saved to DB channel + MongoDB.

        Supports:
          ✅ Photos (JPG / PNG / WebP)
          ✅ Documents (thumbnails, banners, any file)

        MongoDB schema: {name, category, msg_id, message_id, file_type}
        """
        if not await self._auth(msg):
            return
        if not await self._is_group_admin(msg) and not await self._is_bot_admin(msg):
            await msg.reply("⛔ Only group admins can save banners.")
            return
        if not BANNER_DB_CHAT:
            await msg.reply(
                "⚠️ <b>Banner DB not configured.</b>\n"
                "Set <code>BANNER_DB_CHAT</code> env var to a channel ID where the bot is admin.",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        raw_name = " ".join(msg.command[1:]).strip()

        # Must reply to a media message
        replied = msg.reply_to_message
        if not replied:
            await msg.reply(
                "ℹ️ <b>How to use:</b>\n"
                "1. Send or forward a photo / document\n"
                "2. Reply to it with: <code>/save_banner [optional name]</code>\n\n"
                "<b>Example:</b> <code>/save_banner welcome banner</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        # Detect media — photo first, then document
        file_id   = None
        file_type = None
        if replied.photo:
            file_id   = replied.photo.file_id
            file_type = "photo"
        elif replied.document:
            file_id   = replied.document.file_id
            file_type = "document"
            # Auto-name from filename if user gave no name
            if not raw_name and replied.document.file_name:
                raw_name = replied.document.file_name.rsplit(".", 1)[0]

        if not file_id:
            await msg.reply(
                "❌ Please reply to a <b>photo or document</b>.\n"
                "Images and file attachments are both supported.",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        name = (raw_name or "banner").lower().strip()

        # Store pending state — the bnr_cat callback completes the save
        uid = msg.from_user.id if msg.from_user else msg.chat.id
        self.banner_db.set_pending(uid, {
            "name":      name,
            "file_id":   file_id,
            "file_type": file_type,
            "chat_id":   msg.chat.id,
        })

        await msg.reply(
            f"🖼 <b>Save Banner</b>\n\n"
            f"📛 Name: <b>{name}</b>\n"
            f"📄 Type: <code>{file_type}</code>\n\n"
            "Select a <b>category</b>:",
            reply_markup=self.banner_db.category_picker_kb("save"),
            parse_mode=enums.ParseMode.HTML,
        )

    # ── /banner ───────────────────────────────────────────────────────────
    async def cmd_banner(self, _, msg: Message):
        """
        /banner              → show category picker [Ongoing][Completed][Rename File]
        /banner ongoing      → directly list Ongoing banners
        /banner completed    → directly list Completed banners
        /banner rename       → directly list Rename File banners
        /banner <query>      → search all categories by name
        """
        if not await self._auth(msg):
            return
        if not BANNER_DB_CHAT:
            await msg.reply(
                "⚠️ Banner DB channel not configured (BANNER_DB_CHAT).",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        args = " ".join(msg.command[1:]).strip().lower()

        # Direct category shortcut
        if args in BannerDB.VALID_CATEGORIES:
            await self._send_banner_list(msg, category=args, query="", page=0)
            return

        if args:
            # Treat as a name search across all categories
            await self._send_banner_list(msg, category="", query=args, page=0)
            return

        # No args → show category picker
        await msg.reply(
            "🖼 <b>Banners</b>\n\nSelect a category to browse:",
            reply_markup=self.banner_db.category_picker_kb("browse"),
            parse_mode=enums.ParseMode.HTML,
        )

    async def _send_banner_list(self, target, category: str = "", query: str = "",
                                 page: int = 0):
        """
        Shared helper: fetch banners, send paginated list.
        target: any object with a .reply() method (Message or similar).
        """
        banners = await self.banner_db.search(query, category)
        if not banners:
            text = "❌ No banners found"
            if category:
                text += f" in <b>{category.capitalize()}</b>"
            if query:
                text += f" matching <b>{query}</b>"
            await target.reply(text + ".", parse_mode=enums.ParseMode.HTML)
            return

        page_banners, total_pages = self.banner_db.paginate(banners, page)
        kb  = self.banner_db.banner_keyboard(
            page_banners, page, total_pages, query, category
        )
        txt = self.banner_db.banner_list_text(
            page_banners, page, len(banners), query, total_pages, category
        )
        await target.reply(txt, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    # ── /banner_list ─────────────────────────────────────────────────────
    async def cmd_banner_list(self, _, msg: Message):
        """
        /banner_list — show ALL saved banners (all categories) as a paginated list.
        Shortcut: same as /banner with no args but skips the category picker.
        """
        if not await self._auth(msg):
            return
        if not BANNER_DB_CHAT:
            await msg.reply(
                "⚠️ Banner DB channel not configured (BANNER_DB_CHAT).",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        await self._send_banner_list(msg, category="", query="", page=0)

    # ── /search ───────────────────────────────────────────────────────────
    async def cmd_search(self, _, msg: Message):
        """
        /search <anime title> — pick an uploader source, then view torrent
        results formatted as a Telegraph page.

        Uploader sources:
          🔴 varyg1001  — AnimeTosho (VARYG 1080p)
          🟡 ToonsHub   — AnimeTosho ([ToonsHub] 1080p)
          🟢 SubsPlease — AnimeTosho (SubsPlease 1080p)
          🔍 All        — search all three sources combined
        """
        if not await self._auth(msg):
            return
        q = " ".join(msg.command[1:]).strip()
        if not q:
            await msg.reply(
                "ℹ️ <b>Usage:</b> <code>/search &lt;anime title&gt;</code>\n\n"
                "<b>Example:</b>\n"
                "<code>/search That Time I Got Reincarnated as a Slime</code>\n\n"
                "You'll then choose which uploader to search.",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        # Encode query into callback data (max 64 bytes total in callback_data)
        q_safe = q[:48]  # leave room for prefix
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔴 varyg1001",   callback_data=f"srch:varyg:{q_safe}"),
                InlineKeyboardButton("🟡 ToonsHub",    callback_data=f"srch:toons:{q_safe}"),
            ],
            [
                InlineKeyboardButton("🟢 SubsPlease",  callback_data=f"srch:subsp:{q_safe}"),
                InlineKeyboardButton("🔍 All Sources", callback_data=f"srch:all:{q_safe}"),
            ],
            [InlineKeyboardButton("✖ Cancel", callback_data="srch:cancel")],
        ])
        await msg.reply(
            f"🔍 <b>Torrent Search</b>\n\n"
            f"Query: <i>{q}</i>\n\n"
            f"Select uploader source:",
            reply_markup=kb,
            parse_mode=enums.ParseMode.HTML,
        )

    async def _do_torrent_search(self, query_cb, source_key: str, anime_query: str,
                                   page: int = 0):
        """
        Search AnimeTosho for anime_query filtered by source_key.
        Posts results to Telegraph (always) and sends the link.
        Deletes the source-picker message first.
        """
        RESULTS_PER_PAGE = 5   # 5 results/page with inline action buttons

        # ── Delete the uploader-picker message ────────────────────────────
        try:
            await query_cb.message.delete()
        except Exception:
            pass

        await query_cb.answer("🔍 Searching…")
        wait_msg = await query_cb.message.reply(
            "⏳ Fetching results, please wait…", parse_mode=enums.ParseMode.HTML
        )

        SOURCE_MAP = {
            "varyg": [{"q": f"VARYG 1080 {anime_query}",
                       "filter": lambda t: "VARYG" in t.upper() and "1080" in t,
                       "icon": "🔴", "label": "🔴 varyg1001"}],
            "toons": [{"q": f"[ToonsHub] 1080 {anime_query}",
                       "filter": lambda t: "[ToonsHub]" in t and "1080" in t,
                       "icon": "🟡", "label": "🟡 ToonsHub"}],
            "subsp": [{"q": f"SubsPlease {anime_query} 1080",
                       "filter": lambda t: "SubsPlease" in t and "1080" in t,
                       "icon": "🟢", "label": "🟢 SubsPlease"}],
            "all":   [
                {"q": f"VARYG 1080 {anime_query}",
                 "filter": lambda t: "VARYG" in t.upper() and "1080" in t,
                 "icon": "🔴", "label": "🔴 varyg1001"},
                {"q": f"[ToonsHub] 1080 {anime_query}",
                 "filter": lambda t: "[ToonsHub]" in t and "1080" in t,
                 "icon": "🟡", "label": "🟡 ToonsHub"},
                {"q": f"SubsPlease {anime_query} 1080",
                 "filter": lambda t: "SubsPlease" in t and "1080" in t,
                 "icon": "🟢", "label": "🟢 SubsPlease"},
            ],
        }

        all_results = []
        for src_cfg in SOURCE_MAP.get(source_key, SOURCE_MAP["all"]):
            try:
                params = {"t": "search", "q": src_cfg["q"], "limit": 50}
                async with httpx.AsyncClient(timeout=20, headers={"User-Agent": "Mozilla/5.0"}) as client:
                    r = await client.get("https://feed.animetosho.org/json", params=params)
                    if r.status_code != 200:
                        continue
                    data = r.json()
                    if not isinstance(data, list):
                        data = data.get("items", []) if isinstance(data, dict) else []
                    filt = src_cfg["filter"]
                    for item in data:
                        title = item.get("title", "")
                        if not title or not filt(title):
                            continue
                        nyaa_id     = item.get("nyaa_id") or item.get("id", "")
                        torrent_url = item.get("torrent_url") or (
                            f"https://nyaa.si/download/{nyaa_id}.torrent" if nyaa_id else ""
                        )
                        size_b = item.get("total_size") or item.get("size") or 0
                        try:
                            size = f"{int(size_b) / (1024**3):.2f} GiB" if size_b else "?"
                        except Exception:
                            size = "?"
                        all_results.append({
                            "title":       title,
                            "icon":        src_cfg["icon"],
                            "label":       src_cfg["label"],
                            "torrent_url": torrent_url,
                            "size":        size,
                        })
            except Exception as exc:
                logger.warning("Torrent search error '%s': %s", src_cfg["q"], exc)

        try:
            await wait_msg.delete()
        except Exception:
            pass

        if not all_results:
            label_map = {"varyg":"🔴 varyg1001","toons":"🟡 ToonsHub","subsp":"🟢 SubsPlease","all":"🔍 All"}
            await query_cb.message.reply(
                f"❌ No results found for <b>{anime_query}</b> via {label_map.get(source_key, source_key)}.",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        total_results = len(all_results)
        total_pages   = max(1, (total_results + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE)
        page          = max(0, min(page, total_pages - 1))
        page_results  = all_results[page * RESULTS_PER_PAGE : (page + 1) * RESULTS_PER_PAGE]

        # ── Always post full results to Telegraph ─────────────────────────
        tg_link = await self._post_to_telegraph(anime_query, all_results, source_key)

        label_map = {"varyg":"🔴 varyg1001","toons":"🟡 ToonsHub","subsp":"🟢 SubsPlease","all":"🔍 All Sources"}
        src_label = label_map.get(source_key, source_key)

        # ── Build per-result message lines ────────────────────────────────
        lines = [
            f"🎌 <b>Torrent Search Results</b>",
            f"Query: <i>{anime_query}</i>",
            f"Source: {src_label}",
            f"Found: <b>{total_results}</b> result(s)"
            + (f" · Page {page + 1}/{total_pages}" if total_pages > 1 else ""),
            "",
        ]

        # ── Per-result: title + size as text, action buttons as inline keyboard row
        # Format per result:
        #   🔴 MAO S01E03 1080p
        #   📦 0.29 GiB
        #   [📥 Download] [🔗 Graph] [👁 View]
        result_rows = []
        for i, res in enumerate(page_results, page * RESULTS_PER_PAGE + 1):
            lines.append(
                f"{res['icon']} <b>{res['title']}</b>\n"
                f"📦 {res['size']}"
            )
            btn_row = []
            if res.get("torrent_url"):
                btn_row.append(InlineKeyboardButton("📥 Download", url=res["torrent_url"]))
            # Graph = Telegraph full-results page for this search
            if tg_link:
                btn_row.append(InlineKeyboardButton("🔗 Graph", url=tg_link))
            # View = nyaa.si view page extracted from torrent URL
            nyaa_m = re.search(r"/download/(\d+)", res.get("torrent_url", ""))
            if nyaa_m:
                view_url = f"https://nyaa.si/view/{nyaa_m.group(1)}"
                btn_row.append(InlineKeyboardButton("👁 View", url=view_url))
            elif res.get("torrent_url"):
                # Fallback: link to animetosho or subsplease
                btn_row.append(InlineKeyboardButton("👁 View", url=res["torrent_url"]))
            if btn_row:
                result_rows.append(btn_row)

        # Remaining total indicator
        remaining = total_results - (page + 1) * RESULTS_PER_PAGE
        if remaining > 0:
            lines.append(f"\n<i>…and {remaining} more result(s)</i>")

        text = "\n".join(lines)

        # ── Navigation and action rows
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(
                "⬅️ Prev",
                callback_data=f"srch:{source_key}:{anime_query[:40]}:{page - 1}",
            ))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(
                "➡️ Next",
                callback_data=f"srch:{source_key}:{anime_query[:40]}:{page + 1}",
            ))

        # Build keyboard: per-result rows first, then nav, then telegraph, then close
        rows = result_rows[:]
        if nav_buttons:
            rows.append(nav_buttons)
        if tg_link:
            rows.append([InlineKeyboardButton("📄 Full Results on Telegraph", url=tg_link)])
        rows.append([InlineKeyboardButton("✖ Close", callback_data="srch:cancel")])
        kb = InlineKeyboardMarkup(rows)

        # Truncate text to Telegram message limit
        if len(text) > TG_MESSAGE_LIMIT:
            text = text[:TG_MESSAGE_LIMIT - 20] + "\n<i>[truncated]</i>"

        await query_cb.message.reply(
            text,
            reply_markup=kb,
            parse_mode=enums.ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def _post_to_telegraph(self, title: str, results: list, source_key: str) -> Optional[str]:
        """
        Post search results to Telegraph and return the page URL.
        Telegraph createPage requires content as a JSON-encoded array of Node objects.
        Returns None if the post fails — caller should show fallback text.
        """
        label_map = {
            "varyg": "varyg1001 (VARYG 1080p)",
            "toons": "ToonsHub ([ToonsHub] 1080p)",
            "subsp": "SubsPlease (1080p)",
            "all":   "All Sources",
        }
        src_label = label_map.get(source_key, source_key)

        # Build Telegraph Node array (required format for createPage API)
        nodes = [
            {"tag": "h3", "children": [f"Search: {title}"]},
            {"tag": "p",  "children": [f"Source: {src_label} | {len(results)} results"]},
            {"tag": "hr"},
        ]

        for i, r in enumerate(results, 1):
            entry_parts = [f"{i}. {r['icon']} {r['title']}  |  {r['size']}"]
            children = ["\n".join(entry_parts)]

            if r.get("torrent_url"):
                children.append("  ")
                children.append({
                    "tag": "a",
                    "attrs": {"href": r["torrent_url"]},
                    "children": ["⬇ Download"],
                })

            nodes.append({"tag": "p", "children": children})

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    "https://api.telegra.ph/createPage",
                    json={
                        "access_token": "b968da509bb76866c35425099bc0989a5ec3b32997d28ed028",
                        "title":        f"{title} — Torrent Results",
                        "author_name":  "Naruto Timekeeper",
                        "content":      json.dumps(nodes),
                    },
                )
                data = resp.json()
                if data.get("ok"):
                    url = data["result"]["url"]
                    logger.info("Telegraph page created: %s (%d results)", url, len(results))
                    return url
                logger.warning("Telegraph createPage failed: %s", data)
        except Exception as exc:
            logger.warning("Telegraph post failed: %s", exc)
        return None

    # ── Inline mode ───────────────────────────────────────────────────────
    async def handle_inline(self, _, query):
        """
        @botusername <query>

        Inline search across saved banners.
        - Empty query   → return up to 50 banners from all categories
        - Category name → return banners from that category (ongoing/completed/rename)
        - Other query   → full-text name search across all categories

        Photos are returned as InlineQueryResultPhoto.
        Non-image documents are returned as InlineQueryResultArticle.
        Uses get_messages() only — fully bot-token safe.
        """
        if not BANNER_DB_CHAT:
            await query.answer([], cache_time=5)
            return

        q = (query.query or "").strip().lower()

        # Category shortcut: @bot ongoing / @bot completed / @bot rename
        cat_match = q if q in BannerDB.VALID_CATEGORIES else ""
        search_q  = "" if cat_match else q

        banners = await self.banner_db.search(search_q, cat_match)
        results  = []

        for b in banners[:50]:   # Telegram hard limit: 50 inline results
            msg_id = b.get("msg_id") or b.get("message_id")
            name   = b.get("name", "banner")
            cat    = b.get("category", "")

            if not msg_id:
                continue
            try:
                tg_msg = await self.app.get_messages(BANNER_DB_CHAT, msg_id)
                if not tg_msg or getattr(tg_msg, "empty", False):
                    continue

                caption = f"🖼 <b>{name}</b>"
                if cat:
                    caption += f"\n📂 {cat.capitalize()}"

                if tg_msg.photo:
                    # ── Photo result ──────────────────────────────────────
                    results.append(
                        InlineQueryResultPhoto(
                            photo_url=tg_msg.photo.file_id,
                            thumb_url=tg_msg.photo.file_id,
                            title=name,
                            id=f"banner_{msg_id}",
                            caption=caption,
                            parse_mode=enums.ParseMode.HTML,
                        )
                    )
                elif tg_msg.document:
                    mime = (tg_msg.document.mime_type or "")
                    if mime.startswith("image/"):
                        # ── Image-typed document → still show as photo ────
                        results.append(
                            InlineQueryResultPhoto(
                                photo_url=tg_msg.document.file_id,
                                thumb_url=tg_msg.document.file_id,
                                title=name,
                                id=f"banner_{msg_id}",
                                caption=caption,
                                parse_mode=enums.ParseMode.HTML,
                            )
                        )
                    else:
                        # ── Non-image document → article with description ─
                        results.append(
                            InlineQueryResultArticle(
                                id=f"banner_{msg_id}",
                                title=name,
                                description=f"📁 [{cat.capitalize()}]" if cat else "📁 Document",
                                input_message_content=InputTextMessageContent(
                                    caption,
                                    parse_mode=enums.ParseMode.HTML,
                                ),
                            )
                        )
            except Exception as exc:
                logger.debug("handle_inline skip msg_id=%s: %s", msg_id, exc)

        await query.answer(results, cache_time=30)

    # ── /savemonday ───────────────────────────────────────────────────────
    async def cmd_savemonday(self, _, msg: Message):
        """
        /savemonday <text>  — Save a message for Monday.
        /savemonday         — Show the currently saved Monday message.
        """
        if not await self._auth(msg):
            return
        args = " ".join(msg.command[1:]).strip()
        if not args:
            saved = await self.db.get_monday(msg.chat.id)
            if not saved:
                await msg.reply(
                    "📭 No Monday message saved yet.\n"
                    "Usage: <code>/savemonday Your text here</code>",
                    parse_mode=enums.ParseMode.HTML,
                )
            else:
                await msg.reply(
                    f"📅 <b>Saved Monday Message:</b>\n\n{saved}",
                    parse_mode=enums.ParseMode.HTML,
                )
            return
        # Escape HTML to safely store and display user input
        safe_text = (
            args
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        await self.db.save_monday(msg.chat.id, safe_text)
        await msg.reply(
            "✅ <b>Monday message saved!</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    # ── AFK system ────────────────────────────────────────────────────────
    @staticmethod
    def _human_time_ago(dt: datetime) -> str:
        """Return a human-readable string like '2 hours ago' from a UTC datetime."""
        now = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        diff = int((now - dt).total_seconds())
        if diff < 60:
            s = diff
            return f"{s} second{'s' if s != 1 else ''} ago"
        elif diff < 3600:
            m = diff // 60
            return f"{m} minute{'s' if m != 1 else ''} ago"
        elif diff < 86400:
            h = diff // 3600
            return f"{h} hour{'s' if h != 1 else ''} ago"
        else:
            d = diff // 86400
            return f"{d} day{'s' if d != 1 else ''} ago"

    async def cmd_afk(self, _, msg: Message):
        """/afk <reason> — Mark yourself as AFK until you send a message."""
        uid = msg.from_user.id if msg.from_user else None
        if not uid:
            return
        reason = " ".join(msg.command[1:]).strip()
        if not reason:
            reason = "I am AFK"
        safe_reason = (
            reason
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        await self.db.set_afk(uid, safe_reason)
        await msg.reply(
            f"😴 <b>You are now AFK</b>\n<i>{safe_reason}</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    async def _afk_watcher(self, _, msg: Message):
        """
        Passive AFK watcher — runs on every text/caption message:
        1. If the sender is AFK, remove their AFK status silently.
        2. If the message replies to or @mentions an AFK user, notify the chat.
        """
        if not msg.from_user:
            return

        sender_id = msg.from_user.id

        # Skip messages that are /afk commands themselves
        if msg.text and msg.text.strip().startswith("/afk"):
            return

        # 1. Sender returning from AFK — remove silently, no reply
        afk_self = await self.db.get_afk(sender_id)
        if afk_self:
            await self.db.remove_afk(sender_id)
            return

        # 2. Check if this message targets an AFK user
        afk_targets: list = []

        # Reply-to check
        if msg.reply_to_message and msg.reply_to_message.from_user:
            target_id = msg.reply_to_message.from_user.id
            if target_id != sender_id:
                doc = await self.db.get_afk(target_id)
                if doc:
                    afk_targets.append((msg.reply_to_message.from_user, doc))

        # Entity @mention / text_mention check
        from pyrogram.enums import MessageEntityType as _MET
        for ent in (msg.entities or []):
            if ent.type == _MET.MENTION:
                username_raw = (msg.text or "")[ent.offset: ent.offset + ent.length].lstrip("@")
                try:
                    user = await self.app.get_users(username_raw)
                    if user and user.id != sender_id:
                        doc = await self.db.get_afk(user.id)
                        if doc and not any(u.id == user.id for u, _ in afk_targets):
                            afk_targets.append((user, doc))
                except Exception:
                    pass
            elif ent.type == _MET.TEXT_MENTION and ent.user:
                uid2 = ent.user.id
                if uid2 != sender_id:
                    doc = await self.db.get_afk(uid2)
                    if doc and not any(u.id == uid2 for u, _ in afk_targets):
                        afk_targets.append((ent.user, doc))

        for user, doc in afk_targets:
            name = user.first_name or "User"
            safe_name = (
                name
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            afk_msg  = doc.get("message", "AFK")
            since    = doc.get("since")
            time_str = self._human_time_ago(since) if since else "a while ago"
            try:
                await msg.reply(
                    f"😴 <b>{safe_name} is AFK</b>: {afk_msg}\n"
                    f"⏱ Last seen: <i>{time_str}</i>",
                    parse_mode=enums.ParseMode.HTML,
                )
            except Exception as exc:
                logger.debug("AFK reply failed: %s", exc)

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
        app.on_message(filters.command("assigned"))(self.cmd_assigned)
        app.on_message(filters.command("airing"))(self.cmd_airing)
        for _day in ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]:
            app.on_message(filters.command(f"set{_day}"))(self.cmd_set_day)
        app.on_message(filters.command("clearassigned"))(self.cmd_clearassigned)
        app.on_message(filters.command("season"))(self.cmd_season)
        app.on_message(filters.command("relations"))(self.cmd_relations)
        # ── NEW: Gemini AI ──────────────────────────────────────────────────
        app.on_message(filters.command("naruto"))(self.cmd_naruto)
        app.on_message(filters.command("naruto_clear"))(self.cmd_naruto_clear)
        # Auto-trigger: plain text "naruto <prompt>" (private chats and groups)
        app.on_message(
            filters.text & filters.regex(r"(?i)^naruto\s+") & ~filters.command([])
        )(self._auto_naruto)
        # ── NEW: Banner system ──────────────────────────────────────────────
        app.on_message(filters.command("save_banner"))(self.cmd_save_banner)
        app.on_message(filters.command("banner"))(self.cmd_banner)
        app.on_message(filters.command("banner_list"))(self.cmd_banner_list)
        # ── NEW: Torrent search ─────────────────────────────────────────────
        app.on_message(filters.command("search"))(self.cmd_search)
        # NOTE: AI auto-reply on bot-message-reply is DISABLED.
        # Gemini only runs via explicit commands (/naruto etc.)
        # ── New DM / utility commands ──────────────────────────────────────
        app.on_message(filters.command("mode_clear"))(self.cmd_mode_clear)
        app.on_message(filters.command("set_reminder"))(self.cmd_set_reminder)
        app.on_message(filters.command("list_reminders"))(self.cmd_list_reminders)
        app.on_message(filters.command("del_reminder"))(self.cmd_del_reminder)
        # ── SaveMonday ──────────────────────────────────────────────────────
        app.on_message(filters.command("savemonday"))(self.cmd_savemonday)
        # ── AFK system ──────────────────────────────────────────────────────
        app.on_message(filters.command("afk"))(self.cmd_afk)
        # AFK watcher: runs on all text/caption messages (lower priority than commands)
        app.on_message(
            (filters.text | filters.caption) & ~filters.command([]),
            group=1,
        )(self._afk_watcher)
        # ── Bot admin commands ──────────────────────────────────────────────
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
        # ── NEW: Inline mode ────────────────────────────────────────────────
        app.on_inline_query()(self.handle_inline)

    # ── Lifecycle ─────────────────────────────────────────────────────────
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

        # Reload user reminders from MongoDB (survive restarts)
        await self._load_user_reminders()

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
        await self.title_resolver.close()
        await self.gemini.close()        # NEW
        await self.app.stop()
        if self._health:
            await self._health.cleanup()
        self.db.close()
        logger.info("Shutdown complete")

    async def _load_user_reminders(self):
        """
        On startup: reload all active user reminders from MongoDB into APScheduler.
        This ensures weekly reminder jobs survive bot restarts.
        Called once from run() after sched.start() and build_all_jobs().
        """
        try:
            reminders = await self.db.get_all_active_reminders()
            loaded = 0
            day_short_map = {
                "Monday": "mon", "Tuesday": "tue", "Wednesday": "wed",
                "Thursday": "thu", "Friday": "fri", "Saturday": "sat", "Sunday": "sun",
            }
            for r in reminders:
                uid      = r.get("user_id")
                day      = r.get("day_of_week", "")
                time_str = r.get("time", "")
                text     = r.get("message", "")
                if not (uid and day and time_str):
                    continue
                try:
                    hour, minute = map(int, time_str.split(":"))
                except ValueError:
                    continue
                day_short = day_short_map.get(day)
                if not day_short:
                    continue
                job_id = f"user_reminder:{uid}:{day}"

                async def _fire(user_id=uid, reminder_text=text):
                    try:
                        await self.app.send_message(
                            user_id,
                            f"⏰ <b>Reminder</b>\n\n{reminder_text}",
                            parse_mode=enums.ParseMode.HTML,
                        )
                    except Exception as exc:
                        logger.warning("User reminder fire failed uid=%s: %s", user_id, exc)

                self.sched.scheduler.add_job(
                    _fire,
                    "cron",
                    day_of_week=day_short,
                    hour=hour,
                    minute=minute,
                    timezone=TZ_NAME,
                    id=job_id,
                    replace_existing=True,
                    misfire_grace_time=120,
                )
                loaded += 1
            logger.info("_load_user_reminders: loaded %d reminder job(s) from DB", loaded)
        except Exception as exc:
            logger.error("_load_user_reminders failed: %s", exc)


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
