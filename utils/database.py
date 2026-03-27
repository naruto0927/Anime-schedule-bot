"""
utils/database.py — Motor (async MongoDB) data-access layer.

Collections
-----------
chats           subscription, filter, topic mode & per-topic config
jobs            scheduled reminder job records
cache           scrape / AniList response cache (TTL-based)
stats           global counters
auth            group authorisation records
admins          dynamic bot-admin list
nyaa_watermark  per-source pubDate watermark for nyaa dedup
"""

from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from utils.filters import ChatFilter

class Database:
    def __init__(self, uri: str):
        self.client = AsyncIOMotorClient(uri)
        self.db     = self.client["animebot"]
        self.chats  = self.db["chats"]
        self.jobs   = self.db["jobs"]
        self.cache  = self.db["cache"]
        self.stats  = self.db["stats"]
        self.auth          = self.db["auth"]

    async def init_indexes(self):
        await self.chats.create_index("chat_id", unique=True)
        await self.jobs.create_index(
            [("anime_slug", 1), ("episode", 1), ("version", 1)], unique=True
        )
        await self.cache.create_index("key", unique=True)
        await self.auth.create_index("chat_id", unique=True)
        await self.db["admins"].create_index("user_id", unique=True)
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
            for tid, mode in (doc.get("topics") or {}).items():
                if mode == "nyaa":
                    result.append((doc["chat_id"], int(tid)))
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

        # Group 1: chats that have a 'rem' topic assigned (subscribed or not)
        async for doc in self.chats.find({"topics": {"$exists": True}}):
            topics = doc.get("topics") or {}
            for tid, mode in topics.items():
                if mode == "rem":
                    cid = doc["chat_id"]
                    result.append((cid, int(tid), ChatFilter.from_doc(doc)))
                    seen.add(cid)
                    break

        # Group 2: subscribed chats with no topic binding (plain reminders)
        async for doc in self.chats.find({"subscribed": True}):
            cid = doc["chat_id"]
            if cid in seen:
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
