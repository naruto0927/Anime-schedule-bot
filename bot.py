"""
Anime Schedule Telegram Bot
Pyrogram + Motor + httpx + APScheduler
"""

import asyncio
import logging
import os
import signal
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters, idle, enums
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("AnimeBot")

# ──────────────────────────────────────────────────────────────────────────────
# Environment
# ──────────────────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID    = int(os.environ["API_ID"])
API_HASH  = os.environ["API_HASH"]
MONGO_URI = os.environ["MONGO_URI"]
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
TZ_NAME   = os.environ.get("TIMEZONE", "Asia/Kolkata")
TZ        = ZoneInfo(TZ_NAME)
AS_TOKEN  = os.environ.get("ANIMESCHEDULE_TOKEN", "")
PORT      = int(os.environ.get("PORT", "8000"))

ANIMESCHEDULE_API = "https://animeschedule.net/api/v3"
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
UTC = timezone.utc


# ──────────────────────────────────────────────────────────────────────────────
# Health-check server (required by Koyeb)
# ──────────────────────────────────────────────────────────────────────────────
async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", lambda _: web.Response(text="OK"))
    app.router.add_get("/health", lambda _: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health-check server on port %s", PORT)
    return runner


# ──────────────────────────────────────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────────────────────────────────────
class Database:
    def __init__(self, uri: str):
        self.client = AsyncIOMotorClient(uri)
        self.db     = self.client["animebot"]
        self.chats  = self.db["chats"]
        self.modes  = self.db["modes"]
        self.jobs   = self.db["jobs"]
        self.cache  = self.db["cache"]
        self.stats  = self.db["stats"]

    async def init_indexes(self):
        await self.chats.create_index("chat_id", unique=True)
        await self.modes.create_index("chat_id", unique=True)
        await self.jobs.create_index(
            [("anime_slug", 1), ("episode", 1), ("version", 1)], unique=True
        )
        await self.cache.create_index("key", unique=True)
        logger.info("DB indexes ensured")

    # ── Subscriptions ──────────────────────────────────────────────────────
    async def subscribe(self, chat_id: int):
        await self.chats.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, "subscribed": True}},
            upsert=True,
        )

    async def unsubscribe(self, chat_id: int):
        await self.chats.update_one(
            {"chat_id": chat_id}, {"$set": {"subscribed": False}}
        )

    async def is_subscribed(self, chat_id: int) -> bool:
        doc = await self.chats.find_one({"chat_id": chat_id})
        return bool(doc and doc.get("subscribed"))

    async def all_subscribed_chats(self) -> list:
        return [d["chat_id"] async for d in self.chats.find({"subscribed": True})]

    # ── Modes ──────────────────────────────────────────────────────────────
    async def set_mode(self, chat_id: int, mode: str):
        await self.modes.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, "mode": mode}},
            upsert=True,
        )

    async def get_mode(self, chat_id: int) -> str:
        doc = await self.modes.find_one({"chat_id": chat_id})
        return doc["mode"] if doc else "all"

    # ── Jobs (bulk) ────────────────────────────────────────────────────────
    async def bulk_save_jobs(self, job_list: list[dict]):
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

    # ── Cache ──────────────────────────────────────────────────────────────
    async def set_cache(self, key: str, value, ttl_seconds: int = 21600):
        expires_at = datetime.utcnow() + timedelta(seconds=ttl_seconds)
        await self.cache.update_one(
            {"key": key},
            {"$set": {"key": key, "value": value, "expires_at": expires_at}},
            upsert=True,
        )

    async def get_cache(self, key: str):
        doc = await self.cache.find_one({"key": key})
        if not doc:
            return None
        if doc["expires_at"] < datetime.utcnow():
            await self.cache.delete_one({"key": key})
            return None
        return doc["value"]

    # ── Stats ──────────────────────────────────────────────────────────────
    async def inc_stat(self, field: str, amount: int = 1):
        await self.stats.update_one(
            {"_id": "global"}, {"$inc": {field: amount}}, upsert=True
        )

    async def get_stats(self) -> dict:
        doc = await self.stats.find_one({"_id": "global"}) or {}
        doc.pop("_id", None)
        return doc

    async def count_documents(self, collection_name: str) -> int:
        return await self.db[collection_name].count_documents({})

    def close(self):
        self.client.close()


# ──────────────────────────────────────────────────────────────────────────────
# AnimeSchedule API
# ──────────────────────────────────────────────────────────────────────────────
class AnimeScheduleAPI:
    BASE = ANIMESCHEDULE_API

    def __init__(self, db: Database):
        self.db = db
        self._client: Optional[httpx.AsyncClient] = None

    def _make_client(self) -> httpx.AsyncClient:
        headers = {"Accept": "application/json"}
        if AS_TOKEN:
            headers["Authorization"] = f"Bearer {AS_TOKEN}"
        return httpx.AsyncClient(timeout=30, headers=headers)

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = self._make_client()
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _get(self, path: str, params: dict = None):
        url = f"{self.BASE}{path}"
        try:
            r = await self.client.get(url, params=params or {})
            r.raise_for_status()
            await self.db.inc_stat("api_calls")
            return r.json()
        except httpx.HTTPStatusError as exc:
            logger.error("API HTTP error [%s] %s", exc.response.status_code, url)
            return None
        except Exception as exc:
            logger.error("API error %s: %s", url, exc)
            return None

    async def get_timetable(self, year: int, week: int) -> list:
        cache_key = f"timetable:{year}:{week}"
        cached = await self.db.get_cache(cache_key)
        if cached is not None:
            await self.db.inc_stat("cache_hits")
            return cached
        data = await self._get("/timetables", {"year": year, "week": week})
        if data is None:
            return []
        entries = data if isinstance(data, list) else data.get("entries", [])
        await self.db.set_cache(cache_key, entries, ttl_seconds=21600)
        return entries

    async def search_anime(self, query: str) -> list:
        data = await self._get("/anime", {"title": query})
        if data is None:
            return []
        return data if isinstance(data, list) else data.get("anime", [])


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(TZ)
    except Exception:
        return None


def fmt12(dt: Optional[datetime]) -> str:
    return dt.strftime("%I:%M %p") if dt else ""


def current_week_year():
    iso = datetime.now(TZ).isocalendar()
    return iso.year, iso.week


# ──────────────────────────────────────────────────────────────────────────────
# Schedule Processor
# ──────────────────────────────────────────────────────────────────────────────
class ScheduleProcessor:

    @staticmethod
    def process(entries: list) -> dict:
        day_map = {d: [] for d in DAYS_OF_WEEK}
        seen: set = set()

        for e in entries:
            slug  = e.get("route") or e.get("slug") or e.get("title", "")
            title = e.get("title") or slug
            if not slug:
                continue

            mt = (e.get("type") or "").lower()
            if "donghua" in mt or e.get("isDonghua"):
                continue

            raw = parse_dt(e.get("episodeDate") or e.get("airingAt") or "")
            sub = parse_dt(e.get("subDate") or e.get("subAiringAt") or "")
            dub = parse_dt(e.get("dubDate") or e.get("dubAiringAt") or "")

            primary = raw or sub or dub
            if not primary:
                continue

            ep = e.get("episodeNumber") or e.get("episode") or 0
            key = f"{slug}:{ep}"
            if key in seen:
                continue
            seen.add(key)

            day = primary.strftime("%A")
            if day not in day_map:
                continue

            day_map[day].append({
                "slug":       slug,
                "title":      title,
                "date":       primary.strftime("%Y-%m-%d"),
                "raw_time":   raw,
                "sub_time":   sub,
                "dub_time":   dub,
                "episode":    ep,
                "total_eps":  e.get("episodes") or e.get("totalEpisodes") or "?",
                "thumbnail":  e.get("imageVersionRoute") or e.get("thumbnail") or "",
                "popularity": e.get("likes") or e.get("popularity") or 0,
            })

        for d in day_map:
            day_map[d].sort(key=lambda x: x["popularity"], reverse=True)

        return day_map

    @staticmethod
    def anime_block(a: dict, mode: str = "all") -> str:
        has_raw = a["raw_time"] is not None
        has_sub = a["sub_time"] is not None
        has_dub = a["dub_time"] is not None

        # Skip if mode-required version missing
        if mode == "raw" and not has_raw:
            return ""
        if mode == "sub" and not has_sub:
            return ""
        if mode == "dub" and not has_dub:
            return ""

        # Display rules
        show_raw = has_raw
        show_sub = has_sub
        show_dub = has_dub
        if not has_dub:
            show_dub = False
        if not has_sub:
            show_sub = False
        if not has_raw:
            show_raw = False

        if mode == "raw":
            show_sub = show_dub = False
        elif mode == "sub":
            show_raw = show_dub = False
        elif mode == "dub":
            show_raw = show_sub = False

        lines = [
            f"🎬 <b>{a['title']}</b>",
            f"📅 Date: {a['date']}",
            f"📺 Episodes: {a['episode']}/{a['total_eps']}",
        ]
        if show_raw:
            lines.append(f"  🔴 Raw: {fmt12(a['raw_time'])}")
        if show_sub:
            lines.append(f"  🔵 Sub: {fmt12(a['sub_time'])}")
        if show_dub:
            lines.append(f"  🟢 Dub: {fmt12(a['dub_time'])}")

        return "\n".join(lines)

    @staticmethod
    def day_text(day: str, anime_list: list, mode: str = "all") -> str:
        if not anime_list:
            return f"<b>📅 {day}</b>\n\nNo anime scheduled."
        blocks = [b for b in (ScheduleProcessor.anime_block(a, mode) for a in anime_list) if b]
        if not blocks:
            return f"<b>📅 {day}</b>\n\nNo anime for your current mode."
        return f"<b>📅 {day}</b> — {len(blocks)} anime\n\n" + "\n\n".join(blocks)


# ──────────────────────────────────────────────────────────────────────────────
# Reminder Scheduler
# ──────────────────────────────────────────────────────────────────────────────
class ReminderScheduler:
    def __init__(self, db: Database, api: AnimeScheduleAPI):
        self.db        = db
        self.api       = api
        self.app_ref   = None          # set after Client is ready
        self.scheduler = AsyncIOScheduler(timezone=TZ_NAME)

    def start(self):
        self.scheduler.start()
        logger.info("APScheduler started")

    def shutdown(self):
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    async def build_all_jobs(self):
        """Fetch 2 weeks of data, build all reminder jobs in bulk."""
        await self.db.clear_jobs()
        self.scheduler.remove_all_jobs()

        year, week = current_week_year()
        all_jobs: list[dict] = []

        for w in [week, week + 1]:
            entries   = await self.api.get_timetable(year, w)
            processed = ScheduleProcessor.process(entries)
            for day_list in processed.values():
                for anime in day_list:
                    jobs = self._make_jobs(anime)
                    all_jobs.extend(jobs)

        # Bulk write to DB
        await self.db.bulk_save_jobs(all_jobs)

        # Schedule APScheduler jobs
        now = datetime.now(TZ)
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

        # Re-add periodic refresh (removed by remove_all_jobs)
        self.scheduler.add_job(
            self.build_all_jobs,
            "interval",
            hours=6,
            id="refresh_schedule",
            replace_existing=True,
        )
        logger.info("Rebuilt %d future reminder jobs", added)

    def _make_jobs(self, anime: dict) -> list[dict]:
        jobs = []
        for version in ("raw", "sub", "dub"):
            dt = anime[f"{version}_time"]
            if dt is None:
                continue
            jobs.append({
                "anime_slug":  anime["slug"],
                "anime_title": anime["title"],
                "episode":     anime["episode"],
                "total_eps":   anime["total_eps"],
                "version":     version,
                "air_time":    dt.isoformat(),
            })
        return jobs

    async def _send_reminder(self, job: dict):
        if self.app_ref is None:
            return
        chats    = await self.db.all_subscribed_chats()
        title    = job["anime_title"]
        version  = job["version"].capitalize()
        ep       = job["episode"]
        total    = job["total_eps"]
        air_dt   = datetime.fromisoformat(job["air_time"]).astimezone(TZ)
        time_str = fmt12(air_dt)

        text = (
            f"🔔 <b>Episode Released!</b>\n\n"
            f"🎬 <b>{title}</b>\n"
            f"📌 Type: {version}\n"
            f"📺 Episode: {ep}/{total}\n"
            f"🕐 Time: {time_str}"
        )

        for chat_id in chats:
            mode = await self.db.get_mode(chat_id)
            if mode != "all" and mode != job["version"]:
                continue
            try:
                await self.app_ref.send_message(chat_id, text, parse_mode=enums.ParseMode.HTML)
            except Exception as exc:
                logger.warning("Reminder to %s failed: %s", chat_id, exc)


# ──────────────────────────────────────────────────────────────────────────────
# Bot
# ──────────────────────────────────────────────────────────────────────────────
class AnimeBot:
    def __init__(self):
        self.db    = Database(MONGO_URI)
        self.api   = AnimeScheduleAPI(self.db)
        self.sched = ReminderScheduler(self.db, self.api)
        self.app: Optional[Client] = None
        self._health_runner = None

    # ── Keyboards ─────────────────────────────────────────────────────────
    @staticmethod
    def _kb_main() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 View Schedule",      callback_data="menu:schedule")],
            [InlineKeyboardButton("🔔 Reminder Settings", callback_data="menu:reminders")],
            [InlineKeyboardButton("🎛 Mode Settings",      callback_data="menu:mode")],
            [InlineKeyboardButton("📊 Weekly View",        callback_data="menu:weekly")],
            [InlineKeyboardButton("❌ Close",              callback_data="menu:close")],
        ])

    @staticmethod
    def _kb_days() -> InlineKeyboardMarkup:
        rows = []
        for i in range(0, len(DAYS_OF_WEEK), 2):
            rows.append([
                InlineKeyboardButton(d, callback_data=f"day:{d}")
                for d in DAYS_OF_WEEK[i:i+2]
            ])
        rows.append([InlineKeyboardButton("◀ Back", callback_data="menu:back")])
        return InlineKeyboardMarkup(rows)

    @staticmethod
    def _kb_reminders() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Subscribe",           callback_data="rem:subscribe")],
            [InlineKeyboardButton("❌ Unsubscribe",         callback_data="rem:unsubscribe")],
            [InlineKeyboardButton("📋 Subscription Status", callback_data="rem:status")],
            [InlineKeyboardButton("◀ Back",                callback_data="menu:back")],
        ])

    @staticmethod
    def _kb_mode() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🔴 Raw Only",     callback_data="mode:raw")],
            [InlineKeyboardButton("🔵 Sub Only",     callback_data="mode:sub")],
            [InlineKeyboardButton("🟢 Dub Only",     callback_data="mode:dub")],
            [InlineKeyboardButton("🌟 All Versions", callback_data="mode:all")],
            [InlineKeyboardButton("◀ Back",          callback_data="menu:back")],
        ])

    # ── Schedule helper ───────────────────────────────────────────────────
    async def _schedule(self) -> dict:
        y, w = current_week_year()
        return ScheduleProcessor.process(await self.api.get_timetable(y, w))

    # ── Command handlers ──────────────────────────────────────────────────
    async def cmd_start(self, _, msg: Message):
        await msg.reply(
            "👋 <b>Welcome to Anime Schedule Bot!</b>\n\n"
            "Use /settings to open the panel or /help for info.",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_help(self, _, msg: Message):
        await msg.reply(
            "<b>📖 Commands</b>\n\n"
            "/settings — Open settings panel\n"
            "/anime &lt;name&gt; — Search anime\n"
            "/help — This message\n\n"
            "<b>Modes:</b>\n"
            "🔴 Raw  🔵 Sub  🟢 Dub  🌟 All\n\n"
            f"⏰ Times in <b>{TZ_NAME}</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_settings(self, _, msg: Message):
        await msg.reply(
            "⚙️ <b>Settings Panel</b>",
            reply_markup=self._kb_main(),
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_anime(self, _, msg: Message):
        query = " ".join(msg.command[1:]).strip()
        if not query:
            await msg.reply("Usage: /anime &lt;name&gt;", parse_mode=enums.ParseMode.HTML)
            return

        await msg.reply(f"🔍 Searching <i>{query}</i>…", parse_mode=enums.ParseMode.HTML)
        results = await self.api.search_anime(query)
        if not results:
            await msg.reply("❌ No results found.")
            return

        a         = results[0]
        title     = a.get("title") or "Unknown"
        synopsis  = a.get("synopsis") or a.get("description") or "No synopsis."
        episodes  = a.get("episodes") or "?"
        status    = a.get("status") or a.get("airingStatus") or "Unknown"
        thumbnail = a.get("imageVersionRoute") or a.get("thumbnail") or None

        if len(synopsis) > 800:
            synopsis = synopsis[:797] + "…"

        caption = (
            f"<b>{title}</b>\n\n"
            f"📺 Episodes: {episodes}\n"
            f"📡 Status: {status}\n\n"
            f"{synopsis}"
        )
        if thumbnail:
            try:
                await msg.reply_photo(photo=thumbnail, caption=caption, parse_mode=enums.ParseMode.HTML)
                return
            except Exception:
                pass
        await msg.reply(caption, parse_mode=enums.ParseMode.HTML)

    # ── Admin ─────────────────────────────────────────────────────────────
    async def _is_admin(self, msg: Message) -> bool:
        if msg.from_user and msg.from_user.id in ADMIN_IDS:
            return True
        await msg.reply("⛔ Admin only.")
        return False

    async def cmd_reload(self, _, msg: Message):
        if not await self._is_admin(msg):
            return
        await msg.reply("🔄 Reloading…")
        await self.db.cache.drop()
        await self.sched.build_all_jobs()
        await msg.reply("✅ Done.")

    async def cmd_stats(self, _, msg: Message):
        if not await self._is_admin(msg):
            return
        s  = await self.db.get_stats()
        nc = await self.db.count_documents("chats")
        nj = await self.db.count_documents("jobs")
        nch = await self.db.count_documents("cache")
        aj = len(self.sched.scheduler.get_jobs())
        await msg.reply(
            "<b>📊 Stats</b>\n\n"
            f"API Calls:      {s.get('api_calls', 0)}\n"
            f"Cache Hits:     {s.get('cache_hits', 0)}\n"
            f"Scheduler Jobs: {aj}\n"
            f"Chats (DB):     {nc}\n"
            f"Job Docs (DB):  {nj}\n"
            f"Cache Docs:     {nch}",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_setsort(self, _, msg: Message):
        if not await self._is_admin(msg):
            return
        await msg.reply("ℹ️ Sort is fixed to popularity currently.")

    async def cmd_broadcast(self, _, msg: Message):
        if not await self._is_admin(msg):
            return
        text = " ".join(msg.command[1:]).strip()
        if not text:
            await msg.reply("Usage: /broadcast &lt;message&gt;", parse_mode=enums.ParseMode.HTML)
            return
        chats = await self.db.all_subscribed_chats()
        sent = 0
        for cid in chats:
            try:
                await self.app.send_message(cid, text)
                sent += 1
            except Exception:
                pass
        await msg.reply(f"📢 Sent to {sent}/{len(chats)} chats.")

    # ── Callback handler ──────────────────────────────────────────────────
    async def handle_callback(self, _, query: CallbackQuery):
        data    = query.data
        chat_id = query.message.chat.id

        if data == "menu:back":
            await query.edit_message_text(
                "⚙️ <b>Settings Panel</b>",
                reply_markup=self._kb_main(),
                parse_mode=enums.ParseMode.HTML,
            )
        elif data == "menu:close":
            await query.message.delete()
        elif data == "menu:schedule":
            await query.edit_message_text(
                "📅 <b>Select a day</b>",
                reply_markup=self._kb_days(),
                parse_mode=enums.ParseMode.HTML,
            )
        elif data == "menu:reminders":
            await query.edit_message_text(
                "🔔 <b>Reminder Settings</b>",
                reply_markup=self._kb_reminders(),
                parse_mode=enums.ParseMode.HTML,
            )
        elif data == "menu:mode":
            mode = await self.db.get_mode(chat_id)
            await query.edit_message_text(
                f"🎛 <b>Mode Settings</b>\nCurrent: <b>{mode.capitalize()}</b>",
                reply_markup=self._kb_mode(),
                parse_mode=enums.ParseMode.HTML,
            )
        elif data == "menu:weekly":
            await query.answer("Loading…")
            schedule = await self._schedule()
            lines = ["<b>📊 Weekly Overview</b>\n"]
            for d in DAYS_OF_WEEK:
                lines.append(f"• <b>{d}</b>: {len(schedule.get(d, []))} anime")
            await query.edit_message_text(
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀ Back", callback_data="menu:back")]]
                ),
                parse_mode=enums.ParseMode.HTML,
            )
        elif data.startswith("day:"):
            day = data.split(":", 1)[1]
            await query.answer(f"Loading {day}…")
            schedule = await self._schedule()
            mode     = await self.db.get_mode(chat_id)
            text     = ScheduleProcessor.day_text(day, schedule.get(day, []), mode)
            if len(text) > 4000:
                text = text[:3997] + "…"
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀ Back", callback_data="menu:schedule")]]
                ),
                parse_mode=enums.ParseMode.HTML,
            )
        elif data == "rem:subscribe":
            await self.db.subscribe(chat_id)
            await query.answer("✅ Subscribed!", show_alert=True)
        elif data == "rem:unsubscribe":
            await self.db.unsubscribe(chat_id)
            await query.answer("❌ Unsubscribed.", show_alert=True)
        elif data == "rem:status":
            sub = await self.db.is_subscribed(chat_id)
            await query.answer(
                "✅ Subscribed" if sub else "❌ Not subscribed", show_alert=True
            )
        elif data.startswith("mode:"):
            mode = data.split(":", 1)[1]
            labels = {"raw": "🔴 Raw Only", "sub": "🔵 Sub Only",
                      "dub": "🟢 Dub Only", "all": "🌟 All Versions"}
            await self.db.set_mode(chat_id, mode)
            await query.answer(f"Mode → {labels.get(mode, mode)}", show_alert=True)
            await query.edit_message_text(
                f"🎛 <b>Mode Settings</b>\nCurrent: <b>{mode.capitalize()}</b>",
                reply_markup=self._kb_mode(),
                parse_mode=enums.ParseMode.HTML,
            )
        else:
            await query.answer("Unknown action.")

    # ── Register handlers ─────────────────────────────────────────────────
    def _register(self, app: Client):
        app.on_message(filters.command("start"))(self.cmd_start)
        app.on_message(filters.command("help"))(self.cmd_help)
        app.on_message(filters.command("settings"))(self.cmd_settings)
        app.on_message(filters.command("anime"))(self.cmd_anime)
        app.on_message(filters.command("reload"))(self.cmd_reload)
        app.on_message(filters.command("stats"))(self.cmd_stats)
        app.on_message(filters.command("setsort"))(self.cmd_setsort)
        app.on_message(filters.command("broadcast"))(self.cmd_broadcast)
        app.on_callback_query()(self.handle_callback)

    # ── Lifecycle ─────────────────────────────────────────────────────────
    async def run(self):
        # Health-check server (Koyeb needs a TCP listener)
        self._health_runner = await start_health_server()

        # DB
        await self.db.init_indexes()

        # Pyrogram Client — created inside the running loop
        self.app = Client(
            "animebot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            parse_mode=enums.ParseMode.HTML,
        )
        self._register(self.app)
        self.sched.app_ref = self.app

        await self.app.start()

        # Scheduler
        self.sched.start()
        await self.sched.build_all_jobs()

        logger.info("Bot is running")
        await idle()          # blocks until SIGTERM / SIGINT

        # Graceful shutdown
        logger.info("Shutting down…")
        self.sched.shutdown()
        await self.api.close()
        await self.app.stop()
        if self._health_runner:
            await self._health_runner.cleanup()
        self.db.close()
        logger.info("Shutdown complete")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(AnimeBot().run())
