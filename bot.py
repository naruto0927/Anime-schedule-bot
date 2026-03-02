"""
Anime Schedule Telegram Bot
Using: Pyrogram, Motor (MongoDB), httpx, APScheduler
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters, idle
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
BOT_TOKEN  = os.environ["BOT_TOKEN"]
API_ID     = int(os.environ["API_ID"])
API_HASH   = os.environ["API_HASH"]
MONGO_URI  = os.environ["MONGO_URI"]
ADMIN_IDS  = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
TZ_NAME    = os.environ.get("TIMEZONE", "Asia/Kolkata")
TZ         = ZoneInfo(TZ_NAME)
AS_TOKEN   = os.environ.get("ANIMESCHEDULE_TOKEN", "")

ANIMESCHEDULE_API = "https://animeschedule.net/api/v3"
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


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
        await self.chats.update_one({"chat_id": chat_id}, {"$set": {"subscribed": False}})

    async def is_subscribed(self, chat_id: int) -> bool:
        doc = await self.chats.find_one({"chat_id": chat_id})
        return bool(doc and doc.get("subscribed"))

    async def all_subscribed_chats(self):
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

    # ── Jobs ───────────────────────────────────────────────────────────────
    async def save_job(self, job: dict):
        await self.jobs.update_one(
            {
                "anime_slug": job["anime_slug"],
                "episode":    job["episode"],
                "version":    job["version"],
            },
            {"$set": job},
            upsert=True,
        )

    async def all_jobs(self):
        return [d async for d in self.jobs.find({})]

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


# ──────────────────────────────────────────────────────────────────────────────
# AnimeSchedule API client
# ──────────────────────────────────────────────────────────────────────────────
class AnimeScheduleAPI:
    BASE = ANIMESCHEDULE_API

    def __init__(self, db: Database):
        self.db = db
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            headers = {"Content-Type": "application/json"}
            if AS_TOKEN:
                headers["Authorization"] = f"Bearer {AS_TOKEN}"
            self._client = httpx.AsyncClient(timeout=20, headers=headers)
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
            logger.error("API HTTP error %s: %s", url, exc)
            return None
        except Exception as exc:
            logger.error("API error %s: %s", url, exc)
            return None

    async def get_timetable(self, year: int, week: int) -> list:
        cache_key = f"timetable:{year}:{week}"
        cached = await self.db.get_cache(cache_key)
        if cached:
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

    async def get_anime_detail(self, slug: str):
        return await self._get(f"/anime/{slug}")


# ──────────────────────────────────────────────────────────────────────────────
# Schedule Processor
# ──────────────────────────────────────────────────────────────────────────────
def parse_time_to_ist(time_str: str) -> Optional[datetime]:
    if not time_str:
        return None
    try:
        time_str = time_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(time_str)
        return dt.astimezone(TZ)
    except Exception:
        return None


def format_12h(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.strftime("%I:%M %p")


def get_current_week_year():
    now = datetime.now(TZ)
    iso = now.isocalendar()
    return iso.year, iso.week


class ScheduleProcessor:

    @staticmethod
    def process_entries(entries: list) -> dict:
        day_map: dict = {d: [] for d in DAYS_OF_WEEK}
        seen: set = set()

        for entry in entries:
            slug = entry.get("route") or entry.get("slug") or entry.get("title", "")
            if not slug:
                continue

            title = entry.get("title", slug)

            # Skip donghua
            media_type = (entry.get("type") or "").lower()
            if "donghua" in media_type or entry.get("isDonghua"):
                continue

            raw_time = parse_time_to_ist(
                entry.get("episodeDate") or entry.get("airingAt") or ""
            )
            sub_time = parse_time_to_ist(
                entry.get("subDate") or entry.get("subAiringAt") or ""
            )
            dub_time = parse_time_to_ist(
                entry.get("dubDate") or entry.get("dubAiringAt") or ""
            )

            primary_dt = raw_time or sub_time or dub_time
            if not primary_dt:
                continue

            ep = entry.get("episodeNumber") or entry.get("episode") or 0
            dup_key = f"{slug}:{ep}"
            if dup_key in seen:
                continue
            seen.add(dup_key)

            total_eps  = entry.get("episodes") or entry.get("totalEpisodes") or "?"
            popularity = entry.get("likes") or entry.get("popularity") or 0

            processed = {
                "slug":       slug,
                "title":      title,
                "date":       primary_dt.strftime("%Y-%m-%d"),
                "weekday":    primary_dt.strftime("%A"),
                "raw_time":   raw_time,
                "sub_time":   sub_time,
                "dub_time":   dub_time,
                "episode":    ep,
                "total_eps":  total_eps,
                "thumbnail":  entry.get("imageVersionRoute") or entry.get("thumbnail") or "",
                "popularity": popularity,
            }

            day = primary_dt.strftime("%A")
            if day in day_map:
                day_map[day].append(processed)

        for day in day_map:
            day_map[day].sort(key=lambda x: x["popularity"], reverse=True)

        return day_map

    @staticmethod
    def format_anime_block(anime: dict, mode: str = "all") -> str:
        has_dub = anime["dub_time"] is not None
        has_sub = anime["sub_time"] is not None
        has_raw = anime["raw_time"] is not None

        if mode == "raw" and not has_raw:
            return ""
        if mode == "sub" and not has_sub:
            return ""
        if mode == "dub" and not has_dub:
            return ""

        # Display logic
        show_raw = has_raw
        show_sub = has_sub
        show_dub = has_dub

        if has_dub:
            pass  # show all existing
        elif has_sub:
            show_dub = False
        else:
            show_sub = False
            show_dub = False

        # Apply mode restriction
        if mode == "raw":
            show_sub = False
            show_dub = False
        elif mode == "sub":
            show_raw = False
            show_dub = False
        elif mode == "dub":
            show_raw = False
            show_sub = False

        lines = [
            f"🎬 <b>{anime['title']}</b>",
            f"📅 Date: {anime['date']}",
            f"📺 Episodes: {anime['episode']}/{anime['total_eps']}",
        ]
        if show_raw and anime["raw_time"]:
            lines.append(f"  🔴 Raw: {format_12h(anime['raw_time'])}")
        if show_sub and anime["sub_time"]:
            lines.append(f"  🔵 Sub: {format_12h(anime['sub_time'])}")
        if show_dub and anime["dub_time"]:
            lines.append(f"  🟢 Dub: {format_12h(anime['dub_time'])}")

        return "\n".join(lines)

    @staticmethod
    def format_day_schedule(day: str, anime_list: list, mode: str = "all") -> str:
        if not anime_list:
            return f"<b>📅 {day}</b>\n\nNo anime scheduled."

        blocks = [ScheduleProcessor.format_anime_block(a, mode) for a in anime_list]
        blocks = [b for b in blocks if b]

        if not blocks:
            return f"<b>📅 {day}</b>\n\nNo anime for your current mode."

        header = f"<b>📅 {day}</b> — {len(blocks)} anime\n"
        return header + "\n\n".join(blocks)


# ──────────────────────────────────────────────────────────────────────────────
# Reminder Scheduler
# ──────────────────────────────────────────────────────────────────────────────
class ReminderScheduler:
    def __init__(self, db: Database, api: AnimeScheduleAPI, bot: "AnimeBot"):
        self.db        = db
        self.api       = api
        self.bot       = bot
        self.scheduler = AsyncIOScheduler(timezone=TZ_NAME)

    def start(self):
        self.scheduler.start()
        logger.info("Scheduler started")

    async def stop(self):
        self.scheduler.shutdown(wait=False)

    async def build_all_jobs(self):
        await self.db.clear_jobs()
        self.scheduler.remove_all_jobs()

        year, week = get_current_week_year()
        for w in [week, week + 1]:
            entries = await self.api.get_timetable(year, w)
            processed = ScheduleProcessor.process_entries(entries)
            for day_list in processed.values():
                for anime in day_list:
                    await self._schedule_anime(anime)

        # Re-add periodic refresh after remove_all_jobs()
        self.scheduler.add_job(
            self.build_all_jobs,
            "interval",
            hours=6,
            id="refresh_schedule",
            replace_existing=True,
        )
        logger.info("Reminder jobs rebuilt")

    async def _schedule_anime(self, anime: dict):
        versions = {
            "raw": anime["raw_time"],
            "sub": anime["sub_time"],
            "dub": anime["dub_time"],
        }
        for version, dt in versions.items():
            if dt is None:
                continue
            now = datetime.now(TZ)
            if dt <= now:
                continue

            job_meta = {
                "anime_slug":  anime["slug"],
                "anime_title": anime["title"],
                "episode":     anime["episode"],
                "total_eps":   anime["total_eps"],
                "version":     version,
                "air_time":    dt.isoformat(),
            }
            await self.db.save_job(job_meta)

            job_id = f"{anime['slug']}:{anime['episode']}:{version}"
            if self.scheduler.get_job(job_id):
                continue

            self.scheduler.add_job(
                self._send_reminder,
                "date",
                run_date=dt,
                id=job_id,
                args=[job_meta],
                replace_existing=True,
                misfire_grace_time=600,
            )

    async def _send_reminder(self, job: dict):
        chats    = await self.db.all_subscribed_chats()
        title    = job["anime_title"]
        version  = job["version"].capitalize()
        ep       = job["episode"]
        total    = job["total_eps"]
        air_dt   = datetime.fromisoformat(job["air_time"]).astimezone(TZ)
        time_str = format_12h(air_dt)

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
                await self.bot.app.send_message(chat_id, text, parse_mode="html")
            except Exception as exc:
                logger.warning("Reminder send failed to %s: %s", chat_id, exc)


# ──────────────────────────────────────────────────────────────────────────────
# Bot
# ──────────────────────────────────────────────────────────────────────────────
class AnimeBot:
    def __init__(self):
        self.db    = Database(MONGO_URI)
        self.api   = AnimeScheduleAPI(self.db)
        self.sched = ReminderScheduler(self.db, self.api, self)

        self.app = Client(
            "animebot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
        )
        self._register_handlers()

    def _register_handlers(self):
        self.app.on_message(filters.command("start"))(self.cmd_start)
        self.app.on_message(filters.command("help"))(self.cmd_help)
        self.app.on_message(filters.command("settings"))(self.cmd_settings)
        self.app.on_message(filters.command("anime"))(self.cmd_anime)
        self.app.on_message(filters.command("reload"))(self.cmd_reload)
        self.app.on_message(filters.command("stats"))(self.cmd_stats)
        self.app.on_message(filters.command("setsort"))(self.cmd_setsort)
        self.app.on_message(filters.command("broadcast"))(self.cmd_broadcast)
        self.app.on_callback_query()(self.handle_callback)

    # ── Keyboards ─────────────────────────────────────────────────────────
    def _settings_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 View Schedule",       callback_data="menu:schedule")],
            [InlineKeyboardButton("🔔 Reminder Settings",  callback_data="menu:reminders")],
            [InlineKeyboardButton("🎛 Mode Settings",       callback_data="menu:mode")],
            [InlineKeyboardButton("📊 Weekly View",         callback_data="menu:weekly")],
            [InlineKeyboardButton("❌ Close",               callback_data="menu:close")],
        ])

    def _days_keyboard(self) -> InlineKeyboardMarkup:
        rows = []
        for i in range(0, len(DAYS_OF_WEEK), 2):
            row = [
                InlineKeyboardButton(day, callback_data=f"day:{day}")
                for day in DAYS_OF_WEEK[i:i+2]
            ]
            rows.append(row)
        rows.append([InlineKeyboardButton("◀ Back", callback_data="menu:back")])
        return InlineKeyboardMarkup(rows)

    def _reminder_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Subscribe",            callback_data="rem:subscribe")],
            [InlineKeyboardButton("❌ Unsubscribe",          callback_data="rem:unsubscribe")],
            [InlineKeyboardButton("📋 Subscription Status",  callback_data="rem:status")],
            [InlineKeyboardButton("◀ Back",                  callback_data="menu:back")],
        ])

    def _mode_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🔴 Raw Only",     callback_data="mode:raw")],
            [InlineKeyboardButton("🔵 Sub Only",     callback_data="mode:sub")],
            [InlineKeyboardButton("🟢 Dub Only",     callback_data="mode:dub")],
            [InlineKeyboardButton("🌟 All Versions", callback_data="mode:all")],
            [InlineKeyboardButton("◀ Back",          callback_data="menu:back")],
        ])

    # ── Schedule helper ───────────────────────────────────────────────────
    async def _get_schedule(self) -> dict:
        year, week = get_current_week_year()
        entries = await self.api.get_timetable(year, week)
        return ScheduleProcessor.process_entries(entries)

    # ── Commands ──────────────────────────────────────────────────────────
    async def cmd_start(self, _app, message: Message):
        await message.reply(
            "👋 <b>Welcome to Anime Schedule Bot!</b>\n\n"
            "I track anime airing times and send episode reminders.\n\n"
            "Use /settings to open the panel or /help for more info.",
            parse_mode="html",
        )

    async def cmd_help(self, _app, message: Message):
        await message.reply(
            "<b>📖 How to use this bot</b>\n\n"
            "<b>Commands:</b>\n"
            "/settings — Open interactive settings panel\n"
            "/anime &lt;name&gt; — Search for anime info\n"
            "/help — Show this message\n\n"
            "<b>Settings Panel:</b>\n"
            "• <b>View Schedule</b> — See what's airing each day\n"
            "• <b>Reminder Settings</b> — Subscribe/unsubscribe to episode notifications\n"
            "• <b>Mode Settings</b> — Filter by Raw / Sub / Dub / All\n"
            "• <b>Weekly View</b> — See full week overview\n\n"
            "<b>Modes:</b>\n"
            "🔴 Raw — Japanese broadcast only\n"
            "🔵 Sub — Subtitled version\n"
            "🟢 Dub — Dubbed version\n"
            "🌟 All — Show all versions\n\n"
            f"⏰ All times shown in <b>{TZ_NAME}</b>",
            parse_mode="html",
        )

    async def cmd_settings(self, _app, message: Message):
        await message.reply(
            "⚙️ <b>Settings Panel</b>\n\nChoose an option below:",
            reply_markup=self._settings_keyboard(),
            parse_mode="html",
        )

    async def cmd_anime(self, _app, message: Message):
        query = " ".join(message.command[1:]).strip()
        if not query:
            await message.reply("Usage: /anime &lt;name&gt;", parse_mode="html")
            return

        await message.reply(f"🔍 Searching for <i>{query}</i>…", parse_mode="html")
        results = await self.api.search_anime(query)

        if not results:
            await message.reply("❌ No results found.")
            return

        anime     = results[0]
        title     = anime.get("title") or anime.get("name") or "Unknown"
        synopsis  = anime.get("synopsis") or anime.get("description") or "No synopsis available."
        episodes  = anime.get("episodes") or "?"
        status    = anime.get("status") or anime.get("airingStatus") or "Unknown"
        thumbnail = anime.get("imageVersionRoute") or anime.get("thumbnail") or None

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
                await message.reply_photo(photo=thumbnail, caption=caption, parse_mode="html")
                return
            except Exception:
                pass

        await message.reply(caption, parse_mode="html")

    # ── Admin ─────────────────────────────────────────────────────────────
    async def _check_admin(self, message: Message) -> bool:
        if message.from_user and message.from_user.id in ADMIN_IDS:
            return True
        await message.reply("⛔ Admin only command.")
        return False

    async def cmd_reload(self, _app, message: Message):
        if not await self._check_admin(message):
            return
        await message.reply("🔄 Reloading schedule and rebuilding reminder jobs…")
        await self.db.cache.drop()
        await self.sched.build_all_jobs()
        await message.reply("✅ Done.")

    async def cmd_stats(self, _app, message: Message):
        if not await self._check_admin(message):
            return
        stats       = await self.db.get_stats()
        n_chats     = await self.db.count_documents("chats")
        n_jobs      = await self.db.count_documents("jobs")
        n_cache     = await self.db.count_documents("cache")
        active_jobs = len(self.sched.scheduler.get_jobs())

        await message.reply(
            "<b>📊 Bot Statistics</b>\n\n"
            f"API Calls:               {stats.get('api_calls', 0)}\n"
            f"Cache Hits:              {stats.get('cache_hits', 0)}\n"
            f"Active Scheduler Jobs:   {active_jobs}\n"
            f"Subscribed Chats (DB):   {n_chats}\n"
            f"Reminder Job Docs (DB):  {n_jobs}\n"
            f"Cached Responses (DB):   {n_cache}",
            parse_mode="html",
        )

    async def cmd_setsort(self, _app, message: Message):
        if not await self._check_admin(message):
            return
        await message.reply(
            "ℹ️ Sort is currently fixed to popularity. Future versions will support custom sort."
        )

    async def cmd_broadcast(self, _app, message: Message):
        if not await self._check_admin(message):
            return
        text = " ".join(message.command[1:]).strip()
        if not text:
            await message.reply("Usage: /broadcast &lt;message&gt;", parse_mode="html")
            return
        chats = await self.db.all_subscribed_chats()
        sent  = 0
        for chat_id in chats:
            try:
                await self.app.send_message(chat_id, text)
                sent += 1
            except Exception:
                pass
        await message.reply(f"📢 Broadcast sent to {sent}/{len(chats)} chats.")

    # ── Callbacks ─────────────────────────────────────────────────────────
    async def handle_callback(self, _app, query: CallbackQuery):
        data    = query.data
        chat_id = query.message.chat.id

        if data == "menu:back":
            await query.edit_message_text(
                "⚙️ <b>Settings Panel</b>\n\nChoose an option below:",
                reply_markup=self._settings_keyboard(),
                parse_mode="html",
            )

        elif data == "menu:close":
            await query.message.delete()

        elif data == "menu:schedule":
            await query.edit_message_text(
                "📅 <b>View Schedule</b>\n\nSelect a day:",
                reply_markup=self._days_keyboard(),
                parse_mode="html",
            )

        elif data == "menu:reminders":
            await query.edit_message_text(
                "🔔 <b>Reminder Settings</b>\n\nManage your episode notifications:",
                reply_markup=self._reminder_keyboard(),
                parse_mode="html",
            )

        elif data == "menu:mode":
            mode = await self.db.get_mode(chat_id)
            await query.edit_message_text(
                f"🎛 <b>Mode Settings</b>\n\nCurrent mode: <b>{mode.capitalize()}</b>\n\n"
                f"Select version to track:",
                reply_markup=self._mode_keyboard(),
                parse_mode="html",
            )

        elif data == "menu:weekly":
            await query.answer("Loading weekly overview…")
            schedule = await self._get_schedule()
            lines = ["<b>📊 Weekly Anime Overview</b>\n"]
            for day in DAYS_OF_WEEK:
                count = len(schedule.get(day, []))
                lines.append(f"• <b>{day}</b>: {count} anime")
            await query.edit_message_text(
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀ Back", callback_data="menu:back")]]
                ),
                parse_mode="html",
            )

        elif data.startswith("day:"):
            day = data.split(":", 1)[1]
            await query.answer(f"Loading {day}…")
            schedule = await self._get_schedule()
            mode     = await self.db.get_mode(chat_id)
            day_list = schedule.get(day, [])
            text     = ScheduleProcessor.format_day_schedule(day, day_list, mode)
            if len(text) > 4000:
                text = text[:3997] + "…"
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀ Back to Days", callback_data="menu:schedule")]]
                ),
                parse_mode="html",
            )

        elif data == "rem:subscribe":
            await self.db.subscribe(chat_id)
            await query.answer("✅ Subscribed to episode reminders!", show_alert=True)

        elif data == "rem:unsubscribe":
            await self.db.unsubscribe(chat_id)
            await query.answer("❌ Unsubscribed from reminders.", show_alert=True)

        elif data == "rem:status":
            sub    = await self.db.is_subscribed(chat_id)
            status = "✅ Subscribed" if sub else "❌ Not subscribed"
            await query.answer(f"Status: {status}", show_alert=True)

        elif data.startswith("mode:"):
            mode   = data.split(":", 1)[1]
            labels = {
                "raw": "🔴 Raw Only",
                "sub": "🔵 Sub Only",
                "dub": "🟢 Dub Only",
                "all": "🌟 All Versions",
            }
            await self.db.set_mode(chat_id, mode)
            await query.answer(f"Mode set to {labels.get(mode, mode)}", show_alert=True)
            await query.edit_message_text(
                f"🎛 <b>Mode Settings</b>\n\nCurrent mode: <b>{mode.capitalize()}</b>\n\n"
                f"Select version to track:",
                reply_markup=self._mode_keyboard(),
                parse_mode="html",
            )

        else:
            await query.answer("Unknown action.")

    # ── Lifecycle ─────────────────────────────────────────────────────────
    async def start(self):
        await self.db.init_indexes()
        await self.app.start()
        self.sched.start()
        await self.sched.build_all_jobs()
        logger.info("Bot started successfully")

    async def stop(self):
        logger.info("Shutting down…")
        await self.sched.stop()
        await self.api.close()
        await self.app.stop()
        self.db.client.close()
        logger.info("Shutdown complete")

    def run(self):
        async def _main():
            await self.start()
            await idle()          # ← correct Pyrogram 2.x way to keep bot running
            await self.stop()

        try:
            asyncio.run(_main())
        except KeyboardInterrupt:
            logger.info("Interrupted by user")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bot = AnimeBot()
    bot.run()
