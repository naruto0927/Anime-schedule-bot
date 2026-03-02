"""
Naruto Timekeeper — Anime Schedule Telegram Bot
Pyrogram + Motor + httpx + APScheduler + AniList
"""

import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters, idle, enums
from pyrogram.enums import ChatType
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
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
BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID    = int(os.environ["API_ID"])
API_HASH  = os.environ["API_HASH"]
MONGO_URI = os.environ["MONGO_URI"]
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
TZ_NAME   = os.environ.get("TIMEZONE", "Asia/Kolkata")
TZ        = ZoneInfo(TZ_NAME)
AS_TOKEN  = os.environ.get("ANIMESCHEDULE_TOKEN", "")
PORT      = int(os.environ.get("PORT", "8000"))
BOT_IMAGE = os.environ.get("BOT_IMAGE_URL", "")  # optional welcome image URL

ANIMESCHEDULE_API = "https://animeschedule.net/api/v3"
ANILIST_API       = "https://graphql.anilist.co"
DAYS_OF_WEEK      = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# ── Default filter settings (mirrors animeschedule.net) ──────────────────────
DEFAULT_FILTERS = {
    # Air Type: which air types to include
    "air_raw":  True,
    "air_sub":  True,
    "air_dub":  True,
    # Hide Raw When
    "hide_raw_sub_exists": True,   # default ON per site
    "hide_raw_dub_exists": False,
    # Hide Sub When
    "hide_sub_dub_exists": False,
    "hide_sub_raw_exists": False,
    # Hide Dub When
    "hide_dub_sub_exists": False,
    "hide_dub_raw_exists": False,
    # Time Format: "12h" or "24h"
    "time_format": "12h",
    # Images in schedule output
    "show_images": True,
    # Show/hide Donghua
    "show_donghua": False,
    # Sort By: "popularity", "time", "alphabetical", "unaired"
    "sort_by": "popularity",
}


# ── Health server ─────────────────────────────────────────────────────────────
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
        self.client = AsyncIOMotorClient(uri)
        self.db     = self.client["animebot"]
        self.chats  = self.db["chats"]
        self.modes  = self.db["modes"]
        self.jobs   = self.db["jobs"]
        self.cache  = self.db["cache"]
        self.stats  = self.db["stats"]
        self.auth   = self.db["auth"]
        self.fltrs  = self.db["filters"]

    async def init_indexes(self):
        await self.chats.create_index("chat_id", unique=True)
        await self.modes.create_index("chat_id", unique=True)
        await self.jobs.create_index(
            [("anime_slug", 1), ("episode", 1), ("version", 1)], unique=True
        )
        await self.cache.create_index("key", unique=True)
        await self.auth.create_index("chat_id", unique=True)
        await self.fltrs.create_index("chat_id", unique=True)
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

    # ── Modes ─────────────────────────────────────────────────────────────
    async def set_mode(self, chat_id: int, mode: str):
        await self.modes.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, "mode": mode}},
            upsert=True,
        )

    async def get_mode(self, chat_id: int) -> str:
        doc = await self.modes.find_one({"chat_id": chat_id})
        return doc["mode"] if doc else "all"

    # ── Filters ───────────────────────────────────────────────────────────
    async def get_filters(self, chat_id: int) -> dict:
        doc = await self.fltrs.find_one({"chat_id": chat_id})
        result = dict(DEFAULT_FILTERS)
        if doc:
            result.update({k: v for k, v in doc.items() if k not in ("_id", "chat_id")})
        return result

    async def set_filter(self, chat_id: int, key: str, value):
        await self.fltrs.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, key: value}},
            upsert=True,
        )

    async def reset_filters(self, chat_id: int):
        await self.fltrs.delete_one({"chat_id": chat_id})

    # ── Jobs ──────────────────────────────────────────────────────────────
    async def bulk_save_jobs(self, job_list: list):
        if not job_list:
            return
        from pymongo import UpdateOne
        ops = [
            UpdateOne(
                {"anime_slug": j["anime_slug"], "episode": j["episode"], "version": j["version"]},
                {"$set": j}, upsert=True,
            )
            for j in job_list
        ]
        await self.jobs.bulk_write(ops, ordered=False)

    async def clear_jobs(self):
        await self.jobs.delete_many({})

    # ── Cache ─────────────────────────────────────────────────────────────
    async def set_cache(self, key: str, value, ttl: int = 21600):
        exp = datetime.utcnow() + timedelta(seconds=ttl)
        await self.cache.update_one(
            {"key": key},
            {"$set": {"key": key, "value": value, "expires_at": exp}},
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

    # ── Stats ─────────────────────────────────────────────────────────────
    async def inc_stat(self, field: str, n: int = 1):
        await self.stats.update_one({"_id": "global"}, {"$inc": {field: n}}, upsert=True)

    async def get_stats(self) -> dict:
        doc = await self.stats.find_one({"_id": "global"}) or {}
        doc.pop("_id", None)
        return doc

    async def count(self, col: str) -> int:
        return await self.db[col].count_documents({})

    def close(self):
        self.client.close()


# ── AnimeSchedule API ─────────────────────────────────────────────────────────
class AnimeScheduleAPI:
    BASE = ANIMESCHEDULE_API

    def __init__(self, db: Database):
        self.db = db
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            h = {"Accept": "application/json"}
            if AS_TOKEN:
                h["Authorization"] = f"Bearer {AS_TOKEN}"
            self._client = httpx.AsyncClient(timeout=30, headers=h)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _get(self, path: str, params: dict = None):
        url = f"{self.BASE}{path}"
        try:
            r = await self.client.get(url, params=params or {})
            r.raise_for_status()
            await self.db.inc_stat("as_api_calls")
            return r.json()
        except httpx.HTTPStatusError as e:
            logger.error("AS API [%s] %s", e.response.status_code, url)
            return None
        except Exception as e:
            logger.error("AS API error: %s", e)
            return None

    async def get_timetable(self, year: int, week: int) -> list:
        key = f"timetable:{year}:{week}"
        cached = await self.db.get_cache(key)
        if cached is not None:
            await self.db.inc_stat("cache_hits")
            return cached
        data = await self._get("/timetables", {"year": year, "week": week})
        if data is None:
            return []
        entries = data if isinstance(data, list) else data.get("entries", [])
        await self.db.set_cache(key, entries)
        return entries


# ── AniList API ───────────────────────────────────────────────────────────────
class AniListAPI:
    URL = ANILIST_API
    _ANIME_Q = """query($s:String){Media(search:$s,type:ANIME,sort:SEARCH_MATCH){
        id title{romaji english native} description(asHtml:false) episodes status
        averageScore genres coverImage{large} siteUrl startDate{year month day}
        studios(isMain:true){nodes{name}}}}"""
    _MANGA_Q = """query($s:String){Media(search:$s,type:MANGA,sort:SEARCH_MATCH){
        id title{romaji english native} description(asHtml:false) chapters volumes status
        averageScore genres coverImage{large} siteUrl startDate{year month day}
        staff(sort:RELEVANCE){nodes{name{full}}}}}"""

    def __init__(self, db: Database):
        self.db = db
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=30,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _gql(self, query: str, search: str) -> Optional[dict]:
        try:
            r = await self.client.post(self.URL, json={"query": query, "variables": {"s": search}})
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            return data.get("data", {}).get("Media") if "errors" not in data else None
        except Exception as e:
            logger.error("AniList error: %s", e)
            return None

    async def search_anime(self, q: str): return await self._gql(self._ANIME_Q, q)
    async def search_manga(self, q: str): return await self._gql(self._MANGA_Q, q)


# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(TZ)
    except Exception:
        return None


def fmt_time(dt: Optional[datetime], fmt: str = "12h") -> str:
    if not dt:
        return ""
    return dt.strftime("%I:%M %p") if fmt == "12h" else dt.strftime("%H:%M")


def current_week_year():
    iso = datetime.now(TZ).isocalendar()
    return iso.year, iso.week


def clean_html(text: str, limit: int = 900) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    for old, new in [("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"), ("&#039;", "'"), ("&quot;", '"')]:
        text = text.replace(old, new)
    text = text.strip()
    return text[:limit - 1] + "\u2026" if len(text) > limit else text


def al_status(s: str) -> str:
    return {"FINISHED": "Finished", "RELEASING": "\U0001f7e2 Releasing",
            "NOT_YET_RELEASED": "Not Yet Released", "CANCELLED": "Cancelled",
            "HIATUS": "On Hiatus"}.get(s or "", s or "Unknown")


# ── Schedule Processor ────────────────────────────────────────────────────────
class ScheduleProcessor:

    @staticmethod
    def process(entries: list, f: dict) -> dict:
        show_donghua = f.get("show_donghua", False)
        air_raw      = f.get("air_raw", True)
        air_sub      = f.get("air_sub", True)
        air_dub      = f.get("air_dub", True)
        sort_by      = f.get("sort_by", "popularity")
        incl_unaired = sort_by == "unaired"

        best: dict = {}
        for e in entries:
            slug  = e.get("route") or e.get("slug") or e.get("title", "")
            title = e.get("title") or slug
            if not slug:
                continue

            mt         = (e.get("type") or "").lower()
            is_donghua = "donghua" in mt or bool(e.get("isDonghua"))
            if is_donghua and not show_donghua:
                continue

            raw = parse_dt(e.get("episodeDate") or e.get("airingAt") or "") if air_raw else None
            sub = parse_dt(e.get("subDate") or e.get("subAiringAt") or "") if air_sub else None
            dub = parse_dt(e.get("dubDate") or e.get("dubAiringAt") or "") if air_dub else None

            primary = raw or sub or dub
            if not primary:
                continue

            day = primary.strftime("%A")
            if day not in DAYS_OF_WEEK:
                continue

            ep = e.get("episodeNumber") or e.get("episode") or 0
            try:
                ep_num = float(ep)
            except (TypeError, ValueError):
                ep_num = 0

            key      = (slug, day)
            existing = best.get(key)
            if existing is None:
                best[key] = {
                    "slug":       slug,
                    "title":      title,
                    "date":       primary.strftime("%Y-%m-%d"),
                    "raw_time":   raw,
                    "sub_time":   sub,
                    "dub_time":   dub,
                    "episode":    ep,
                    "ep_num":     ep_num,
                    "total_eps":  e.get("episodes") or e.get("totalEpisodes") or "?",
                    "thumbnail":  e.get("imageVersionRoute") or e.get("thumbnail") or "",
                    "popularity": e.get("likes") or e.get("popularity") or 0,
                    "unaired":    not bool(e.get("airingAt") or e.get("episodeDate")),
                }
            else:
                if ep_num > existing["ep_num"]:
                    existing["episode"] = ep
                    existing["ep_num"]  = ep_num
                    existing["date"]    = primary.strftime("%Y-%m-%d")
                    if raw:
                        existing["raw_time"] = raw
                if sub and not existing["sub_time"]:
                    existing["sub_time"] = sub
                if dub and not existing["dub_time"]:
                    existing["dub_time"] = dub
                if e.get("episodes") or e.get("totalEpisodes"):
                    existing["total_eps"] = e.get("episodes") or e.get("totalEpisodes")

        day_map = {d: [] for d in DAYS_OF_WEEK}
        for (slug, day), anime in best.items():
            anime.pop("ep_num", None)
            day_map[day].append(anime)

        def sort_key(x):
            if sort_by == "time":
                t = x["raw_time"] or x["sub_time"] or x["dub_time"]
                return t.timestamp() if t else 0
            elif sort_by == "alphabetical":
                return x["title"].lower()
            elif sort_by == "unaired":
                return (0 if x.get("unaired") else 1, x["title"].lower())
            else:
                return -(x.get("popularity") or 0)

        for d in day_map:
            day_map[d].sort(key=sort_key)

        return day_map

    @staticmethod
    def _apply_hide_rules(a: dict, f: dict):
        has_raw = a["raw_time"] is not None
        has_sub = a["sub_time"] is not None
        has_dub = a["dub_time"] is not None
        show_raw, show_sub, show_dub = has_raw, has_sub, has_dub
        if has_sub and f.get("hide_raw_sub_exists", True):  show_raw = False
        if has_dub and f.get("hide_raw_dub_exists", False): show_raw = False
        if has_dub and f.get("hide_sub_dub_exists", False): show_sub = False
        if has_raw and f.get("hide_sub_raw_exists", False): show_sub = False
        if has_sub and f.get("hide_dub_sub_exists", False): show_dub = False
        if has_raw and f.get("hide_dub_raw_exists", False): show_dub = False
        return show_raw, show_sub, show_dub

    @staticmethod
    def anime_block(a: dict, f: dict) -> str:
        has_raw = a["raw_time"] is not None
        has_sub = a["sub_time"] is not None
        has_dub = a["dub_time"] is not None
        if not (has_raw or has_sub or has_dub):
            return ""
        show_raw, show_sub, show_dub = ScheduleProcessor._apply_hide_rules(a, f)
        if not (show_raw or show_sub or show_dub):
            return ""
        tf = f.get("time_format", "12h")
        lines = [
            f"\U0001f3ac <b>{a['title']}</b>",
            f"\U0001f4c5 {a['date']}  \U0001f4fa Ep {a['episode']}/{a['total_eps']}",
        ]
        if show_raw and a["raw_time"]:
            lines.append(f"  \U0001f534 Raw: {fmt_time(a['raw_time'], tf)}")
        if show_sub and a["sub_time"]:
            lines.append(f"  \U0001f535 Sub: {fmt_time(a['sub_time'], tf)}")
        if show_dub and a["dub_time"]:
            lines.append(f"  \U0001f7e2 Dub: {fmt_time(a['dub_time'], tf)}")
        return "\n".join(lines)

    @staticmethod
    def day_text(day: str, anime_list: list, f: dict) -> str:
        if not anime_list:
            return f"<b>\U0001f4c5 {day}</b>\n\nNo anime scheduled."
        blocks = [b for b in (ScheduleProcessor.anime_block(a, f) for a in anime_list) if b]
        if not blocks:
            return f"<b>\U0001f4c5 {day}</b>\n\nNo anime matches your current filters."
        return f"<b>\U0001f4c5 {day}</b> \u2014 {len(blocks)} anime\n\n" + "\n\n".join(blocks)

    def day_pages(day: str, anime_list: list, f: dict, max_len: int = 3800) -> list:
        """Split day schedule into pages that fit within Telegram's text limit."""
        if not anime_list:
            return [f"<b>\U0001f4c5 {day}</b>\n\nNo anime scheduled."]
        blocks = [b for b in (ScheduleProcessor.anime_block(a, f) for a in anime_list) if b]
        if not blocks:
            return [f"<b>\U0001f4c5 {day}</b>\n\nNo anime matches your current filters."]

        pages = []
        current_blocks = []
        current_len = 0
        total = len(blocks)

        for block in blocks:
            # +2 for the "\n\n" separator
            if current_blocks and current_len + len(block) + 2 > max_len:
                pages.append(current_blocks)
                current_blocks = [block]
                current_len = len(block)
            else:
                current_blocks.append(block)
                current_len += len(block) + (2 if current_blocks else 0)

        if current_blocks:
            pages.append(current_blocks)

        result = []
        for i, page_blocks in enumerate(pages):
            header = f"<b>\U0001f4c5 {day}</b> \u2014 {total} anime"
            if len(pages) > 1:
                header += f"  <i>(Page {i+1}/{len(pages)})</i>"
            result.append(header + "\n\n" + "\n\n".join(page_blocks))
        return result


# ── Filter Panel ──────────────────────────────────────────────────────────────
def _chk(active: bool) -> str:
    return "\u2705" if active else "\u25fb\ufe0f"


def _sel(active: bool) -> str:
    return "\u25b6\ufe0f " if active else ""


def build_filter_kb(f: dict, page: int = 1) -> InlineKeyboardMarkup:
    """Paginated filter keyboard to stay within Telegram caption limits.
    Page 1: Air Type + Hide rules
    Page 2: Time Format, Images, Donghua, Sort By
    """
    sb = f.get("sort_by", "popularity")
    tf = f.get("time_format", "12h")
    si = f.get("show_images", True)
    sd = f.get("show_donghua", False)

    if page == 1:
        rows = [
            [InlineKeyboardButton("\u2500\u2500\u2500 Air Type \u2500\u2500\u2500", callback_data="fltr:_")],
            [
                InlineKeyboardButton(f"{_chk(f['air_raw'])} Raw",  callback_data="fltr:t:air_raw"),
                InlineKeyboardButton(f"{_chk(f['air_sub'])} Sub",  callback_data="fltr:t:air_sub"),
                InlineKeyboardButton(f"{_chk(f['air_dub'])} Dub",  callback_data="fltr:t:air_dub"),
            ],
            [InlineKeyboardButton("\u2500\u2500\u2500 Hide Raw When \u2500\u2500\u2500", callback_data="fltr:_")],
            [
                InlineKeyboardButton(f"{_chk(f['hide_raw_sub_exists'])} Sub Exists",
                                     callback_data="fltr:t:hide_raw_sub_exists"),
                InlineKeyboardButton(f"{_chk(f['hide_raw_dub_exists'])} Dub Exists",
                                     callback_data="fltr:t:hide_raw_dub_exists"),
            ],
            [InlineKeyboardButton("\u2500\u2500\u2500 Hide Sub When \u2500\u2500\u2500", callback_data="fltr:_")],
            [
                InlineKeyboardButton(f"{_chk(f['hide_sub_dub_exists'])} Dub Exists",
                                     callback_data="fltr:t:hide_sub_dub_exists"),
                InlineKeyboardButton(f"{_chk(f['hide_sub_raw_exists'])} Raw Exists",
                                     callback_data="fltr:t:hide_sub_raw_exists"),
            ],
            [InlineKeyboardButton("\u2500\u2500\u2500 Hide Dub When \u2500\u2500\u2500", callback_data="fltr:_")],
            [
                InlineKeyboardButton(f"{_chk(f['hide_dub_sub_exists'])} Sub Exists",
                                     callback_data="fltr:t:hide_dub_sub_exists"),
                InlineKeyboardButton(f"{_chk(f['hide_dub_raw_exists'])} Raw Exists",
                                     callback_data="fltr:t:hide_dub_raw_exists"),
            ],
            [
                InlineKeyboardButton("\u25c0 Back",       callback_data="menu:back"),
                InlineKeyboardButton("\U0001f504 Reset",  callback_data="fltr:reset"),
                InlineKeyboardButton("More \u25b6",       callback_data="fltr:pg:2"),
            ],
            [InlineKeyboardButton("\u2714\ufe0f Close",   callback_data="fltr:close")],
        ]
    else:
        rows = [
            [InlineKeyboardButton("\u2500\u2500\u2500 Time Format \u2500\u2500\u2500", callback_data="fltr:_")],
            [
                InlineKeyboardButton(f"{_sel(tf=='12h')}12H", callback_data="fltr:s:time_format:12h"),
                InlineKeyboardButton(f"{_sel(tf=='24h')}24H", callback_data="fltr:s:time_format:24h"),
            ],
            [InlineKeyboardButton("\u2500\u2500\u2500 Images \u2500\u2500\u2500", callback_data="fltr:_")],
            [
                InlineKeyboardButton(f"{_sel(si)}Show",     callback_data="fltr:s:show_images:true"),
                InlineKeyboardButton(f"{_sel(not si)}Hide", callback_data="fltr:s:show_images:false"),
            ],
            [InlineKeyboardButton("\u2500\u2500\u2500 Donghua \u2500\u2500\u2500", callback_data="fltr:_")],
            [
                InlineKeyboardButton(f"{_sel(sd)}Show",     callback_data="fltr:s:show_donghua:true"),
                InlineKeyboardButton(f"{_sel(not sd)}Hide", callback_data="fltr:s:show_donghua:false"),
            ],
            [InlineKeyboardButton("\u2500\u2500\u2500 Sort By \u2500\u2500\u2500", callback_data="fltr:_")],
            [
                InlineKeyboardButton(f"{_sel(sb=='alphabetical')}A-Z",
                                     callback_data="fltr:s:sort_by:alphabetical"),
                InlineKeyboardButton(f"{_sel(sb=='time')}Time",
                                     callback_data="fltr:s:sort_by:time"),
                InlineKeyboardButton(f"{_sel(sb=='popularity')}Popularity",
                                     callback_data="fltr:s:sort_by:popularity"),
                InlineKeyboardButton(f"{_sel(sb=='unaired')}Unaired",
                                     callback_data="fltr:s:sort_by:unaired"),
            ],
            [
                InlineKeyboardButton("\u25c4 Back",       callback_data="fltr:pg:1"),
                InlineKeyboardButton("\U0001f504 Reset",  callback_data="fltr:reset"),
                InlineKeyboardButton("\u2714\ufe0f Close", callback_data="fltr:close"),
            ],
        ]

    return InlineKeyboardMarkup(rows)


def filter_text(f: dict, page: int = 1) -> str:
    sb = f.get("sort_by", "popularity").capitalize()
    tf = f.get("time_format", "12h").upper()
    si = "Show" if f.get("show_images", True) else "Hide"
    sd = "Show" if f.get("show_donghua", False) else "Hide"

    air = " | ".join(v for k, v in [("air_raw", "Raw"), ("air_sub", "Sub"), ("air_dub", "Dub")]
                     if f.get(k, True))
    hide_raw = " / ".join(filter(None, [
        "Sub Exists" if f.get("hide_raw_sub_exists") else "",
        "Dub Exists" if f.get("hide_raw_dub_exists") else "",
    ])) or "Never"
    hide_sub = " / ".join(filter(None, [
        "Dub Exists" if f.get("hide_sub_dub_exists") else "",
        "Raw Exists" if f.get("hide_sub_raw_exists") else "",
    ])) or "Never"
    hide_dub = " / ".join(filter(None, [
        "Sub Exists" if f.get("hide_dub_sub_exists") else "",
        "Raw Exists" if f.get("hide_dub_raw_exists") else "",
    ])) or "Never"

    if page == 1:
        return (
            "\U0001f3a8 <b>Filter Settings</b> <i>(Page 1/2 \u2014 Air &amp; Hide Rules)</i>\n\n"
            f"\U0001f4e1 <b>Air Type:</b> {air or 'None'}\n"
            f"\U0001f534 <b>Hide Raw When:</b> {hide_raw}\n"
            f"\U0001f535 <b>Hide Sub When:</b> {hide_sub}\n"
            f"\U0001f7e2 <b>Hide Dub When:</b> {hide_dub}\n\n"
            "<i>Tap any button below to change settings.\nChanges apply instantly.</i>"
        )
    else:
        return (
            "\U0001f3a8 <b>Filter Settings</b> <i>(Page 2/2 \u2014 Display Options)</i>\n\n"
            f"\U0001f550 <b>Time Format:</b> {tf}\n"
            f"\U0001f5bc <b>Images:</b> {si}\n"
            f"\U0001f409 <b>Donghua:</b> {sd}\n"
            f"\U0001f4ca <b>Sort By:</b> {sb}\n\n"
            "<i>Tap any button below to change settings.\nChanges apply instantly.</i>"
        )


# ── Reminder Scheduler ────────────────────────────────────────────────────────
class ReminderScheduler:
    def __init__(self, db: Database, api: AnimeScheduleAPI):
        self.db        = db
        self.api       = api
        self.app_ref   = None
        self.scheduler = AsyncIOScheduler(timezone=TZ_NAME)

    def start(self):
        self.scheduler.start()
        logger.info("Scheduler started")

    def shutdown(self):
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    async def build_all_jobs(self):
        await self.db.clear_jobs()
        self.scheduler.remove_all_jobs()
        year, week = current_week_year()
        all_jobs = []
        for w in [week, week + 1]:
            entries   = await self.api.get_timetable(year, w)
            processed = ScheduleProcessor.process(entries, DEFAULT_FILTERS)
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
                self._send_reminder, "date",
                run_date=dt, id=jid, args=[j],
                replace_existing=True, misfire_grace_time=600,
            )
            added += 1
        self.scheduler.add_job(
            self.build_all_jobs, "interval", hours=6,
            id="refresh_schedule", replace_existing=True,
        )
        logger.info("Built %d reminder jobs", added)

    def _make_jobs(self, anime: dict) -> list:
        jobs = []
        for v in ("raw", "sub", "dub"):
            dt = anime[f"{v}_time"]
            if dt is None:
                continue
            jobs.append({
                "anime_slug":  anime["slug"],
                "anime_title": anime["title"],
                "episode":     anime["episode"],
                "total_eps":   anime["total_eps"],
                "version":     v,
                "air_time":    dt.isoformat(),
            })
        return jobs

    async def _send_reminder(self, job: dict):
        if not self.app_ref:
            return
        chats  = await self.db.all_subscribed_chats()
        air_dt = datetime.fromisoformat(job["air_time"]).astimezone(TZ)
        text   = (
            f"\U0001f514 <b>Episode Alert!</b>\n\n"
            f"\U0001f3ac <b>{job['anime_title']}</b>\n"
            f"\U0001f4cc Type: {job['version'].capitalize()}\n"
            f"\U0001f4fa Episode: {job['episode']}/{job['total_eps']}\n"
            f"\U0001f550 Time: {fmt_time(air_dt)}"
        )
        for cid in chats:
            mode = await self.db.get_mode(cid)
            if mode != "all" and mode != job["version"]:
                continue
            try:
                await self.app_ref.send_message(cid, text, parse_mode=enums.ParseMode.HTML)
            except Exception as e:
                logger.warning("Reminder to %s failed: %s", cid, e)


# ── Bot ───────────────────────────────────────────────────────────────────────
class AnimeBot:
    def __init__(self):
        self.db     = Database(MONGO_URI)
        self.as_api = AnimeScheduleAPI(self.db)
        self.al_api = AniListAPI(self.db)
        self.sched  = ReminderScheduler(self.db, self.as_api)
        self.app: Optional[Client] = None
        self._health = None

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

    async def _is_bot_admin(self, msg: Message) -> bool:
        if msg.from_user and msg.from_user.id in ADMIN_IDS:
            return True
        await msg.reply("\u26d4 Bot admin only.")
        return False

    # ── Static keyboards ──────────────────────────────────────────────────
    @staticmethod
    def _kb_main() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f4c5 View Schedule",      callback_data="menu:schedule")],
            [InlineKeyboardButton("\U0001f3a8 Filter Settings",    callback_data="menu:filter"),
             InlineKeyboardButton("\U0001f39b Mode Settings",      callback_data="menu:mode")],
            [InlineKeyboardButton("\U0001f514 Reminder Settings",  callback_data="menu:reminders")],
            [InlineKeyboardButton("\U0001f4ca Weekly Overview",    callback_data="menu:weekly")],
            [InlineKeyboardButton("\u274c Close",                  callback_data="menu:close")],
        ])

    @staticmethod
    def _kb_days() -> InlineKeyboardMarkup:
        rows = []
        for i in range(0, len(DAYS_OF_WEEK), 2):
            rows.append([InlineKeyboardButton(d, callback_data=f"day:{d}")
                         for d in DAYS_OF_WEEK[i:i+2]])
        rows.append([InlineKeyboardButton("\u25c0 Back", callback_data="menu:back")])
        return InlineKeyboardMarkup(rows)

    @staticmethod
    def _kb_reminders() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("\u2705 Subscribe",               callback_data="rem:on")],
            [InlineKeyboardButton("\u274c Unsubscribe",             callback_data="rem:off")],
            [InlineKeyboardButton("\U0001f4cb Status",              callback_data="rem:status")],
            [InlineKeyboardButton("\u25c0 Back",                    callback_data="menu:back")],
        ])

    @staticmethod
    def _kb_mode() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f534 Raw Only",     callback_data="mode:raw")],
            [InlineKeyboardButton("\U0001f535 Sub Only",     callback_data="mode:sub")],
            [InlineKeyboardButton("\U0001f7e2 Dub Only",     callback_data="mode:dub")],
            [InlineKeyboardButton("\U0001f31f All Versions", callback_data="mode:all")],
            [InlineKeyboardButton("\u25c0 Back",             callback_data="menu:back")],
        ])

    # ── Schedule helper ───────────────────────────────────────────────────
    async def _get_schedule(self, chat_id: int) -> dict:
        y, w = current_week_year()
        entries = await self.as_api.get_timetable(y, w)
        f = await self.db.get_filters(chat_id)
        return ScheduleProcessor.process(entries, f)

    # ── /start ────────────────────────────────────────────────────────────
    async def cmd_start(self, _, msg: Message):
        if not await self._auth(msg):
            return
        name = msg.from_user.first_name if msg.from_user else "Shinobi"
        text = (
            f"\U0001f343 <b>Naruto Timekeeper</b> \U0001f343\n\n"
            f"Yo, <b>{name}</b>!\n\n"
            "I track every anime episode that airs \u2014 raw, sub, and dub \u2014 "
            "so you never miss a moment.\n\n"
            "\U0001f4dc <b>What I can do:</b>\n"
            "  \u2022 Weekly anime schedule by day\n"
            "  \u2022 Episode reminders the moment they air\n"
            "  \u2022 Filter by air type, hide rules, sort order\n"
            "  \u2022 Search any anime or manga via AniList\n\n"
            "Hit the button below to get started \u2193"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f4c5 View Schedule",  callback_data="menu:schedule")],
            [InlineKeyboardButton("\U0001f3a8 Filters",        callback_data="menu:filter"),
             InlineKeyboardButton("\U0001f39b Mode",           callback_data="menu:mode")],
            [InlineKeyboardButton("\U0001f514 Reminders",      callback_data="menu:reminders")],
            [InlineKeyboardButton("\U0001f4d6 Help",           callback_data="menu:help")],
        ])
        if BOT_IMAGE:
            try:
                await msg.reply_photo(photo=BOT_IMAGE, caption=text,
                                      reply_markup=kb, parse_mode=enums.ParseMode.HTML)
                return
            except Exception:
                pass
        await msg.reply(text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    # ── /help ─────────────────────────────────────────────────────────────
    async def cmd_help(self, _, msg: Message):
        if not await self._auth(msg):
            return
        text = (
            "\U0001f343 <b>Naruto Timekeeper \u2014 Help</b>\n\n"
            "<b>\U0001f4cc User Commands</b>\n"
            "/start \u2014 Welcome screen with quick buttons\n"
            "/settings \u2014 Open the full settings panel\n"
            "/filter \u2014 Edit all schedule display filters\n"
            "/mode [raw|sub|dub|all] \u2014 Set version mode\n"
            "/anime &lt;name&gt; \u2014 Search anime on AniList\n"
            "/manga &lt;name&gt; \u2014 Search manga on AniList\n"
            "/help \u2014 Show this message\n\n"
            "<b>\U0001f6e1 Group Admin Commands</b>\n"
            "/auth \u2014 Authorize group to use this bot\n"
            "/deauth \u2014 Remove group authorization\n\n"
            "<b>\U0001f527 Bot Admin Commands</b>\n"
            "/reload \u2014 Force schedule refresh\n"
            "/stats \u2014 View bot usage statistics\n"
            "/broadcast &lt;msg&gt; \u2014 Send to all subscribed chats\n\n"
            "<b>\U0001f3a8 Filters (/filter)</b>\n"
            "  \u2022 Air Type \u2014 Include Raw / Sub / Dub\n"
            "  \u2022 Hide Raw When \u2014 Sub or Dub exists\n"
            "  \u2022 Hide Sub When \u2014 Dub or Raw exists\n"
            "  \u2022 Hide Dub When \u2014 Sub or Raw exists\n"
            "  \u2022 Time Format \u2014 12H or 24H\n"
            "  \u2022 Images \u2014 Show/Hide thumbnails\n"
            "  \u2022 Donghua \u2014 Show/Hide Chinese anime\n"
            "  \u2022 Sort By \u2014 A-Z / Time / Popularity / Unaired\n\n"
            f"\U0001f550 All times in <b>{TZ_NAME}</b>"
        )
        if BOT_IMAGE:
            try:
                await msg.reply_photo(photo=BOT_IMAGE, caption=text, parse_mode=enums.ParseMode.HTML)
                return
            except Exception:
                pass
        await msg.reply(text, parse_mode=enums.ParseMode.HTML)

    # ── /settings ────────────────────────────────────────────────────────
    async def cmd_settings(self, _, msg: Message):
        if not await self._auth(msg):
            return
        text = (
            "\U0001f343 <b>Naruto Timekeeper</b>\n\n"
            "Choose an option from the panel below:"
        )
        await msg.reply(text, reply_markup=self._kb_main(), parse_mode=enums.ParseMode.HTML)

    # ── /filter ───────────────────────────────────────────────────────────
    async def cmd_filter(self, _, msg: Message):
        if not await self._auth(msg):
            return
        f = await self.db.get_filters(msg.chat.id)
        await msg.reply(filter_text(f), reply_markup=build_filter_kb(f),
                        parse_mode=enums.ParseMode.HTML)

    # ── /mode ─────────────────────────────────────────────────────────────
    async def cmd_mode(self, _, msg: Message):
        if not await self._auth(msg):
            return
        arg = " ".join(msg.command[1:]).strip().lower()
        valid = {"raw": "Raw", "sub": "Sub", "dub": "Dub", "all": "All"}
        if arg not in valid:
            await msg.reply(
                "<b>/mode usage:</b>\n"
                "/mode raw \u2014 Raw broadcasts only\n"
                "/mode sub \u2014 Subtitled only\n"
                "/mode dub \u2014 Dubbed only\n"
                "/mode all \u2014 All versions",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        await self.db.set_mode(msg.chat.id, arg)
        await msg.reply(f"Mode set to <b>{valid[arg]}</b>", parse_mode=enums.ParseMode.HTML)

    # ── /auth /deauth ─────────────────────────────────────────────────────
    async def cmd_auth(self, _, msg: Message):
        if msg.chat.type == ChatType.PRIVATE:
            await msg.reply("This command is for groups only.")
            return
        if not await self._is_group_admin(msg):
            await msg.reply("\u26d4 Only group admins can run this.")
            return
        await self.db.authorize_group(msg.chat.id, msg.from_user.id)
        await msg.reply(
            "\u2705 <b>Group authorized!</b>\nThis group can now use Naruto Timekeeper.",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_deauth(self, _, msg: Message):
        if msg.chat.type == ChatType.PRIVATE:
            await msg.reply("This command is for groups only.")
            return
        if not await self._is_group_admin(msg):
            await msg.reply("\u26d4 Only group admins can run this.")
            return
        await self.db.deauthorize_group(msg.chat.id)
        await msg.reply("\u274c Group deauthorized. Bot will no longer respond here.")

    # ── /anime ────────────────────────────────────────────────────────────
    async def cmd_anime(self, _, msg: Message):
        if not await self._auth(msg):
            return
        q = " ".join(msg.command[1:]).strip()
        if not q:
            await msg.reply("Usage: /anime &lt;name&gt;", parse_mode=enums.ParseMode.HTML)
            return
        await msg.reply(f"Searching anime: <i>{q}</i>\u2026", parse_mode=enums.ParseMode.HTML)
        r = await self.al_api.search_anime(q)
        if not r:
            await msg.reply("\u274c No anime found.")
            return
        t     = r.get("title", {})
        title = t.get("english") or t.get("romaji") or "Unknown"
        nat   = t.get("native") or ""
        sd    = r.get("startDate") or {}
        start = f"{sd['year']}-{sd.get('month',0):02}-{sd.get('day',0):02}" if sd.get("year") else "?"
        cap   = (
            f"<b>{title}</b>" + (f" (<i>{nat}</i>)" if nat else "") + "\n\n"
            f"\U0001f4fa Episodes: {r.get('episodes') or '?'}\n"
            f"\U0001f4e1 Status: {al_status(r.get('status',''))}\n"
            f"\u2b50 Score: {r.get('averageScore') or 'N/A'}/100\n"
            f"\U0001f3ad Genres: {', '.join(r.get('genres') or [])}\n"
            f"\U0001f3a8 Studio: {', '.join(n['name'] for n in (r.get('studios') or {}).get('nodes', [])) or '?'}\n"
            f"\U0001f4c5 Started: {start}\n\n"
            f"{clean_html(r.get('description') or 'No description.')}\n\n"
            f"<a href=\"{r.get('siteUrl','')}\">View on AniList</a>"
        )
        thumb = (r.get("coverImage") or {}).get("large")
        if thumb:
            try:
                await msg.reply_photo(photo=thumb, caption=cap, parse_mode=enums.ParseMode.HTML)
                return
            except Exception:
                pass
        await msg.reply(cap, parse_mode=enums.ParseMode.HTML)

    # ── /manga ────────────────────────────────────────────────────────────
    async def cmd_manga(self, _, msg: Message):
        if not await self._auth(msg):
            return
        q = " ".join(msg.command[1:]).strip()
        if not q:
            await msg.reply("Usage: /manga &lt;name&gt;", parse_mode=enums.ParseMode.HTML)
            return
        await msg.reply(f"Searching manga: <i>{q}</i>\u2026", parse_mode=enums.ParseMode.HTML)
        r = await self.al_api.search_manga(q)
        if not r:
            await msg.reply("\u274c No manga found.")
            return
        t      = r.get("title", {})
        title  = t.get("english") or t.get("romaji") or "Unknown"
        nat    = t.get("native") or ""
        sd     = r.get("startDate") or {}
        start  = f"{sd['year']}-{sd.get('month',0):02}-{sd.get('day',0):02}" if sd.get("year") else "?"
        staff  = ", ".join(n["name"]["full"]
                           for n in (r.get("staff") or {}).get("nodes", [])[:3])
        cap    = (
            f"<b>{title}</b>" + (f" (<i>{nat}</i>)" if nat else "") + "\n\n"
            f"\U0001f4d6 Chapters: {r.get('chapters') or '?'}\n"
            f"\U0001f4da Volumes: {r.get('volumes') or '?'}\n"
            f"\U0001f4e1 Status: {al_status(r.get('status',''))}\n"
            f"\u2b50 Score: {r.get('averageScore') or 'N/A'}/100\n"
            f"\U0001f3ad Genres: {', '.join(r.get('genres') or [])}\n"
            f"\u270d Author: {staff or '?'}\n"
            f"\U0001f4c5 Started: {start}\n\n"
            f"{clean_html(r.get('description') or 'No description.')}\n\n"
            f"<a href=\"{r.get('siteUrl','')}\">View on AniList</a>"
        )
        thumb = (r.get("coverImage") or {}).get("large")
        if thumb:
            try:
                await msg.reply_photo(photo=thumb, caption=cap, parse_mode=enums.ParseMode.HTML)
                return
            except Exception:
                pass
        await msg.reply(cap, parse_mode=enums.ParseMode.HTML)

    # ── Admin commands ────────────────────────────────────────────────────
    async def cmd_reload(self, _, msg: Message):
        if not await self._is_bot_admin(msg):
            return
        await msg.reply("\U0001f504 Reloading schedule\u2026")
        await self.db.cache.drop()
        await self.sched.build_all_jobs()
        await msg.reply("\u2705 Done.")

    async def cmd_stats(self, _, msg: Message):
        if not await self._is_bot_admin(msg):
            return
        s  = await self.db.get_stats()
        await msg.reply(
            "<b>\U0001f4ca Bot Stats</b>\n\n"
            f"AS API Calls:   {s.get('as_api_calls', 0)}\n"
            f"AL API Calls:   {s.get('al_api_calls', 0)}\n"
            f"Cache Hits:     {s.get('cache_hits', 0)}\n"
            f"Sched Jobs:     {len(self.sched.scheduler.get_jobs())}\n"
            f"Auth Groups:    {await self.db.count_auth()}\n"
            f"Subscribed:     {await self.db.count('chats')}\n"
            f"Job Docs:       {await self.db.count('jobs')}\n"
            f"Cache Docs:     {await self.db.count('cache')}",
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

    # ── Callback handler ──────────────────────────────────────────────────
    @staticmethod
    async def safe_edit(query, text: str, reply_markup=None, parse_mode=None):
        """Edit message text with keyboard."""
        await query.edit_message_text(
            text, reply_markup=reply_markup, parse_mode=parse_mode
        )

    async def handle_callback(self, _, query: CallbackQuery):
        data    = query.data
        chat_id = query.message.chat.id
        ctype   = query.message.chat.type

        # Auth check
        if not await self.db.is_authorized(chat_id, ctype):
            await query.answer("\U0001f512 Group not authorized.", show_alert=True)
            return

        # ── Filter callbacks ──────────────────────────────────────────────
        if data.startswith("fltr:"):
            parts = data.split(":")
            action = parts[1]

            # Determine current page from message text
            cur_page = 2 if "Page 2/2" in (query.message.text or "") else 1

            if action == "_":
                await query.answer()
                return

            elif action == "pg":  # page navigation
                pg = int(parts[2])
                f = await self.db.get_filters(chat_id)
                await self.safe_edit(query,
                    filter_text(f, pg), reply_markup=build_filter_kb(f, pg),
                    parse_mode=enums.ParseMode.HTML,
                )
                await query.answer()
                return

            elif action == "reset":
                await self.db.reset_filters(chat_id)
                f = await self.db.get_filters(chat_id)
                await self.safe_edit(query,
                    filter_text(f, cur_page), reply_markup=build_filter_kb(f, cur_page),
                    parse_mode=enums.ParseMode.HTML,
                )
                await query.answer("\u2705 Filters reset to defaults")
                return

            elif action == "close":
                await query.message.delete()
                return

            elif action == "t":  # toggle boolean
                key = parts[2]
                f   = await self.db.get_filters(chat_id)
                await self.db.set_filter(chat_id, key, not f.get(key, DEFAULT_FILTERS.get(key)))
                f = await self.db.get_filters(chat_id)
                await self.safe_edit(query,
                    filter_text(f, cur_page), reply_markup=build_filter_kb(f, cur_page),
                    parse_mode=enums.ParseMode.HTML,
                )
                await query.answer()
                return

            elif action == "s":  # set value
                key   = parts[2]
                value = parts[3]
                if value == "true":
                    value = True
                elif value == "false":
                    value = False
                await self.db.set_filter(chat_id, key, value)
                f = await self.db.get_filters(chat_id)
                await self.safe_edit(query,
                    filter_text(f, cur_page), reply_markup=build_filter_kb(f, cur_page),
                    parse_mode=enums.ParseMode.HTML,
                )
                await query.answer()
                return

        # ── Menu callbacks ────────────────────────────────────────────────
        if data == "menu:back":
            text = "\U0001f343 <b>Naruto Timekeeper</b>\n\nChoose an option:"
            await self.safe_edit(query,text, reply_markup=self._kb_main(),
                                          parse_mode=enums.ParseMode.HTML)

        elif data == "menu:close":
            await query.message.delete()

        elif data == "menu:help":
            text = (
                "\U0001f343 <b>Naruto Timekeeper \u2014 Help</b>\n\n"
                "/start \u2014 Welcome screen\n"
                "/settings \u2014 Full settings panel\n"
                "/filter \u2014 Schedule display filters\n"
                "/mode [raw|sub|dub|all] \u2014 Version mode\n"
                "/anime &lt;name&gt; \u2014 Search anime (AniList)\n"
                "/manga &lt;name&gt; \u2014 Search manga (AniList)\n"
                "/auth \u2014 Authorize group (admin)\n\n"
                f"\U0001f550 Times in <b>{TZ_NAME}</b>"
            )
            await self.safe_edit(query,
                text,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u25c0 Back", callback_data="menu:back")]]
                ),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:schedule":
            await self.safe_edit(query,
                "\U0001f4c5 <b>Select a day to view the schedule:</b>",
                reply_markup=self._kb_days(), parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:filter":
            f = await self.db.get_filters(chat_id)
            await self.safe_edit(query,
                filter_text(f), reply_markup=build_filter_kb(f),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:reminders":
            await self.safe_edit(query,
                "\U0001f514 <b>Reminder Settings</b>\n\nSubscribe to get notified when episodes air:",
                reply_markup=self._kb_reminders(), parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:mode":
            mode = await self.db.get_mode(chat_id)
            await self.safe_edit(query,
                f"\U0001f39b <b>Mode Settings</b>\n\nCurrent: <b>{mode.capitalize()}</b>",
                reply_markup=self._kb_mode(), parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:weekly":
            await query.answer("Loading\u2026")
            schedule = await self._get_schedule(chat_id)
            lines    = ["\U0001f4ca <b>Weekly Anime Overview</b>\n"]
            for d in DAYS_OF_WEEK:
                lines.append(f"  <b>{d}:</b> {len(schedule.get(d, []))} anime")
            await self.safe_edit(query,
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u25c0 Back", callback_data="menu:back")]]
                ),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data.startswith("day:"):
            parts    = data.split(":", 2)
            day      = parts[1]
            pg       = int(parts[2]) if len(parts) > 2 else 0  # 0-indexed
            await query.answer(f"Loading {day}\u2026")
            schedule = await self._get_schedule(chat_id)
            f        = await self.db.get_filters(chat_id)
            pages    = ScheduleProcessor.day_pages(day, schedule.get(day, []), f)
            total_pages = len(pages)
            pg = max(0, min(pg, total_pages - 1))
            text = pages[pg]

            # Build nav keyboard
            nav = []
            if total_pages > 1:
                row = []
                if pg > 0:
                    row.append(InlineKeyboardButton(f"\u25c4 Prev", callback_data=f"day:{day}:{pg-1}"))
                row.append(InlineKeyboardButton(f"Page {pg+1}/{total_pages}", callback_data="fltr:_"))
                if pg < total_pages - 1:
                    row.append(InlineKeyboardButton(f"Next \u25ba", callback_data=f"day:{day}:{pg+1}"))
                nav.append(row)
            nav.append([InlineKeyboardButton("\u25c0 Back", callback_data="menu:schedule")])

            kb = InlineKeyboardMarkup(nav)
            await self.safe_edit(query, text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

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

        elif data.startswith("mode:"):
            mode   = data.split(":", 1)[1]
            labels = {"raw": "Raw Only", "sub": "Sub Only", "dub": "Dub Only", "all": "All Versions"}
            await self.db.set_mode(chat_id, mode)
            await query.answer(f"Mode \u2192 {labels.get(mode, mode)}", show_alert=True)
            await self.safe_edit(query,
                f"\U0001f39b <b>Mode Settings</b>\n\nCurrent: <b>{mode.capitalize()}</b>",
                reply_markup=self._kb_mode(), parse_mode=enums.ParseMode.HTML,
            )

        else:
            await query.answer("Unknown action.")

    # ── Register ──────────────────────────────────────────────────────────
    def _register(self, app: Client):
        app.on_message(filters.command("start"))(self.cmd_start)
        app.on_message(filters.command("help"))(self.cmd_help)
        app.on_message(filters.command("settings"))(self.cmd_settings)
        app.on_message(filters.command("filter"))(self.cmd_filter)
        app.on_message(filters.command("mode"))(self.cmd_mode)
        app.on_message(filters.command("anime"))(self.cmd_anime)
        app.on_message(filters.command("manga"))(self.cmd_manga)
        app.on_message(filters.command("auth"))(self.cmd_auth)
        app.on_message(filters.command("deauth"))(self.cmd_deauth)
        app.on_message(filters.command("reload"))(self.cmd_reload)
        app.on_message(filters.command("stats"))(self.cmd_stats)
        app.on_message(filters.command("broadcast"))(self.cmd_broadcast)
        app.on_callback_query()(self.handle_callback)

    # ── Lifecycle ─────────────────────────────────────────────────────────
    async def run(self):
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

        logger.info("Naruto Timekeeper is running")
        await idle()

        logger.info("Shutting down\u2026")
        self.sched.shutdown()
        await self.as_api.close()
        await self.al_api.close()
        await self.app.stop()
        if self._health:
            await self._health.cleanup()
        self.db.close()
        logger.info("Shutdown complete")


# ── Entry ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(AnimeBot().run())
