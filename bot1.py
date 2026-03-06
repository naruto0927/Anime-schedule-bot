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
from datetime import datetime, timedelta
from typing import Any, Optional
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
AS_TOKEN  = os.environ.get("ANIMESCHEDULE_TOKEN", "")  # Required — get free token at animeschedule.net/users/<user>/settings/api
PORT      = int(os.environ.get("PORT", "8000"))
BOT_IMAGE = os.environ.get("BOT_IMAGE_URL", "")

ANILIST_API  = "https://graphql.anilist.co"
AS_BASE_URL  = "https://animeschedule.net"
AS_CDN_BASE  = "https://img.animeschedule.net/production/assets/public/img/"
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Null sentinel used by animeschedule.net for absent datetime values
_AS_NULL_DT = "0001-01-01T00:00:00Z"


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
        self.jobs   = self.db["jobs"]
        self.cache  = self.db["cache"]
        self.stats  = self.db["stats"]
        self.auth   = self.db["auth"]

    async def init_indexes(self):
        await self.chats.create_index("chat_id", unique=True)
        await self.jobs.create_index(
            [("anime_slug", 1), ("episode", 1), ("version", 1)], unique=True
        )
        await self.cache.create_index("key", unique=True)
        await self.auth.create_index("chat_id", unique=True)
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
        exp = datetime.utcnow() + timedelta(seconds=ttl)
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
        now = datetime.utcnow()
        if exp.tzinfo is not None:
            exp = exp.replace(tzinfo=None)
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

    def close(self):
        self.client.close()


# ── AnimeSchedule Scraper ─────────────────────────────────────────────────────
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
    async def get_timetable(self, year: int, week: int) -> list:
        """
        Return a list of normalised entry dicts for the given ISO year/week.
        Results are cached for 6 hours.  On scrape failure the stale cache
        (ignoring TTL) is used so the bot never crashes.
        """
        cache_key = f"scrape:timetable:{year}:{week}"

        cached = await self.db.get_cache(cache_key)
        if cached is not None:
            await self.db.inc_stat("cache_hits")
            logger.debug("Cache hit for %s", cache_key)
            return cached

        entries = await self._fetch_timetable(year, week)

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
    async def _fetch_timetable(self, year: int, week: int) -> list:
        entries = await self._fetch_via_api(year, week)
        if entries:
            return entries
        return await self._fetch_via_html(year, week)

    async def _fetch_via_api(self, year: int, week: int) -> list:
        """
        Call GET /api/v3/timetables (authenticated with Bearer token).

        A single call without an airType filter returns ALL entries for all
        three airTypes (raw, sub, dub) for the given week.  Each entry has:
            route, title, airType, episodeDate, episodeNumber, episodes,
            imageVersionRoute, likes
        _coerce_entry() maps episodeDate → the correct slot (raw/sub/dub).
        ScheduleProcessor.process() merges entries sharing the same route.

        Authentication: ANIMESCHEDULE_TOKEN env var must be set.
        Get a free token at: animeschedule.net/users/<username>/settings/api
        """
        if not AS_TOKEN:
            logger.error(
                "ANIMESCHEDULE_TOKEN is not set — cannot fetch schedule data. "
                "Get a free token at animeschedule.net/users/<username>/settings/api"
            )
            return []

        params = {"week": week, "year": year, "tz": "UTC"}
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
        title = (
            pick("title", "Title", "romaji", "Romaji", "english", "English")
            or route
        )
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

        return {
            "route":             slug,
            "title":             title,
            "airType":           air_type,
            "episodeNumber":     pick("episodeNumber", "EpisodeNumber", "episode") or 0,
            "episodes":          pick("episodes", "Episodes", "totalEpisodes")     or 0,
            "episodeDate":       raw_date,
            "subDate":           sub_date,
            "dubDate":           dub_date,
            "imageVersionRoute": image_route,
            "likes":             pick("likes", "Likes", "popularity", "Popularity") or 0,
        }


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
                headers={
                    "Content-Type": "application/json",
                    "Accept":       "application/json",
                },
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _gql(self, query: str, search: str) -> Optional[dict]:
        try:
            r = await self.client.post(
                self.URL,
                json={"query": query, "variables": {"s": search}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            return data.get("data", {}).get("Media") if "errors" not in data else None
        except Exception as exc:
            logger.error("AniList error: %s", exc)
            return None

    async def search_anime(self, q: str):
        return await self._gql(self._ANIME_Q, q)

    async def search_manga(self, q: str):
        return await self._gql(self._MANGA_Q, q)


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


# ── Schedule Processor ────────────────────────────────────────────────────────
class ScheduleProcessor:
    """
    Converts a flat list of AnimeScheduleScraper entry dicts into a
    {DayName: [anime_dict, ...]} mapping ready for display.

    Expected entry keys (from AnimeScheduleScraper._coerce_entry):
        route, title, episodeNumber, episodes,
        episodeDate, subDate, dubDate,
        imageVersionRoute, likes

    Always shows raw + sub + dub together.  No filtering, no mode logic.
    """

    @staticmethod
    def process(entries: list) -> dict:
        """
        Build a week day-map from scraper entries.

        animeschedule.net returns ONE entry per (show, airType) — i.e. the
        same show appears up to three times in the list (raw, sub, dub), each
        with a different EpisodeDate.  We merge entries that share the same
        Route into a single anime block so the display shows all three times
        together.

        Always includes raw, sub, and dub times when available.
        No filtering, no hide rules, no mode logic.
        """
        # key = (route_slug, day_name) → merged anime dict
        merged: dict = {}

        for e in entries:
            slug  = e.get("route") or e.get("slug") or e.get("title", "")
            title = e.get("title") or slug
            if not slug:
                continue

            # Each normalised entry has exactly one non-empty date slot
            raw = parse_dt(e.get("episodeDate") or "")
            sub = parse_dt(e.get("subDate")     or "")
            dub = parse_dt(e.get("dubDate")     or "")

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
                ep_num = 0.0

            key      = (slug, day)
            existing = merged.get(key)

            if existing is None:
                merged[key] = {
                    "slug":       slug,
                    "title":      title,
                    "date":       primary.strftime("%Y-%m-%d"),
                    "raw_time":   raw,
                    "sub_time":   sub,
                    "dub_time":   dub,
                    "episode":    ep,
                    "ep_num":     ep_num,
                    "total_eps":  e.get("episodes") or e.get("totalEpisodes") or "?",
                    "thumbnail":  e.get("imageVersionRoute") or "",
                    "popularity": e.get("likes") or e.get("popularity") or 0,
                }
            else:
                # Merge this airType's time slot into the existing record
                if raw and not existing["raw_time"]:
                    existing["raw_time"] = raw
                if sub and not existing["sub_time"]:
                    existing["sub_time"] = sub
                if dub and not existing["dub_time"]:
                    existing["dub_time"] = dub
                # Keep the highest episode number seen across all airType entries
                if ep_num > existing["ep_num"]:
                    existing["episode"] = ep
                    existing["ep_num"]  = ep_num
                total = e.get("episodes") or e.get("totalEpisodes")
                if total:
                    existing["total_eps"] = total
                # Use earliest available date as the display date
                if primary.strftime("%Y-%m-%d") < existing["date"]:
                    existing["date"] = primary.strftime("%Y-%m-%d")

        day_map: dict = {d: [] for d in DAYS_OF_WEEK}
        for (slug, day), anime in merged.items():
            anime.pop("ep_num", None)
            day_map[day].append(anime)

        for d in day_map:
            day_map[d].sort(key=lambda x: -(x.get("popularity") or 0))

        return day_map

    @staticmethod
    def anime_block(a: dict) -> str:
        """Render a single anime entry showing all available version times."""
        has_raw = a.get("raw_time") is not None
        has_sub = a.get("sub_time") is not None
        has_dub = a.get("dub_time") is not None
        if not (has_raw or has_sub or has_dub):
            return ""
        lines = [
            f"\U0001f3ac <b>{a['title']}</b>",
            f"\U0001f4c5 {a['date']}  \U0001f4fa Ep {a['episode']}/{a['total_eps']}",
        ]
        if has_raw:
            lines.append(f"  \U0001f534 Raw: {fmt_time(a['raw_time'])}")
        if has_sub:
            lines.append(f"  \U0001f535 Sub: {fmt_time(a['sub_time'])}")
        if has_dub:
            lines.append(f"  \U0001f7e2 Dub: {fmt_time(a['dub_time'])}")
        return "\n".join(lines)

    @staticmethod
    def day_pages(day: str, anime_list: list, max_len: int = 3800) -> list:
        """Split a day's schedule into Telegram-safe text pages."""
        if not anime_list:
            return [f"<b>\U0001f4c5 {day}</b>\n\nNo anime scheduled."]

        blocks = [b for b in (ScheduleProcessor.anime_block(a) for a in anime_list) if b]
        if not blocks:
            return [f"<b>\U0001f4c5 {day}</b>\n\nNo anime scheduled."]

        pages: list        = []
        current_blocks: list = []
        current_len: int   = 0
        total: int         = len(blocks)

        for block in blocks:
            if current_blocks and current_len + len(block) + 2 > max_len:
                pages.append(current_blocks)
                current_blocks = [block]
                current_len    = len(block)
            else:
                current_blocks.append(block)
                current_len += len(block) + (2 if len(current_blocks) > 1 else 0)

        if current_blocks:
            pages.append(current_blocks)

        result = []
        for i, page_blocks in enumerate(pages):
            header = f"<b>\U0001f4c5 {day}</b> \u2014 {total} anime"
            if len(pages) > 1:
                header += f"  <i>(Page {i + 1}/{len(pages)})</i>"
            result.append(header + "\n\n" + "\n\n".join(page_blocks))
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
        self.scheduler.remove_all_jobs()

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

        self.scheduler.add_job(
            self.build_all_jobs,
            "interval",
            hours=6,
            id="refresh_schedule",
            replace_existing=True,
        )
        logger.info("Built %d reminder jobs", added)

    def _make_jobs(self, anime: dict) -> list:
        jobs = []
        for v in ("raw", "sub", "dub"):
            dt = anime.get(f"{v}_time")
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
            try:
                await self.app_ref.send_message(cid, text, parse_mode=enums.ParseMode.HTML)
            except Exception as exc:
                logger.warning("Reminder to %s failed: %s", cid, exc)


# ── Bot ───────────────────────────────────────────────────────────────────────
class AnimeBot:
    def __init__(self):
        self.db      = Database(MONGO_URI)
        self.scraper = AnimeScheduleScraper(self.db)
        self.al_api  = AniListAPI(self.db)
        self.sched   = ReminderScheduler(self.db, self.scraper)
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
            [InlineKeyboardButton("\U0001f4c5 View Schedule",     callback_data="menu:schedule")],
            [InlineKeyboardButton("\U0001f514 Reminder Settings", callback_data="menu:reminders")],
            [InlineKeyboardButton("\U0001f4ca Weekly Overview",   callback_data="menu:weekly")],
            [InlineKeyboardButton("\U0001f4d6 Help",              callback_data="menu:help")],
            [InlineKeyboardButton("\u274c Close",                 callback_data="menu:close")],
        ])

    @staticmethod
    def _kb_days() -> InlineKeyboardMarkup:
        rows = []
        for i in range(0, len(DAYS_OF_WEEK), 2):
            rows.append(
                [InlineKeyboardButton(d, callback_data=f"day:{d}")
                 for d in DAYS_OF_WEEK[i : i + 2]]
            )
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
    async def _get_schedule(self) -> dict:
        y, w    = current_week_year()
        entries = await self.scraper.get_timetable(y, w)
        return ScheduleProcessor.process(entries)

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
            "\U0001f343 <b>Naruto Timekeeper \u2014 Help</b>\n\n"
            "<b>\U0001f4cc User Commands</b>\n"
            "/start \u2014 Welcome screen with quick buttons\n"
            "/settings \u2014 Open the full settings panel\n"
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
            f"\U0001f550 All times in <b>{TZ_NAME}</b>"
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
        start = (
            f"{sd['year']}-{int(sd.get('month') or 0):02}-{int(sd.get('day') or 0):02}"
            if sd.get("year") else "?"
        )
        cap = (
            f"<b>{title}</b>" + (f" (<i>{nat}</i>)" if nat else "") + "\n\n"
            f"\U0001f4fa Episodes: {r.get('episodes') or '?'}\n"
            f"\U0001f4e1 Status: {al_status(r.get('status', ''))}\n"
            f"\u2b50 Score: {r.get('averageScore') or 'N/A'}/100\n"
            f"\U0001f3ad Genres: {', '.join(r.get('genres') or [])}\n"
            f"\U0001f3a8 Studio: "
            f"{', '.join(n['name'] for n in (r.get('studios') or {}).get('nodes', [])) or '?'}\n"
            f"\U0001f4c5 Started: {start}\n\n"
            f"{clean_html(r.get('description') or 'No description.')}\n\n"
            f"<a href=\"{r.get('siteUrl', '')}\">View on AniList</a>"
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
        t     = r.get("title", {})
        title = t.get("english") or t.get("romaji") or "Unknown"
        nat   = t.get("native") or ""
        sd    = r.get("startDate") or {}
        start = (
            f"{sd['year']}-{int(sd.get('month') or 0):02}-{int(sd.get('day') or 0):02}"
            if sd.get("year") else "?"
        )
        staff = ", ".join(
            n["name"]["full"]
            for n in (r.get("staff") or {}).get("nodes", [])[:3]
        )
        cap = (
            f"<b>{title}</b>" + (f" (<i>{nat}</i>)" if nat else "") + "\n\n"
            f"\U0001f4d6 Chapters: {r.get('chapters') or '?'}\n"
            f"\U0001f4da Volumes: {r.get('volumes') or '?'}\n"
            f"\U0001f4e1 Status: {al_status(r.get('status', ''))}\n"
            f"\u2b50 Score: {r.get('averageScore') or 'N/A'}/100\n"
            f"\U0001f3ad Genres: {', '.join(r.get('genres') or [])}\n"
            f"\u270d Author: {staff or '?'}\n"
            f"\U0001f4c5 Started: {start}\n\n"
            f"{clean_html(r.get('description') or 'No description.')}\n\n"
            f"<a href=\"{r.get('siteUrl', '')}\">View on AniList</a>"
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
        s = await self.db.get_stats()
        await msg.reply(
            "<b>\U0001f4ca Bot Stats</b>\n\n"
            f"Token Set:        {'Yes' if AS_TOKEN else 'NO — set ANIMESCHEDULE_TOKEN!'}\n"
            f"Scrape Calls:     {s.get('scrape_calls', 0)}\n"
            f"Scrape Fallbacks: {s.get('scrape_fallbacks', 0)}\n"
            f"AL API Calls:     {s.get('al_api_calls', 0)}\n"
            f"Cache Hits:       {s.get('cache_hits', 0)}\n"
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

    # ── Callback handler ──────────────────────────────────────────────────
    @staticmethod
    async def safe_edit(query, text: str, reply_markup=None, parse_mode=None):
        """Edit a message, silently ignoring MESSAGE_NOT_MODIFIED errors."""
        try:
            await query.edit_message_text(
                text, reply_markup=reply_markup, parse_mode=parse_mode
            )
        except Exception as exc:
            # Telegram raises 400 MESSAGE_NOT_MODIFIED when content is unchanged.
            # This is harmless — the message already shows the correct content.
            err = str(exc)
            if "MESSAGE_NOT_MODIFIED" in err or "message is not modified" in err.lower():
                return
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
            await self.safe_edit(
                query,
                "\U0001f4c5 <b>Select a day to view the schedule:</b>",
                reply_markup=self._kb_days(),
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

        elif data == "menu:weekly":
            await query.answer("Loading\u2026")
            schedule = await self._get_schedule()
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
            parts = data.split(":", 2)
            day   = parts[1]
            pg    = int(parts[2]) if len(parts) > 2 else 0

            await query.answer(f"Loading {day}\u2026")
            schedule    = await self._get_schedule()
            pages       = ScheduleProcessor.day_pages(day, schedule.get(day, []))
            total_pages = len(pages)
            pg          = max(0, min(pg, total_pages - 1))
            text        = pages[pg]

            nav: list = []
            if total_pages > 1:
                row: list = []
                if pg > 0:
                    row.append(
                        InlineKeyboardButton(
                            "\u25c4 Prev", callback_data=f"day:{day}:{pg - 1}"
                        )
                    )
                row.append(
                    InlineKeyboardButton(
                        f"Page {pg + 1}/{total_pages}", callback_data="noop"
                    )
                )
                if pg < total_pages - 1:
                    row.append(
                        InlineKeyboardButton(
                            "Next \u25ba", callback_data=f"day:{day}:{pg + 1}"
                        )
                    )
                nav.append(row)
            nav.append(
                [InlineKeyboardButton("\u25c0 Back", callback_data="menu:schedule")]
            )

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

        elif data == "noop":
            await query.answer()

        else:
            await query.answer("Unknown action.")

    # ── Register ──────────────────────────────────────────────────────────
    def _register(self, app: Client):
        app.on_message(filters.command("start"))(self.cmd_start)
        app.on_message(filters.command("help"))(self.cmd_help)
        app.on_message(filters.command("settings"))(self.cmd_settings)
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

        logger.info("Naruto Timekeeper is running")
        await idle()

        logger.info("Shutting down\u2026")
        self.sched.shutdown()
        await self.scraper.close()
        await self.al_api.close()
        await self.app.stop()
        if self._health:
            await self._health.cleanup()
        self.db.close()
        logger.info("Shutdown complete")


# ── Entry ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(AnimeBot().run())
