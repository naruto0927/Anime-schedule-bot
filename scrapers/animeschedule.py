"""
scrapers/animeschedule.py — animeschedule.net weekly timetable client.

Fetch strategy (tries in order):
  1. GET /api/v3/timetables  (Bearer token — primary)
  2. __NEXT_DATA__ JSON from page HTML
  3. Inline window.__* JSON scan

Results cached 6 hours. Stale cache served on failure.
Token: animeschedule.net/users/<username>/settings/api
"""

from __future__ import annotations
import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional
import httpx
from bs4 import BeautifulSoup
from config import AS_TOKEN, AS_BASE_URL, AS_CDN_BASE, _AS_NULL_DT
from utils.database import Database
from utils.filters import ChatFilter

logger = logging.getLogger("NarutoTimekeeper")

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
