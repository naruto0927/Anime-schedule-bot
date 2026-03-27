"""
scrapers/season.py — MyAnimeList seasonal scraper + AniList enrichment.

Scrapes TV-New section of the MAL seasonal page; enriches each title
with the English name from AniList. Cached 24 hours in MongoDB.
"""

from __future__ import annotations
import asyncio
import logging
import re
from typing import Optional
import httpx
from bs4 import BeautifulSoup
from utils.database import Database

logger = logging.getLogger("NarutoTimekeeper")

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
