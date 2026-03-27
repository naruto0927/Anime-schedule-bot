"""
scrapers/anilist.py — AniList GraphQL API client.

search_anime_list / search_manga_list  — up to 8 matches for a query
get_anime_by_id / get_manga_by_id      — full detail card by AniList ID

Results cached 6 hours (detail) / 30 min (search lists) in MongoDB.
"""

from __future__ import annotations
import asyncio
import logging
from typing import Optional
import httpx
from config import ANILIST_API
from utils.database import Database

logger = logging.getLogger("NarutoTimekeeper")

class AniListAPI:
    URL = ANILIST_API

    # Single best-match query (used for detailed card)
    _ANIME_Q = """query($s:String){Media(search:$s,type:ANIME,sort:SEARCH_MATCH){
        id title{romaji english native} description(asHtml:false) episodes status
        averageScore genres coverImage{large} siteUrl startDate{year month day}
        studios(isMain:true){nodes{name}}}}"""
    _MANGA_Q = """query($s:String){Media(search:$s,type:MANGA,sort:SEARCH_MATCH){
        id title{romaji english native} description(asHtml:false) chapters volumes status
        averageScore genres coverImage{large} siteUrl startDate{year month day}
        staff(sort:RELEVANCE){nodes{name{full}}}}}"""

    # Multi-result search — returns up to 8 matches with id, title, format, status
    _ANIME_SEARCH_Q = """query($s:String){Page(perPage:8){
        media(search:$s,type:ANIME,sort:SEARCH_MATCH){
            id title{romaji english} format status episodes
            coverImage{medium} siteUrl startDate{year}}}}"""
    _MANGA_SEARCH_Q = """query($s:String){Page(perPage:8){
        media(search:$s,type:MANGA,sort:SEARCH_MATCH){
            id title{romaji english} format status chapters volumes
            coverImage{medium} siteUrl startDate{year}}}}"""

    # Detail by ID (for when user picks from inline list)
    _ANIME_BY_ID = """query($id:Int){Media(id:$id,type:ANIME){
        id title{romaji english native} description(asHtml:false) episodes status
        averageScore genres coverImage{large} siteUrl startDate{year month day}
        studios(isMain:true){nodes{name}}}}"""
    _MANGA_BY_ID = """query($id:Int){Media(id:$id,type:MANGA){
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

    async def search_anime_list(self, q: str) -> list:
        """Return up to 8 search results for inline buttons."""
        try:
            r = await self.client.post(
                self.URL,
                json={"query": self._ANIME_SEARCH_Q, "variables": {"s": q}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            return (data.get("data", {}).get("Page", {}).get("media") or []) if "errors" not in data else []
        except Exception as exc:
            logger.error("AniList search list error: %s", exc)
            return []

    async def search_manga_list(self, q: str) -> list:
        """Return up to 8 manga search results for inline buttons."""
        try:
            r = await self.client.post(
                self.URL,
                json={"query": self._MANGA_SEARCH_Q, "variables": {"s": q}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            return (data.get("data", {}).get("Page", {}).get("media") or []) if "errors" not in data else []
        except Exception as exc:
            logger.error("AniList manga search list error: %s", exc)
            return []

    async def get_anime_by_id(self, media_id: int) -> Optional[dict]:
        """Fetch full anime detail by AniList media ID."""
        try:
            r = await self.client.post(
                self.URL,
                json={"query": self._ANIME_BY_ID, "variables": {"id": media_id}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            return data.get("data", {}).get("Media") if "errors" not in data else None
        except Exception as exc:
            logger.error("AniList get_anime_by_id error: %s", exc)
            return None

    async def get_manga_by_id(self, media_id: int) -> Optional[dict]:
        """Fetch full manga detail by AniList media ID."""
        try:
            r = await self.client.post(
                self.URL,
                json={"query": self._MANGA_BY_ID, "variables": {"id": media_id}},
            )
            r.raise_for_status()
            await self.db.inc_stat("al_api_calls")
            data = r.json()
            return data.get("data", {}).get("Media") if "errors" not in data else None
        except Exception as exc:
            logger.error("AniList get_manga_by_id error: %s", exc)
            return None
