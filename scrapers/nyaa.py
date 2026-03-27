"""
scrapers/nyaa.py — Real-time RSS torrent monitor (1080p).

Sources: varyg1001 · ToonsHub · SubsPlease
Watermark-based dedup stored in MongoDB. First run seeds silently.
Retries on HTTP 429; html.parser fallback if xml parser misbehaves.
"""

from __future__ import annotations
import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional
import httpx
from bs4 import BeautifulSoup
from utils.database import Database

logger = logging.getLogger("NarutoTimekeeper")

class NyaaScraper:
    """
    Real-time RSS monitoring for three 1080p torrent sources:
      1. varyg1001  — https://nyaa.si/user/varyg1001?page=rss&q=1080
      2. ToonsHub   — https://nyaa.si/?page=rss&q=%5BToonsHub%5D+1080
      3. SubsPlease — https://subsplease.org/rss/?t&r=1080

    Uses pubDate watermarking stored in MongoDB to detect genuinely new
    entries.  On first run the watermark is set to NOW so no backlog is
    sent.  Only entries with pubDate strictly after the stored watermark
    are returned as new.
    """

    SOURCES = {
        "varyg1001":  "https://nyaa.si/user/varyg1001?page=rss&q=1080",
        "ToonsHub":   "https://nyaa.si/?page=rss&q=%5BToonsHub%5D+1080",
        "subsplease": "https://subsplease.org/rss/?t&r=1080",
    }
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml,application/xml,text/xml,*/*",
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
        Fetch all RSS sources and return only entries published after the
        stored watermark.

        First-run behaviour: if no watermark exists for a source, seed it
        with the newest pubDate from the current feed and return nothing —
        preventing any backlog from being sent on bot startup or restart.
        """
        new_entries: list = []
        for source, url in self.SOURCES.items():
            entries = await self._fetch_rss(url, source)
            if not entries:
                continue

            watermark = await self._get_watermark(source)
            newest_dt = max(e["pub_dt"] for e in entries)

            if watermark is None:
                # First run — seed watermark silently, send nothing
                await self._set_watermark(source, newest_dt)
                logger.info("Nyaa [%s] seeded watermark at %s", source, newest_dt)
                continue

            fresh = [e for e in entries if e["pub_dt"] > watermark]
            logger.info(
                "Nyaa [%s] %d total / %d new (watermark=%s newest=%s)",
                source, len(entries), len(fresh),
                watermark.strftime("%d %b %H:%M") if watermark else "none",
                newest_dt.strftime("%d %b %H:%M"),
            )
            if fresh:
                # Update watermark to the newest entry we found
                await self._set_watermark(source, newest_dt)
                new_entries.extend(fresh)

        # Sort chronologically so oldest posts first
        new_entries.sort(key=lambda e: e["pub_dt"])
        return new_entries

    async def _fetch_rss(self, url: str, source: str) -> list:
        for attempt in range(2):
            try:
                r = await self.client.get(url)
                if r.status_code == 429:
                    logger.warning("Nyaa RSS [%s] rate-limited (429), retrying after 15s", source)
                    await asyncio.sleep(15)
                    continue
                if r.status_code != 200:
                    logger.warning(
                        "Nyaa RSS [%s] returned %d — body: %s",
                        source, r.status_code, r.text[:300],
                    )
                    return []
                entries = await asyncio.get_event_loop().run_in_executor(
                    None, self._parse_rss, r.text, source
                )
                logger.info("Nyaa RSS [%s] fetched %d entries", source, len(entries))
                return entries
            except Exception as exc:
                logger.warning("Nyaa RSS fetch error [%s] attempt %d: %s", source, attempt + 1, exc)
                if attempt == 0:
                    await asyncio.sleep(5)
        return []

    @staticmethod
    def _parse_rss(xml: str, source: str) -> list:
        """
        Parse a nyaa.si or subsplease.org RSS feed.

        nyaa.si RSS item fields (standard RSS 2.0 + nyaa namespace):
          <title>      — torrent title
          <link>       — view URL  (nyaa.si/view/<id>)
          <pubDate>    — RFC 2822 datetime
          <nyaa:size>  — human-readable size
          <enclosure>  — url attr is the .torrent download link
          <guid>       — unique ID (nyaa view URL)

        subsplease RSS item fields:
          <title>      — torrent title
          <link>       — .torrent download URL
          <pubDate>    — RFC 2822 datetime
          <guid>       — unique ID
        """
        from email.utils import parsedate_to_datetime
        try:
            # Try xml parser first (requires lxml); fall back to html.parser
            # so RSS is parsed correctly even if lxml behaves unexpectedly.
            try:
                soup = BeautifulSoup(xml, "xml")
                # Sanity-check: xml parser sometimes returns empty channel
                if not soup.find("item"):
                    raise ValueError("xml parser found no items — retrying with lxml-xml")
            except Exception:
                soup = BeautifulSoup(xml, "html.parser")
            entries = []
            for item in soup.find_all("item"):
                title_tag = item.find("title")
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)

                # pubDate → aware datetime
                pub_tag = item.find("pubDate")
                if not pub_tag:
                    continue
                try:
                    pub_dt = parsedate_to_datetime(pub_tag.get_text(strip=True))
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    continue

                # Source-specific field extraction
                if source in ("varyg1001", "ToonsHub"):
                    guid_tag = item.find("guid")
                    view_url = guid_tag.get_text(strip=True) if guid_tag else ""
                    # nyaa:size tag (namespace)
                    size_tag = item.find("size") or item.find("nyaa:size")
                    size     = size_tag.get_text(strip=True) if size_tag else "?"
                    # Torrent link from <enclosure> or derive from guid
                    enc = item.find("enclosure")
                    if enc and enc.get("url"):
                        torrent_url = enc["url"]
                    else:
                        m = re.search(r"/view/(\d+)", view_url)
                        torrent_url = f"https://nyaa.si/download/{m.group(1)}.torrent" if m else ""
                    uid = view_url or title
                    # Apply 1080 filter for nyaa sources
                    if "1080" not in title:
                        continue
                    if source == "ToonsHub" and "[ToonsHub]" not in title:
                        continue

                else:  # subsplease
                    link_tag = item.find("link")
                    torrent_url = link_tag.get_text(strip=True) if link_tag else ""
                    guid_tag    = item.find("guid")
                    uid         = guid_tag.get_text(strip=True) if guid_tag else torrent_url
                    view_url    = "https://subsplease.org/"
                    size        = "~1.4 GiB"

                entries.append({
                    "id":          uid,
                    "title":       title,
                    "torrent_url": torrent_url,
                    "view_url":    view_url,
                    "size":        size,
                    "uploader":    source,
                    "pub_dt":      pub_dt,
                })
            return entries
        except Exception as exc:
            logger.error("Nyaa RSS parse error [%s]: %s", source, exc)
            return []

    @staticmethod
    def format_entry(e: dict) -> str:
        """Format a torrent entry for Telegram (HTML)."""
        icons = {
            "varyg1001":  "🔴",
            "ToonsHub":   "🟡",
            "subsplease": "🟢",
        }
        up_icon  = icons.get(e["uploader"], "⚪")
        pub_str  = e["pub_dt"].strftime("%d %b %Y %I:%M %p UTC") if e.get("pub_dt") else ""
        view_line = (
            f"\n🔗 <a href=\"{e['view_url']}\">View on Nyaa</a>"
            if "nyaa.si" in e.get("view_url", "")
            else f"\n🌐 <a href=\"{e['view_url']}\">subsplease.org</a>"
        )
        return (
            f"🎌 <b>{e['title']}</b>\n"
            f"{up_icon} <code>{e['uploader']}</code>"
            + (f" · {pub_str}" if pub_str else "") +
            f"\n📦 Size: {e['size']}"
            f"\n⬇️ <a href=\"{e['torrent_url']}\">Download Torrent</a>"
            f"{view_line}"
        )
