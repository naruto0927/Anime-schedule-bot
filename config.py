"""
config.py — Central configuration for Naruto Timekeeper.
All environment variables, logging setup, and shared constants live here.
Every other module imports from config rather than touching os.environ directly.
"""

import logging
import os
from zoneinfo import ZoneInfo

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("NarutoTimekeeper")
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# ── Telegram / Mongo ──────────────────────────────────────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID    = int(os.environ["API_ID"])
API_HASH  = os.environ["API_HASH"]
MONGO_URI = os.environ["MONGO_URI"]
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]

# ── Bot settings ──────────────────────────────────────────────────────────────
TZ_NAME   = os.environ.get("TIMEZONE", "Asia/Kolkata")
TZ        = ZoneInfo(TZ_NAME)
PORT      = int(os.environ.get("PORT", "8000"))
BOT_IMAGE = os.environ.get("BOT_IMAGE_URL", "")

# ── AnimeSchedule.net ─────────────────────────────────────────────────────────
# Free API token: animeschedule.net/users/<username>/settings/api
AS_TOKEN    = os.environ.get("ANIMESCHEDULE_TOKEN", "")
AS_BASE_URL = "https://animeschedule.net"
AS_CDN_BASE = "https://img.animeschedule.net/production/assets/public/img/"
_AS_NULL_DT = "0001-01-01T00:00:00Z"   # sentinel for absent datetime fields

# ── AniList ───────────────────────────────────────────────────────────────────
ANILIST_API = "https://graphql.anilist.co"

# ── Nyaa.si ───────────────────────────────────────────────────────────────────
NYAA_BASE_URL      = "https://nyaa.si"
NYAA_POLL_INTERVAL = int(os.environ.get("NYAA_POLL_INTERVAL", "300"))  # seconds

# ── Schedule constants ────────────────────────────────────────────────────────
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

MEDIA_TYPES: dict = {
    "tv": "TV", "tv-short": "TV Short", "ona": "ONA",
    "ova": "OVA", "special": "Special", "movie": "Movie",
}

STREAMS: dict = {
    "crunchyroll": "Crunchyroll", "netflix": "Netflix",
    "hidive": "HiDive",          "amazon": "Amazon",
    "hulu": "Hulu",              "youtube": "YouTube",
    "disney+": "Disney+",        "bilibili": "BiliBili",
    "appletv": "Apple TV",       "oceanveil": "OceanVeil",
}
