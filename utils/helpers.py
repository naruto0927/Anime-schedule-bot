"""
utils/helpers.py — Shared utility functions.

parse_dt           Parse ISO-8601 / RFC-2822 strings → aware datetime
fmt_time           Format datetime for Telegram messages
current_week_year  Return (year, week) ISO tuple in bot timezone
clean_html         Strip unsafe tags, truncate for Telegram captions
al_status          Map AniList status strings → human-readable labels
"""

from __future__ import annotations
import re
from datetime import datetime, timezone
from typing import Optional
from config import TZ

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
