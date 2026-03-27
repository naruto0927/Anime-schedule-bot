"""
utils/scheduler.py — APScheduler wrapper for episode reminder jobs.

Builds one date-based job per (anime, episode, version) combination.
Jobs fire at the exact air datetime and fan out to all subscribed
chats / configured topics via _send_reminder.
Re-built every 6 hours and on /reload.
"""

from __future__ import annotations
import asyncio
import logging
from datetime import datetime
from typing import Optional, TYPE_CHECKING
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from config import TZ, TZ_NAME
from utils.helpers import fmt_time, current_week_year

if TYPE_CHECKING:
    from utils.database import Database
    from scrapers.animeschedule import AnimeScheduleScraper

logger = logging.getLogger("NarutoTimekeeper")

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
        is_donghua = bool(anime.get("donghua"))
        for v in ("raw", "sub", "dub"):
            dt = anime.get(f"{v}_time")
            if dt is None:
                continue
            # Use per-version episode number; fall back to shared total_eps
            ep       = anime.get(f"{v}_ep") or "?"
            total_ep = anime.get("total_eps") or "?"
            jobs.append({
                "anime_slug":  anime["slug"],
                "anime_title": anime["title"],
                "episode":     ep,
                "total_eps":   total_ep,
                "version":     v,
                "air_time":    dt.isoformat(),
                "donghua":     is_donghua,   # needed for per-chat hide_donghua filter
            })
        return jobs

    async def _send_reminder(self, job: dict):
        if not self.app_ref:
            return
        air_dt  = datetime.fromisoformat(job["air_time"]).astimezone(TZ)
        version = job["version"].lower()
        text    = (
            f"\U0001f514 <b>Episode Alert!</b>\n\n"
            f"\U0001f3ac <b>{job['anime_title']}</b>\n"
            f"\U0001f4cc Type: {version.capitalize()}\n"
            f"\U0001f4fa Episode: {job['episode']}/{job['total_eps']}\n"
            f"\U0001f550 Time: {fmt_time(air_dt)}"
        )
        is_donghua = bool(job.get("donghua"))

        chats_with_filters = await self.db.all_chats_with_rem_topic()
        for cid, topic_id, flt in chats_with_filters:
            # ── Donghua check (applies to every delivery path) ────────────
            if flt.hide_donghua and is_donghua:
                continue

            if topic_id:
                # Topic is set — use ONLY per-topic rem config for air type filtering.
                # The shared /filter air types do NOT apply here so each topic
                # can independently choose raw/sub/dub.
                cfg     = await self.db.get_topic_cfg(cid, topic_id)
                # Merge over full defaults so any missing key (partial save)
                # never silently falls back to "always send".
                _rem_defaults = {"show_raw": True, "show_sub": True, "show_dub": True}
                rem_cfg = {**_rem_defaults, **cfg.get("rem", {})}
                if not rem_cfg[f"show_{version}"]:
                    continue
            else:
                # No topic bound — fall back to chat-level filter
                if not flt.allows(version):
                    continue
            try:
                await self.app_ref.send_message(
                    cid, text,
                    parse_mode=enums.ParseMode.HTML,
                    reply_to_message_id=topic_id,
                )
            except Exception as exc:
                logger.warning("Reminder to %s (topic=%s) failed: %s", cid, topic_id, exc)
