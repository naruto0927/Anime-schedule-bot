"""
scrapers/schedule_processor.py — Timetable merger and Telegram page renderer.

Merge rule
----------
Raw + Sub for the same show on the same day → one combined block.
Dub always gets its own day-slot.

Donghua filtering applied in process() (data) AND day_pages() (display).
"""

from __future__ import annotations
from datetime import datetime
from typing import Optional
from config import DAYS_OF_WEEK
from utils.filters import ChatFilter
from utils.helpers import fmt_time, parse_dt

class ScheduleProcessor:
    """
    Converts a flat list of AnimeScheduleScraper entry dicts into a
    {DayName: [anime_dict, ...]} mapping ready for display.

    KEY DESIGN DECISION — per-version independence:
    ───────────────────────────────────────────────
    animeschedule.net returns one entry per (show, airType).  Raw, Sub, and
    Dub are often on DIFFERENT days with DIFFERENT episode numbers.
    e.g. Sub ep 8 airs Wednesday, Dub ep 6 airs Saturday.

    Each (slug, airType) pair is placed on its OWN day independently.
    When the same show has Raw and Sub on the SAME day, those two ARE merged
    into one block (key = slug+day, only same-day same-show entries combine).
    Dub on a different day appears as a separate entry on that day, showing
    only the Dub line with its own episode number and date.

    This means a show can appear on multiple days — once for Raw/Sub and
    again (later in the week) for Dub — which is correct and expected.
    """

    @staticmethod
    def process(entries: list, flt: Optional["ChatFilter"] = None) -> dict:
        """
        Build a {DayName: [anime_dict, ...]} map.

        flt — optional ChatFilter; if provided, entries for disabled air types
              are skipped entirely so they don't appear in the schedule or
              reminder jobs for this chat.

        Merge rule: entries are grouped by (slug, day, airType-group).
        Raw and Sub that fall on the same day are merged into one block.
        Dub is always kept separate per its own air day.
        """
        merged: dict = {}

        for e in entries:
            slug  = e.get("route") or e.get("slug") or e.get("title", "")
            title = e.get("title") or slug
            if not slug:
                continue

            air_type = e.get("airType", "raw").lower()

            # ── Apply chat filter: air type ──────────────────────────────
            if flt is not None and not flt.allows_air_type(air_type):
                continue

            # ── Apply chat filter: hide donghua ───────────────────────────
            is_donghua = bool(e.get("donghua"))
            if flt is not None and flt.hide_donghua and is_donghua:
                continue

            # Determine the datetime for this specific version
            ep_date_str = (
                e.get("episodeDate") or
                e.get("subDate")     or
                e.get("dubDate")     or ""
            )
            dt = parse_dt(ep_date_str)
            if not dt:
                continue

            day = dt.strftime("%A")
            if day not in DAYS_OF_WEEK:
                continue

            ep = e.get("episodeNumber") or e.get("episode") or 0
            try:
                ep_num = float(ep)
            except (TypeError, ValueError):
                ep_num = 0.0

            total_eps  = e.get("episodes") or e.get("totalEpisodes") or "?"
            popularity = e.get("likes") or e.get("popularity") or 0
            thumbnail  = e.get("imageVersionRoute") or ""

            # Dub always gets its own day-slot; raw and sub share a slot per day
            version_group = "dub" if air_type == "dub" else "raw_sub"
            key = (slug, day, version_group)

            existing = merged.get(key)
            if existing is None:
                merged[key] = {
                    "slug":        slug,
                    "title":       title,
                    "popularity":  popularity,
                    "thumbnail":   thumbnail,
                    # Raw version slot
                    "raw_time":    dt    if air_type == "raw" else None,
                    "raw_ep":      ep    if air_type == "raw" else None,
                    "raw_date":    day   if air_type == "raw" else None,
                    # Sub version slot
                    "sub_time":    dt    if air_type == "sub" else None,
                    "sub_ep":      ep    if air_type == "sub" else None,
                    "sub_date":    day   if air_type == "sub" else None,
                    # Dub version slot
                    "dub_time":    dt    if air_type == "dub" else None,
                    "dub_ep":      ep    if air_type == "dub" else None,
                    "dub_date":    day   if air_type == "dub" else None,
                    # Shared display fields — taken from whichever version is present
                    "total_eps":   total_eps,
                    "ep_num":      ep_num,   # for dedup comparisons only
                }
            else:
                # Same show, same day, same version_group — fill in any missing slot
                if air_type == "raw" and existing["raw_time"] is None:
                    existing["raw_time"] = dt
                    existing["raw_ep"]   = ep
                    existing["raw_date"] = day
                elif air_type == "sub" and existing["sub_time"] is None:
                    existing["sub_time"] = dt
                    existing["sub_ep"]   = ep
                    existing["sub_date"] = day
                elif air_type == "dub" and existing["dub_time"] is None:
                    existing["dub_time"] = dt
                    existing["dub_ep"]   = ep
                    existing["dub_date"] = day
                if ep_num > existing["ep_num"]:
                    existing["ep_num"] = ep_num
                if total_eps and total_eps != "?":
                    existing["total_eps"] = total_eps

        day_map: dict = {d: [] for d in DAYS_OF_WEEK}
        for (slug, day, _vgroup), anime in merged.items():
            anime.pop("ep_num", None)
            day_map[day].append(anime)

        for d in day_map:
            day_map[d].sort(key=lambda x: -(x.get("popularity") or 0))

        return day_map

    @staticmethod
    def anime_block_rawsub(a: dict) -> str:
        """Render a Raw/Sub entry block (shown in the Raw & Sub section)."""
        has_raw = a.get("raw_time") is not None
        has_sub = a.get("sub_time") is not None
        if not (has_raw or has_sub):
            return ""
        lines = [f"🎬 <b>{a['title']}</b>"]
        if has_raw:
            raw_ep = a.get("raw_ep")
            ep_str = f" Ep {raw_ep}" if raw_ep else ""
            lines.append(f"  🔴 Raw{ep_str}: {fmt_time(a['raw_time'])}")
        if has_sub:
            sub_ep = a.get("sub_ep")
            ep_str = f" Ep {sub_ep}" if sub_ep else ""
            lines.append(f"  🔵 Sub{ep_str}: {fmt_time(a['sub_time'])}")
        total = a.get("total_eps", "?")
        if total and total != "?":
            lines.append(f"  📺 {total} eps total")
        return "\n".join(lines)

    @staticmethod
    def anime_block_dub(a: dict) -> str:
        """Render a Dub-only entry block (shown in the Dub section)."""
        if a.get("dub_time") is None:
            return ""
        dub_ep = a.get("dub_ep")
        ep_str = f" Ep {dub_ep}" if dub_ep else ""
        total  = a.get("total_eps", "?")
        lines  = [f"🎬 <b>{a['title']}</b>"]
        lines.append(f"  🟢 Dub{ep_str}: {fmt_time(a['dub_time'])}")
        if total and total != "?":
            lines.append(f"  📺 {total} eps total")
        return "\n".join(lines)

    @staticmethod
    def day_pages(day: str, anime_list: list, per_page: int = 15,
                  flt: Optional["ChatFilter"] = None,
                  year: Optional[int] = None, week: Optional[int] = None) -> list:
        """
        Build paginated schedule for a day.
        Format matches the reference screenshot:
          - Day header as plain bold text with actual date
          - Section headers (Raw & Sub / Dubbed) as plain text with count
          - Each show entry wrapped in its own <blockquote>
          - Divider line between sections
        """
        import datetime as _dt

        # Compute the actual calendar date for this day name
        day_index = DAYS_OF_WEEK.index(day) + 1  # ISO: Mon=1 … Sun=7
        if year and week:
            try:
                day_date = _dt.date.fromisocalendar(year, week, day_index)
                date_str = day_date.strftime("%d %b %Y")   # e.g. "25 Mar 2026"
            except Exception:
                date_str = ""
        else:
            date_str = ""

        header_date = f" {date_str}" if date_str else ""

        if not anime_list:
            return [f"\U0001f4c5 <b>{day}{header_date}</b>\n\nNo anime scheduled."]

        show_rawsub   = flt is None or "raw" in flt.air_types or "sub" in flt.air_types
        show_dub      = flt is None or "dub" in flt.air_types
        hide_donghua  = flt is not None and flt.hide_donghua

        def _visible(a: dict) -> bool:
            """Return False if this anime should be hidden by the current filter."""
            return not (hide_donghua and bool(a.get("donghua")))

        rawsub_entries = []
        if show_rawsub:
            for a in anime_list:
                if not _visible(a):
                    continue
                block = ScheduleProcessor.anime_block_rawsub(a)
                if block:
                    rawsub_entries.append(block)

        dub_entries = []
        if show_dub:
            for a in anime_list:
                if not _visible(a):
                    continue
                block = ScheduleProcessor.anime_block_dub(a)
                if block:
                    dub_entries.append(block)

        if not rawsub_entries and not dub_entries:
            return [f"\U0001f4c5 <b>{day}{header_date}</b>\n\nNo anime scheduled."]

        total_shows = len(rawsub_entries) + len(dub_entries)
        DIVIDER = "\u2500" * 16

        # Build flat item list: ("section_rawsub"), ("section_dub"), ("divider"), ("entry", text)
        items: list = []
        if rawsub_entries:
            items.append(("section_rawsub", len(rawsub_entries)))
            for b in rawsub_entries:
                items.append(("entry", b))
        if dub_entries:
            if rawsub_entries:
                items.append(("divider", DIVIDER))
            items.append(("section_dub", len(dub_entries)))
            for b in dub_entries:
                items.append(("entry", b))

        # Paginate — only "entry" items count toward per_page
        pages: list = []
        current: list = []
        entry_count = 0

        for item in items:
            kind = item[0]
            if kind == "entry":
                if entry_count >= per_page and current:
                    pages.append(current)
                    current = []
                    entry_count = 0
                current.append(item)
                entry_count += 1
            else:
                # Non-entry items: flush page if we hit limit, then attach header to new page
                if entry_count >= per_page and current:
                    pages.append(current)
                    current = []
                    entry_count = 0
                current.append(item)

        if current:
            pages.append(current)

        total_pages = len(pages)
        result = []
        for i, page_items in enumerate(pages):
            day_header = f"\U0001f4c5 <b>{day}{header_date}</b> \u2014 {total_shows} show(s)"
            parts = [day_header, ""]
            for item in page_items:
                kind = item[0]
                if kind == "section_rawsub":
                    count = item[1]
                    parts.append(f"\U0001f4fa <b>Raw &amp; Sub \u2014 {count} show(s)</b>")
                    parts.append("")
                elif kind == "section_dub":
                    count = item[1]
                    parts.append(f"\U0001f3ac <b>Dubbed \u2014 {count} show(s)</b>")
                    parts.append("")
                elif kind == "divider":
                    parts.append(item[1])
                    parts.append("")
                elif kind == "entry":
                    parts.append(f"<blockquote>{item[1]}</blockquote>")
                    parts.append("")  # blank line after every entry
            result.append("\n".join(parts))

        return result
