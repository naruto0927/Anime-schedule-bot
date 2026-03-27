"""
bot/keyboards.py — All InlineKeyboardMarkup builders and filter display text.

All methods are @staticmethod and return InlineKeyboardMarkup
or (text, InlineKeyboardMarkup). Mixed into AnimeBot via KeyboardsMixin.
"""

from __future__ import annotations
import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

from pyrogram import enums
from pyrogram.enums import ChatType
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import (
    ADMIN_IDS, BOT_IMAGE, DAYS_OF_WEEK, MEDIA_TYPES, STREAMS, TZ, TZ_NAME,
)
from utils.filters import ChatFilter
from utils.helpers import al_status, clean_html, current_week_year, fmt_time

logger = logging.getLogger("NarutoTimekeeper")


class KeyboardsMixin:
    """Static keyboard builders — mixed into AnimeBot."""

    def _kb_main() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f4c5 View Schedule",     callback_data="menu:schedule")],
            [InlineKeyboardButton("\U0001f514 Reminder Settings", callback_data="menu:reminders")],
            [InlineKeyboardButton("\U0001f4ca Weekly Overview",   callback_data="menu:weekly")],
            [InlineKeyboardButton("\U0001f39b Filter Settings",   callback_data="menu:filter")],
            [InlineKeyboardButton("\U0001f4d6 Help",              callback_data="menu:help")],
            [InlineKeyboardButton("\u274c Close",                 callback_data="menu:close")],
        ])

    def _kb_days(year: Optional[int] = None, week: Optional[int] = None) -> InlineKeyboardMarkup:
        """
        Day-picker keyboard for a given year/week (defaults to current week).
        Includes Prev Week / Next Week navigation and a week label.
        """
        import datetime as _dt
        if year is None or week is None:
            iso = datetime.now(TZ).isocalendar()
            year, week = iso.year, iso.week

        # Compute prev/next week using ISO calendar arithmetic
        # Find Monday of the target week
        monday = _dt.date.fromisocalendar(year, week, 1)
        prev_mon = monday - _dt.timedelta(weeks=1)
        next_mon = monday + _dt.timedelta(weeks=1)
        prev_iso = prev_mon.isocalendar()
        next_iso = next_mon.isocalendar()

        rows = []
        for i in range(0, len(DAYS_OF_WEEK), 2):
            rows.append([
                InlineKeyboardButton(d, callback_data=f"day:{d}:{year}:{week}:0")
                for d in DAYS_OF_WEEK[i : i + 2]
            ])

        # Week navigation row
        rows.append([
            InlineKeyboardButton(
                "\u25c4 Prev Week",
                callback_data=f"sched:week:{prev_iso.year}:{prev_iso.week}",
            ),
            InlineKeyboardButton(
                f"W{week} / {year}",
                callback_data="noop",
            ),
            InlineKeyboardButton(
                "Next Week \u25ba",
                callback_data=f"sched:week:{next_iso.year}:{next_iso.week}",
            ),
        ])
        rows.append([InlineKeyboardButton("\u25c0 Back", callback_data="menu:back")])
        return InlineKeyboardMarkup(rows)

    def _kb_reminders() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("\u2705 Subscribe",   callback_data="rem:on")],
            [InlineKeyboardButton("\u274c Unsubscribe", callback_data="rem:off")],
            [InlineKeyboardButton("\U0001f4cb Status",  callback_data="rem:status")],
            [InlineKeyboardButton("\u25c0 Back",        callback_data="menu:back")],
        ])

    def _topic_settings_panel(topic_id: int, current_mode: Optional[str], target_chat_id: int = 0) -> tuple:
        """Return (text, InlineKeyboardMarkup) for the topic settings main panel."""
        mode_label = {
            "rem":  "📅 Reminders",
            "nyaa": "🌐 Nyaa Scraper",
        }.get(current_mode or "", "None")

        cid = target_chat_id  # embed in callback data so handler knows which chat
        text = (
            f"⚙️ <b>Topic Settings</b>\n\n"
            f"Chat: <code>{cid}</code>\n"
            f"Topic ID: <code>{topic_id}</code>\n"
            f"Current mode: <b>{mode_label}</b>\n\n"
            "Select a mode to assign this topic, or configure the current mode below."
        )
        rows = [
            [
                InlineKeyboardButton(
                    "📅 Reminder" + (" ✅" if current_mode == "rem"  else ""),
                    callback_data=f"ts:set:rem:{cid}:{topic_id}",
                ),
                InlineKeyboardButton(
                    "🌐 Nyaa"     + (" ✅" if current_mode == "nyaa" else ""),
                    callback_data=f"ts:set:nyaa:{cid}:{topic_id}",
                ),
            ],
        ]
        if current_mode == "rem":
            rows.append([InlineKeyboardButton(
                "🔧 Reminder Settings", callback_data=f"ts:cfg:rem:{cid}:{topic_id}"
            )])
        elif current_mode == "nyaa":
            rows.append([InlineKeyboardButton(
                "🔧 Nyaa Settings", callback_data=f"ts:cfg:nyaa:{cid}:{topic_id}"
            )])
        rows.append([InlineKeyboardButton(
            "🚫 Clear Mode", callback_data=f"ts:clear:{cid}:{topic_id}"
        )])
        rows.append([InlineKeyboardButton("✖ Close", callback_data="ts:close")])
        return text, InlineKeyboardMarkup(rows)

    def _rem_settings_kb(topic_id: int, rem_cfg: dict, target_chat_id: int = 0) -> InlineKeyboardMarkup:
        """Keyboard for reminder-topic sub-settings (air types + schedule filter)."""
        def tick(v): return "✅" if v else "⬜"
        show_raw = rem_cfg.get("show_raw", True)
        show_sub = rem_cfg.get("show_sub", True)
        show_dub = rem_cfg.get("show_dub", True)
        cid = target_chat_id
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{tick(show_raw)} Raw", callback_data=f"ts:rem:raw:{cid}:{topic_id}")],
            [InlineKeyboardButton(f"{tick(show_sub)} Sub", callback_data=f"ts:rem:sub:{cid}:{topic_id}")],
            [InlineKeyboardButton(f"{tick(show_dub)} Dub", callback_data=f"ts:rem:dub:{cid}:{topic_id}")],
            [InlineKeyboardButton("🎛 Schedule Filter",    callback_data=f"ts:flt:main:{cid}:{topic_id}")],
            [InlineKeyboardButton("◀ Back",               callback_data=f"ts:back:{cid}:{topic_id}")],
        ])

    def _nyaa_settings_kb(topic_id: int, nyaa_cfg: dict, target_chat_id: int = 0) -> InlineKeyboardMarkup:
        """Keyboard for nyaa-topic sub-settings."""
        def tick(v): return "✅" if v else "⬜"
        varyg      = nyaa_cfg.get("varyg",      True)
        toonshub   = nyaa_cfg.get("toonshub",   True)
        subsplease = nyaa_cfg.get("subsplease", True)
        cid = target_chat_id
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🔴 {tick(varyg)}    varyg1001",    callback_data=f"ts:nyaa:varyg:{cid}:{topic_id}")],
            [InlineKeyboardButton(f"🟡 {tick(toonshub)} ToonsHub",     callback_data=f"ts:nyaa:toonshub:{cid}:{topic_id}")],
            [InlineKeyboardButton(f"🟢 {tick(subsplease)} subsplease", callback_data=f"ts:nyaa:subsplease:{cid}:{topic_id}")],
            [InlineKeyboardButton("◀ Back", callback_data=f"ts:back:{cid}:{topic_id}")],
        ])

    def _filter_text(flt: "ChatFilter") -> str:
        air_on  = ", ".join(sorted(flt.air_types)).upper() or "None"
        mt_on   = ", ".join(MEDIA_TYPES.get(s, s) for s in sorted(flt.media_types)) or "All"
        st_on   = ", ".join(STREAMS.get(s, s) for s in sorted(flt.streams)) or "All"
        donghua = "Hidden" if flt.hide_donghua else "Shown"
        return (
            "🎛 <b>Schedule Filters</b>\n\n"
            f"📡 <b>Air Types:</b> {air_on}\n"
            f"📺 <b>Media Types:</b> {mt_on}\n"
            f"🎞 <b>Streams:</b> {st_on}\n"
            f"🐉 <b>Donghua:</b> {donghua}\n\n"
            "<i>Use the buttons below to adjust filters.</i>"
        )

    def _kb_filter(flt: "ChatFilter", section: str = "main") -> InlineKeyboardMarkup:
        def tick(on: bool) -> str:
            return "✅" if on else "⬜"

        if section == "airtype":
            rows = [
                [InlineKeyboardButton(f"{tick('raw' in flt.air_types)} Raw",  callback_data="flt:air:raw")],
                [InlineKeyboardButton(f"{tick('sub' in flt.air_types)} Sub",  callback_data="flt:air:sub")],
                [InlineKeyboardButton(f"{tick('dub' in flt.air_types)} Dub",  callback_data="flt:air:dub")],
                [InlineKeyboardButton("◀ Back", callback_data="flt:main")],
            ]
        elif section == "mediatype":
            rows = []
            for slug, label in MEDIA_TYPES.items():
                rows.append([InlineKeyboardButton(
                    f"{tick(slug in flt.media_types)} {label}",
                    callback_data=f"flt:mt:{slug}",
                )])
            rows.append([InlineKeyboardButton("🔄 Show All", callback_data="flt:mt:reset")])
            rows.append([InlineKeyboardButton("◀ Back", callback_data="flt:main")])
        elif section == "streams":
            rows = []
            for slug, label in STREAMS.items():
                rows.append([InlineKeyboardButton(
                    f"{tick(slug in flt.streams)} {label}",
                    callback_data=f"flt:st:{slug}",
                )])
            rows.append([InlineKeyboardButton("🔄 Show All", callback_data="flt:st:reset")])
            rows.append([InlineKeyboardButton("◀ Back", callback_data="flt:main")])
        elif section == "other":
            rows = [
                [InlineKeyboardButton(
                    f"{tick(flt.hide_donghua)} Hide Donghua",
                    callback_data="flt:other:donghua",
                )],
                [InlineKeyboardButton("◀ Back", callback_data="flt:main")],
            ]
        else:  # main
            rows = [
                [InlineKeyboardButton("📡 Air Types",   callback_data="flt:section:airtype")],
                [InlineKeyboardButton("📺 Media Types", callback_data="flt:section:mediatype")],
                [InlineKeyboardButton("🎞 Streams",     callback_data="flt:section:streams")],
                [InlineKeyboardButton("🐉 Other",       callback_data="flt:section:other")],
                [InlineKeyboardButton("🔄 Reset All",   callback_data="flt:reset")],
                [
                    InlineKeyboardButton("◀ Back", callback_data="menu:back"),
                    InlineKeyboardButton("✖ Close", callback_data="flt:close"),
                ],
            ]
        return InlineKeyboardMarkup(rows)

    def _kb_topic_filter(flt: "ChatFilter", section: str, cid: int, tid: int) -> InlineKeyboardMarkup:
        """
        Like _kb_filter but scoped to a specific topic.
        Uses ts:flt:* callback data with cid and tid embedded.
        """
        def tick(on: bool) -> str:
            return "✅" if on else "⬜"

        back_to_rem = f"ts:cfg:rem:{cid}:{tid}"  # back → rem settings

        if section == "airtype":
            rows = [
                [InlineKeyboardButton(f"{tick('raw' in flt.air_types)} Raw", callback_data=f"ts:flt:air:raw:{cid}:{tid}")],
                [InlineKeyboardButton(f"{tick('sub' in flt.air_types)} Sub", callback_data=f"ts:flt:air:sub:{cid}:{tid}")],
                [InlineKeyboardButton(f"{tick('dub' in flt.air_types)} Dub", callback_data=f"ts:flt:air:dub:{cid}:{tid}")],
                [InlineKeyboardButton("◀ Back", callback_data=f"ts:flt:main:{cid}:{tid}")],
            ]
        elif section == "mediatype":
            rows = []
            for slug, label in MEDIA_TYPES.items():
                rows.append([InlineKeyboardButton(
                    f"{tick(slug in flt.media_types)} {label}",
                    callback_data=f"ts:flt:mt:{slug}:{cid}:{tid}",
                )])
            rows.append([InlineKeyboardButton("🔄 Show All", callback_data=f"ts:flt:mt:reset:{cid}:{tid}")])
            rows.append([InlineKeyboardButton("◀ Back",      callback_data=f"ts:flt:main:{cid}:{tid}")])
        elif section == "streams":
            rows = []
            for slug, label in STREAMS.items():
                rows.append([InlineKeyboardButton(
                    f"{tick(slug in flt.streams)} {label}",
                    callback_data=f"ts:flt:st:{slug}:{cid}:{tid}",
                )])
            rows.append([InlineKeyboardButton("🔄 Show All", callback_data=f"ts:flt:st:reset:{cid}:{tid}")])
            rows.append([InlineKeyboardButton("◀ Back",      callback_data=f"ts:flt:main:{cid}:{tid}")])
        elif section == "other":
            rows = [
                [InlineKeyboardButton(
                    f"{tick(flt.hide_donghua)} Hide Donghua",
                    callback_data=f"ts:flt:other:donghua:{cid}:{tid}",
                )],
                [InlineKeyboardButton("◀ Back", callback_data=f"ts:flt:main:{cid}:{tid}")],
            ]
        else:  # main
            rows = [
                [InlineKeyboardButton("📡 Air Types",   callback_data=f"ts:flt:section:airtype:{cid}:{tid}")],
                [InlineKeyboardButton("📺 Media Types", callback_data=f"ts:flt:section:mediatype:{cid}:{tid}")],
                [InlineKeyboardButton("🎞 Streams",     callback_data=f"ts:flt:section:streams:{cid}:{tid}")],
                [InlineKeyboardButton("🐉 Other",       callback_data=f"ts:flt:section:other:{cid}:{tid}")],
                [InlineKeyboardButton("🔄 Reset All",   callback_data=f"ts:flt:reset:{cid}:{tid}")],
                [InlineKeyboardButton("◀ Back",         callback_data=back_to_rem)],
            ]
        return InlineKeyboardMarkup(rows)

    def _kb_season(year: int, season: str, page: int, total_pages: int) -> InlineKeyboardMarkup:
        rows: list = []
        nav: list = []
        if page > 0:
            nav.append(InlineKeyboardButton(
                "◀ Prev", callback_data=f"season:{year}:{season}:{page - 1}"
            ))
        if total_pages > 1:
            nav.append(InlineKeyboardButton(
                f"{page + 1}/{total_pages}", callback_data="noop"
            ))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(
                "Next ▶", callback_data=f"season:{year}:{season}:{page + 1}"
            ))
        if nav:
            rows.append(nav)
        return InlineKeyboardMarkup(rows) if rows else None
