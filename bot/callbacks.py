"""
bot/callbacks.py — Inline-keyboard callback query router.

handle_callback() dispatches on callback_data prefix:
  menu:    Main-menu navigation
  day:     Schedule day view + pagination
  rem:     Reminder subscription toggle
  sched:   Week navigation
  flt:     Chat-level schedule filter toggles
  ts:      Topic-mode settings (rem / nyaa toggles)
  al:      AniList search, detail cards, Back navigation
  season:  Seasonal list pagination
  noop     Page-label no-op buttons
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
from scrapers.season import SeasonScraper


class CallbacksMixin:
    """Callback handler methods — mixed into AnimeBot."""

    async def safe_edit(query, text: str, reply_markup=None, parse_mode=None):
        """Edit a message text. Falls back to delete+resend for photo messages.
        Handles FloodWait by sleeping and retrying once."""
        import re as _re
        for attempt in range(2):
            try:
                await query.edit_message_text(
                    text, reply_markup=reply_markup, parse_mode=parse_mode
                )
                return
            except Exception as exc:
                err = str(exc)
                if "MESSAGE_NOT_MODIFIED" in err or "message is not modified" in err.lower():
                    return
                if "FLOOD_WAIT" in err:
                    m = _re.search(r"wait of (\d+) seconds", err)
                    wait = int(m.group(1)) + 1 if m else 5
                    logger.info("safe_edit FloodWait %ds, retrying…", wait)
                    await asyncio.sleep(wait)
                    continue
                if "MEDIA_CAPTION_TOO_LONG" in err or "caption" in err.lower():
                    try:
                        chat_id = query.message.chat.id
                        await query.message.delete()
                        await query.message._client.send_message(
                            chat_id, text,
                            reply_markup=reply_markup,
                            parse_mode=parse_mode,
                        )
                        return
                    except Exception:
                        pass
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
            y, w = current_week_year()
            await self.safe_edit(
                query,
                f"\U0001f4c5 <b>Select a day</b> \u2014 Week {w}, {y}:",
                reply_markup=self._kb_days(y, w),
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

        elif data == "menu:filter":
            flt = await self.db.get_filter(chat_id)
            await self.safe_edit(
                query,
                self._filter_text(flt),
                reply_markup=self._kb_filter(flt, section="main"),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data == "menu:weekly":
            await query.answer("Loading\u2026")
            schedule = await self._get_schedule(chat_id)
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
            # Format: day:{day}:{year}:{week}:{pg}
            parts  = data.split(":")
            day    = parts[1]
            d_year = int(parts[2]) if len(parts) > 2 else None
            d_week = int(parts[3]) if len(parts) > 3 else None
            pg     = int(parts[4]) if len(parts) > 4 else 0

            await query.answer(f"Loading {day}\u2026")
            schedule    = await self._get_schedule(chat_id, year=d_year, week=d_week)
            flt         = await self.db.get_filter(chat_id)
            pages       = ScheduleProcessor.day_pages(day, schedule.get(day, []), flt=flt, year=d_year, week=d_week)
            total_pages = len(pages)
            pg          = max(0, min(pg, total_pages - 1))
            text        = pages[pg]

            # Back button returns to week view for the same year/week
            back_data = (f"sched:week:{d_year}:{d_week}"
                         if d_year and d_week else "menu:schedule")

            # Always show pagination row (season-style)
            nav: list = []
            page_row: list = []
            if pg > 0:
                page_row.append(InlineKeyboardButton(
                    "\u25c4 Prev",
                    callback_data=f"day:{day}:{d_year}:{d_week}:{pg - 1}",
                ))
            page_row.append(InlineKeyboardButton(
                f"{pg + 1}/{total_pages}", callback_data="noop"
            ))
            if pg < total_pages - 1:
                page_row.append(InlineKeyboardButton(
                    "Next \u25ba",
                    callback_data=f"day:{day}:{d_year}:{d_week}:{pg + 1}",
                ))
            nav.append(page_row)
            nav.append([InlineKeyboardButton("\u25c0 Back", callback_data=back_data)])

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

        elif data.startswith("sched:week:"):
            # sched:week:<year>:<week>
            parts  = data.split(":")
            s_year = int(parts[2])
            s_week = int(parts[3])
            await query.answer(f"Week {s_week}, {s_year}")
            await self.safe_edit(
                query,
                f"\U0001f4c5 <b>Select a day</b> \u2014 Week {s_week}, {s_year}:",
                reply_markup=self._kb_days(s_year, s_week),
                parse_mode=enums.ParseMode.HTML,
            )

        elif data.startswith("flt:"):
            # Routing table:
            #   flt:main               — show main filter menu
            #   flt:section:<name>     — open sub-panel (airtype/mediatype/streams/other)
            #   flt:air:<key>          — toggle air type (raw/sub/dub)
            #   flt:mt:<slug>          — toggle media type slug (or "reset")
            #   flt:st:<slug>          — toggle stream slug (or "reset")
            #   flt:other:<key>        — toggle other flags (donghua)
            #   flt:reset              — reset entire filter to defaults
            #   flt:close              — delete the filter message
            parts  = data.split(":")
            action = parts[1] if len(parts) > 1 else ""

            async def _refresh(section: str = "main"):
                flt = await self.db.get_filter(chat_id)
                await self.safe_edit(
                    query,
                    self._filter_text(flt),
                    reply_markup=self._kb_filter(flt, section=section),
                    parse_mode=enums.ParseMode.HTML,
                )

            if action == "main":
                await _refresh("main")
                await query.answer()

            elif action == "section":
                section = parts[2] if len(parts) > 2 else "main"
                await _refresh(section)
                await query.answer()

            elif action == "air":
                key = parts[2] if len(parts) > 2 else ""
                if key in ("raw", "sub", "dub"):
                    flt = await self.db.get_filter(chat_id)
                    if key in flt.air_types:
                        flt.air_types.discard(key)
                    else:
                        flt.air_types.add(key)
                    # At least one air type must stay on
                    if not flt.air_types:
                        flt.air_types = {"raw", "sub", "dub"}
                        await query.answer(
                            "⚠️ At least one air type must be enabled.",
                            show_alert=True,
                        )
                    else:
                        await self.db.set_filter(chat_id, flt)
                        await query.answer(
                            f"{'Enabled' if key in flt.air_types else 'Disabled'} "
                            f"{key.capitalize()}"
                        )
                    await self.safe_edit(
                        query,
                        self._filter_text(flt),
                        reply_markup=self._kb_filter(flt, section="airtype"),
                        parse_mode=enums.ParseMode.HTML,
                    )

            elif action == "mt":
                slug = parts[2] if len(parts) > 2 else ""
                flt  = await self.db.get_filter(chat_id)
                if slug == "reset":
                    flt.media_types = set()
                    await query.answer("✅ Showing all media types")
                elif slug in MEDIA_TYPES:
                    if slug in flt.media_types:
                        flt.media_types.discard(slug)
                        await query.answer(f"Removed: {MEDIA_TYPES[slug]}")
                    else:
                        flt.media_types.add(slug)
                        await query.answer(f"Added: {MEDIA_TYPES[slug]}")
                await self.db.set_filter(chat_id, flt)
                await self.safe_edit(
                    query,
                    self._filter_text(flt),
                    reply_markup=self._kb_filter(flt, section="mediatype"),
                    parse_mode=enums.ParseMode.HTML,
                )

            elif action == "st":
                slug = parts[2] if len(parts) > 2 else ""
                flt  = await self.db.get_filter(chat_id)
                if slug == "reset":
                    flt.streams = set()
                    await query.answer("✅ Showing all platforms")
                elif slug in STREAMS:
                    if slug in flt.streams:
                        flt.streams.discard(slug)
                        await query.answer(f"Removed: {STREAMS[slug]}")
                    else:
                        flt.streams.add(slug)
                        await query.answer(f"Added: {STREAMS[slug]}")
                await self.db.set_filter(chat_id, flt)
                await self.safe_edit(
                    query,
                    self._filter_text(flt),
                    reply_markup=self._kb_filter(flt, section="streams"),
                    parse_mode=enums.ParseMode.HTML,
                )

            elif action == "other":
                key = parts[2] if len(parts) > 2 else ""
                flt = await self.db.get_filter(chat_id)
                if key == "donghua":
                    flt.hide_donghua = not flt.hide_donghua
                    state = "hidden" if flt.hide_donghua else "shown"
                    await query.answer(f"Donghua: {state}")
                await self.db.set_filter(chat_id, flt)
                await self.safe_edit(
                    query,
                    self._filter_text(flt),
                    reply_markup=self._kb_filter(flt, section="other"),
                    parse_mode=enums.ParseMode.HTML,
                )

            elif action == "reset":
                await self.db.reset_filter(chat_id)
                flt = ChatFilter()
                await self.safe_edit(
                    query,
                    self._filter_text(flt),
                    reply_markup=self._kb_filter(flt, section="main"),
                    parse_mode=enums.ParseMode.HTML,
                )
                await query.answer("✅ All filters reset to default")

            elif action == "close":
                await query.message.delete()

            else:
                await query.answer()

        elif data.startswith("al:"):
            # al:close                     — delete the current message
            # al:anime:<id>                — show anime card (delete search list)
            # al:manga:<id>                — show manga card (delete search list)
            # al:back:anime:<id>           — re-show search list, delete card
            # al:back:manga:<id>           — re-show search list, delete card
            parts   = data.split(":")
            al_type = parts[1] if len(parts) > 1 else ""

            if al_type == "close":
                try:
                    await query.message.delete()
                except Exception:
                    pass
                return

            if al_type in ("anime", "manga"):
                al_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
                if not al_id:
                    await query.answer("Invalid ID.", show_alert=True)
                    return
                await query.answer("Loading…")
                # query.message is the search-list message — delete it, send card as new msg
                search_msg = query.message
                if al_type == "anime":
                    r = await self.al_api.get_anime_by_id(al_id)
                    if r:
                        await self._send_anime_card(search_msg, r, search_msg=search_msg, al_type="anime")
                    else:
                        await query.answer("❌ Not found.", show_alert=True)
                else:
                    r = await self.al_api.get_manga_by_id(al_id)
                    if r:
                        await self._send_manga_card(search_msg, r, search_msg=search_msg, al_type="manga")
                    else:
                        await query.answer("❌ Not found.", show_alert=True)
                return

            if al_type == "back":
                # al:back:<media_type>:<id>
                # Restore the search list from cache (populated when /anime or /manga ran).
                await query.answer()
                sub = parts[2] if len(parts) > 2 else "anime"   # "anime" or "manga"
                cache_key = f"al:search:{sub}:{chat_id}"
                cached = await self.db.get_cache(cache_key)
                if cached:
                    q       = cached.get("q", "")
                    results = cached.get("results", [])
                    rows    = self._al_search_rows(results, sub)
                    try:
                        await query.message.edit_text(
                            f"\U0001f50d <b>Results for \"{q}\"</b>\nTap a title to view details:",
                            reply_markup=InlineKeyboardMarkup(rows),
                            parse_mode=enums.ParseMode.HTML,
                        )
                        return
                    except Exception:
                        pass
                # Fallback: just delete the card if cache is gone
                try:
                    await query.message.delete()
                except Exception:
                    pass
                return

        elif data.startswith("season:"):
            # season:<year>:<season>:<page>
            parts = data.split(":")
            if len(parts) == 4:
                s_year   = int(parts[1])
                s_season = parts[2]
                s_page   = int(parts[3])
                await query.answer(f"Loading page {s_page + 1}…")
                titles = await self.season.get_season(s_year, s_season)
                if not titles:
                    await query.answer("No data available.", show_alert=True)
                    return
                text, total_pages = SeasonScraper.paginate(titles, s_year, s_season, page=s_page)
                kb = self._kb_season(s_year, s_season, s_page, total_pages)
                await self.safe_edit(
                    query, text, reply_markup=kb, parse_mode=enums.ParseMode.HTML
                )
            else:
                await query.answer()

        elif data.startswith("ts:"):
            # Topic settings callbacks — all require group admin
            # ts:set:<mode>:<tid>      — assign mode to topic
            # ts:clear:<cid>:<tid>           — clear topic mode
            # ts:cfg:rem:<cid>:<tid>         — open rem sub-settings
            # ts:cfg:nyaa:<cid>:<tid>        — open nyaa sub-settings
            # ts:rem:<key>:<cid>:<tid>       — toggle rem sub-setting
            # ts:nyaa:<key>:<cid>:<tid>      — toggle nyaa sub-setting
            # ts:back:<cid>:<tid>            — back to topic main panel
            # ts:close                       — delete the panel
            if not await self._is_group_admin(query.message):
                await query.answer("⛔ Admins only.", show_alert=True)
                return

            parts  = data.split(":")
            action = parts[1] if len(parts) > 1 else ""

            if action == "close":
                try:
                    await query.message.delete()
                except Exception:
                    pass
                await query.answer()
                return

            # All actions (except close) encode <cid> and <tid> as the last two parts
            try:
                t_cid = int(parts[-2])
                t_tid = int(parts[-1])
            except (ValueError, IndexError):
                await query.answer("Invalid callback data.", show_alert=True)
                return

            if action == "set":
                t_mode = parts[2] if len(parts) > 2 else ""
                if t_mode in ("rem", "nyaa"):
                    await self.db.set_topic_mode(t_cid, t_tid, t_mode)
                    await query.answer(f"✅ Mode set to {'Reminders' if t_mode == 'rem' else 'Nyaa'}")
                text, kb = self._topic_settings_panel(t_tid, t_mode, t_cid)
                await self.safe_edit(query, text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

            elif action == "clear":
                await self.db.set_topic_mode(t_cid, t_tid, "")
                await query.answer("🚫 Topic mode cleared.")
                text, kb = self._topic_settings_panel(t_tid, None, t_cid)
                await self.safe_edit(query, text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

            elif action == "cfg":
                sub = parts[2] if len(parts) > 2 else ""
                cfg = await self.db.get_topic_cfg(t_cid, t_tid)
                if sub == "rem":
                    rem_cfg = cfg.get("rem", {"show_raw": True, "show_sub": True, "show_dub": True})
                    await self.safe_edit(
                        query,
                        f"🔧 <b>Reminder Settings</b> — Topic <code>{t_tid}</code>\n\n"
                        "<b>Air Types</b> — toggle which versions this topic receives:\n"
                        "<i>Schedule Filter</i> — filter shows by media type, stream, donghua:",
                        reply_markup=self._rem_settings_kb(t_tid, rem_cfg, t_cid),
                        parse_mode=enums.ParseMode.HTML,
                    )
                elif sub == "nyaa":
                    nyaa_cfg = cfg.get("nyaa", {"varyg": True, "toonshub": True, "subsplease": True})
                    await self.safe_edit(
                        query,
                        f"🔧 <b>Nyaa Settings</b> — Topic <code>{t_tid}</code>\n\n"
                        "Toggle which uploaders this topic receives:",
                        reply_markup=self._nyaa_settings_kb(t_tid, nyaa_cfg, t_cid),
                        parse_mode=enums.ParseMode.HTML,
                    )
                await query.answer()

            elif action == "rem":
                key = parts[2] if len(parts) > 2 else ""
                if key in ("raw", "sub", "dub"):
                    cfg = await self.db.get_topic_cfg(t_cid, t_tid)
                    # Always initialise ALL three keys so partial DB saves
                    # (e.g. only "show_sub" present) don't leave raw/dub
                    # defaulting to True and bypassing the filter.
                    _defaults = {"show_raw": True, "show_sub": True, "show_dub": True}
                    rem_cfg = {**_defaults, **cfg.get("rem", {})}
                    field = f"show_{key}"
                    rem_cfg[field] = not rem_cfg[field]
                    cfg["rem"] = rem_cfg
                    await self.db.set_topic_cfg(t_cid, t_tid, cfg)
                    await query.answer(f"{'✅' if rem_cfg[field] else '⬜'} {key.capitalize()} {'on' if rem_cfg[field] else 'off'}")
                    await self.safe_edit(
                        query,
                        f"🔧 <b>Reminder Settings</b> — Topic <code>{t_tid}</code>\n\n"
                        "Toggle which air types this topic receives:",
                        reply_markup=self._rem_settings_kb(t_tid, rem_cfg, t_cid),
                        parse_mode=enums.ParseMode.HTML,
                    )

            elif action == "nyaa":
                key = parts[2] if len(parts) > 2 else ""
                if key in ("varyg", "toonshub", "subsplease"):
                    cfg = await self.db.get_topic_cfg(t_cid, t_tid)
                    _nyaa_defaults = {"varyg": True, "toonshub": True, "subsplease": True}
                    nyaa_cfg = {**_nyaa_defaults, **cfg.get("nyaa", {})}
                    nyaa_cfg[key] = not nyaa_cfg[key]
                    cfg["nyaa"] = nyaa_cfg
                    await self.db.set_topic_cfg(t_cid, t_tid, cfg)
                    lbl = {"varyg": "varyg1001", "toonshub": "ToonsHub", "subsplease": "subsplease"}.get(key, key)
                    await query.answer(f"{'✅' if nyaa_cfg[key] else '⬜'} {lbl} {'on' if nyaa_cfg[key] else 'off'}")
                    await self.safe_edit(
                        query,
                        f"🔧 <b>Nyaa Settings</b> — Topic <code>{t_tid}</code>\n\n"
                        "Toggle which uploaders this topic receives:",
                        reply_markup=self._nyaa_settings_kb(t_tid, nyaa_cfg, t_cid),
                        parse_mode=enums.ParseMode.HTML,
                    )

            elif action == "back":
                current_mode = await self.db.get_topic_mode(t_cid, t_tid)
                text, kb = self._topic_settings_panel(t_tid, current_mode, t_cid)
                await self.safe_edit(query, text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)
                await query.answer()

            elif action == "flt":
                # ts:flt:<sub>:...:<cid>:<tid>
                # sub: main, section, air, mt, st, other, reset
                # cid and tid are always the last two parts
                sub = parts[2] if len(parts) > 2 else "main"

                async def _flt_refresh(sec: str):
                    flt = await self.db.get_topic_filter(t_cid, t_tid)
                    await self.safe_edit(
                        query,
                        f"🎛 <b>Schedule Filter</b> — Topic <code>{t_tid}</code>\n\n"
                        + AnimeBot._filter_text(flt),
                        reply_markup=self._kb_topic_filter(flt, sec, t_cid, t_tid),
                        parse_mode=enums.ParseMode.HTML,
                    )

                if sub == "main":
                    await _flt_refresh("main")
                    await query.answer()

                elif sub == "section":
                    sec = parts[3] if len(parts) > 3 else "main"
                    await _flt_refresh(sec)
                    await query.answer()

                elif sub == "air":
                    key = parts[3] if len(parts) > 3 else ""
                    if key in ("raw", "sub", "dub"):
                        flt = await self.db.get_topic_filter(t_cid, t_tid)
                        if key in flt.air_types:
                            flt.air_types.discard(key)
                        else:
                            flt.air_types.add(key)
                        if not flt.air_types:
                            flt.air_types = {"raw", "sub", "dub"}
                            await query.answer("⚠️ At least one air type must be enabled.", show_alert=True)
                        else:
                            await self.db.set_topic_filter(t_cid, t_tid, flt)
                            await query.answer(f"{'Enabled' if key in flt.air_types else 'Disabled'} {key.capitalize()}")
                        await _flt_refresh("airtype")

                elif sub == "mt":
                    slug = parts[3] if len(parts) > 3 else ""
                    flt  = await self.db.get_topic_filter(t_cid, t_tid)
                    if slug == "reset":
                        flt.media_types = set()
                        await query.answer("✅ Showing all media types")
                    elif slug in MEDIA_TYPES:
                        if slug in flt.media_types:
                            flt.media_types.discard(slug)
                            await query.answer(f"Removed: {MEDIA_TYPES[slug]}")
                        else:
                            flt.media_types.add(slug)
                            await query.answer(f"Added: {MEDIA_TYPES[slug]}")
                    await self.db.set_topic_filter(t_cid, t_tid, flt)
                    await _flt_refresh("mediatype")

                elif sub == "st":
                    slug = parts[3] if len(parts) > 3 else ""
                    flt  = await self.db.get_topic_filter(t_cid, t_tid)
                    if slug == "reset":
                        flt.streams = set()
                        await query.answer("✅ Showing all platforms")
                    elif slug in STREAMS:
                        if slug in flt.streams:
                            flt.streams.discard(slug)
                            await query.answer(f"Removed: {STREAMS[slug]}")
                        else:
                            flt.streams.add(slug)
                            await query.answer(f"Added: {STREAMS[slug]}")
                    await self.db.set_topic_filter(t_cid, t_tid, flt)
                    await _flt_refresh("streams")

                elif sub == "other":
                    key = parts[3] if len(parts) > 3 else ""
                    flt = await self.db.get_topic_filter(t_cid, t_tid)
                    if key == "donghua":
                        flt.hide_donghua = not flt.hide_donghua
                        await query.answer(f"Donghua: {'hidden' if flt.hide_donghua else 'shown'}")
                    await self.db.set_topic_filter(t_cid, t_tid, flt)
                    await _flt_refresh("other")

                elif sub == "reset":
                    await self.db.reset_topic_filter(t_cid, t_tid)
                    await query.answer("✅ Topic filter reset to defaults")
                    await _flt_refresh("main")

                else:
                    await query.answer()

            else:
                await query.answer()

        elif data == "noop":
            await query.answer()

        else:
            await query.answer("Unknown action.")
