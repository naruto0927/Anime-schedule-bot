"""
bot/commands.py — All Telegram command handlers.

Each cmd_* is registered via AnimeBot._register() and bound to the
Pyrogram Client. Depends on self.db, self.scraper, self.al_api,
self.sched, and self.app from AnimeBot.__init__.
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
from scrapers.schedule_processor import ScheduleProcessor
from scrapers.season import SeasonScraper


class CommandsMixin:
    """Command handler methods — mixed into AnimeBot."""

    async def _get_schedule(self, chat_id: Optional[int] = None,
                            year: Optional[int] = None,
                            week: Optional[int] = None) -> dict:
        """
        Fetch and process the schedule for a given year/week.
        Defaults to the current week.  Applies chat filter if chat_id provided.
        """
        if year is None or week is None:
            year, week = current_week_year()
        flt     = await self.db.get_filter(chat_id) if chat_id is not None else None
        entries = await self.scraper.get_timetable(year, week, flt=flt)
        return ScheduleProcessor.process(entries, flt=flt)

    async def cmd_start(self, _, msg: Message):
        if not await self._auth(msg):
            return
        await self.db.track_user(msg)  # track private users for /users count
        name = msg.from_user.first_name if msg.from_user else "Shinobi"
        text = (
            f"\U0001f343 <b>Naruto Timekeeper</b> \U0001f343\n\n"
            f"Yo, <b>{name}</b>!\n\n"
            "I track every anime episode that airs \u2014 raw, sub, and dub \u2014 "
            "so you never miss a moment.\n\n"
            "\U0001f4dc <b>What I can do:</b>\n"
            "  \u2022 Weekly anime schedule by day\n"
            "  \u2022 Episode reminders the moment they air\n"
            "  \u2022 Search any anime or manga via AniList\n\n"
            "Hit the button below to get started \u2193"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f4c5 View Schedule",   callback_data="menu:schedule")],
            [InlineKeyboardButton("\U0001f514 Reminders",       callback_data="menu:reminders")],
            [InlineKeyboardButton("\U0001f4ca Weekly Overview", callback_data="menu:weekly")],
            [InlineKeyboardButton("\U0001f4d6 Help",            callback_data="menu:help")],
        ])
        if BOT_IMAGE:
            try:
                await msg.reply_photo(
                    photo=BOT_IMAGE, caption=text,
                    reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                )
                return
            except Exception:
                pass
        await msg.reply(text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    async def cmd_help(self, _, msg: Message):
        if not await self._auth(msg):
            return
        text = (
            "🍃 <b>Naruto Timekeeper — Help</b>\n\n"
            "<b>📌 User Commands</b>\n"
            "/start — Welcome screen\n"
            "/settings — Full settings panel\n"
            "/anime &lt;name&gt; — Search anime (AniList)\n"
            "/manga &lt;name&gt; — Search manga (AniList)\n"
            "/filter — Schedule filters\n"
            "/season &lt;year&gt; &lt;season&gt; — Seasonal anime list\n"
            "/help — Show this message\n\n"
            "<b>🛡 Group Admin Commands</b>\n"
            "/auth — Authorize group\n"
            "/deauth — Remove group authorization\n"
            "/mode — Open topic settings panel\n"
            "/mode &lt;chat_id&gt;|&lt;topic_id&gt; — Topic settings by ID\n\n"
            "<b>🔧 Bot Admin Commands</b>\n"
            "/reload — Force schedule refresh\n"
            "/stats — Bot usage statistics\n"
            "/broadcast &lt;msg&gt; — Send to all subscribed chats\n"
            "/addadmin &lt;user_id&gt; — Add a bot admin\n"
            "/remadmin &lt;user_id&gt; — Remove a bot admin\n"
            "/admins — List all bot admins\n"
            "/grouplist — List authorized groups\n"
            "/users — User &amp; group counts\n"
            "/restart — Restart the bot\n\n"
            f"🕐 All times in <b>{TZ_NAME}</b>"
        )
        if BOT_IMAGE:
            try:
                await msg.reply_photo(
                    photo=BOT_IMAGE, caption=text, parse_mode=enums.ParseMode.HTML,
                )
                return
            except Exception:
                pass
        await msg.reply(text, parse_mode=enums.ParseMode.HTML)

    async def cmd_settings(self, _, msg: Message):
        if not await self._auth(msg):
            return
        await msg.reply(
            "\U0001f343 <b>Naruto Timekeeper</b>\n\nChoose an option from the panel below:",
            reply_markup=self._kb_main(),
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_auth(self, _, msg: Message):
        """
        /auth — authorize this group to use the bot (group admin only).
        Use /mode inside a topic to assign rem/nyaa modes.
        """
        if msg.chat.type == ChatType.PRIVATE:
            await msg.reply("This command is for groups only.")
            return
        if not await self._is_group_admin(msg):
            await msg.reply("⛔ Only group admins can run this.")
            return
        await self.db.authorize_group(msg.chat.id, msg.from_user.id)
        await msg.reply(
            "✅ <b>Group authorized!</b>\n"
            "This group can now use Naruto Timekeeper.\n\n"
            "<b>Tip:</b> Use <code>/mode</code> inside a topic to assign "
            "it a mode (Reminders or Nyaa).",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_mode(self, _, msg: Message):
        """
        /mode                          — open topic settings for current topic
        /mode <chat_id>|<topic_id>     — open topic settings by ID from anywhere
        Example: /mode -1003739341690|46
        """
        if not await self._is_group_admin(msg):
            await msg.reply("⛔ Only group admins can run this.")
            return

        args = msg.command[1:] if len(msg.command) > 1 else []
        target_str = args[0] if args else None

        target_chat_id  = None
        target_topic_id = None

        if target_str and "|" in target_str:
            # Explicit: /mode -1003739341690|46
            parts = target_str.split("|", 1)
            try:
                target_chat_id  = int(parts[0])
                target_topic_id = int(parts[1])
            except ValueError:
                await msg.reply(
                    "❌ Invalid format. Use: <code>/mode -1001234567890|46</code>",
                    parse_mode=enums.ParseMode.HTML,
                )
                return
        else:
            # Auto-detect from current message
            if msg.chat.type == ChatType.PRIVATE:
                await msg.reply(
                    "Use: <code>/mode &lt;chat_id&gt;|&lt;topic_id&gt;</code>\n"
                    "Example: <code>/mode -1003739341690|46</code>",
                    parse_mode=enums.ParseMode.HTML,
                )
                return
            target_chat_id  = msg.chat.id
            target_topic_id = self._get_topic_id(msg)

            # Debug: log all relevant attrs so we can see what Pyrogram provides
            logger.info(
                "cmd_mode topic detection: chat=%s topic=%s | "
                "topic_obj=%s reply_to_top=%s is_topic=%s reply_to=%s thread=%s",
                msg.chat.id,
                target_topic_id,
                getattr(getattr(msg, 'topic', None), 'id', None),
                getattr(msg, 'reply_to_top_message_id', None),
                getattr(msg, 'is_topic_message', None),
                getattr(msg, 'reply_to_message_id', None),
                getattr(msg, 'message_thread_id', None),
            )

            if not target_topic_id:
                await msg.reply(
                    "⚠️ Could not detect topic ID automatically.\n\n"
                    "Use the explicit format instead:\n"
                    "<code>/mode &lt;chat_id&gt;|&lt;topic_id&gt;</code>\n\n"
                    "Your topic ID is in its link — e.g. <code>t.me/c/3739341690/<b>46</b></code> → ID is <b>46</b>\n\n"
                    "Example: <code>/mode -1003739341690|46</code>",
                    parse_mode=enums.ParseMode.HTML,
                )
                return

        current = await self.db.get_topic_mode(target_chat_id, target_topic_id)
        text, kb = self._topic_settings_panel(target_topic_id, current, target_chat_id)
        await msg.reply(text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    async def cmd_deauth(self, _, msg: Message):
        """
        /deauth            — deauthorize current group
        /deauth <chat_id>  — deauthorize a specific group by ID (bot admin only)
        """
        args           = msg.command[1:] if len(msg.command) > 1 else []
        target_str     = args[0] if args else None

        if target_str and target_str.lstrip("-").isdigit():
            # Bot admin deauthorizing another chat by ID
            if not await self._is_bot_admin(msg):
                return
            target_chat_id = int(target_str)
        else:
            # Deauthorize current group — requires group admin
            if msg.chat.type == ChatType.PRIVATE:
                await msg.reply("This command is for groups only.")
                return
            if not await self._is_group_admin(msg):
                await msg.reply("⛔ Only group admins can run this.")
                return
            target_chat_id = msg.chat.id

        await self.db.deauthorize_group(target_chat_id)
        await msg.reply(
            f"❌ Chat <code>{target_chat_id}</code> deauthorized.",
            parse_mode=enums.ParseMode.HTML,
        )

    def _al_search_rows(results: list, al_type: str) -> list:
        """Build InlineKeyboardButton rows for an AniList search result list."""
        rows = []
        for item in results:
            item_id   = item.get("id")
            t         = item.get("title", {})
            label     = t.get("english") or t.get("romaji") or "Unknown"
            year      = (item.get("startDate") or {}).get("year") or ""
            fmt       = (item.get("format") or "").replace("_", " ").title()
            btn_label = f"{label} ({fmt}{', ' + str(year) if year else ''})"
            rows.append([InlineKeyboardButton(
                btn_label[:60],
                callback_data=f"al:{al_type}:{item_id}",
            )])
        rows.append([InlineKeyboardButton("✖ Close", callback_data="al:close")])
        return rows

    async def cmd_anime(self, _, msg: Message):
        if not await self._auth(msg):
            return
        q = " ".join(msg.command[1:]).strip()
        if not q:
            await msg.reply("Usage: /anime &lt;name&gt;", parse_mode=enums.ParseMode.HTML)
            return

        wait = await msg.reply(
            f"Searching anime: <i>{q}</i>\u2026", parse_mode=enums.ParseMode.HTML
        )
        results = await self.al_api.search_anime_list(q)
        try:
            await wait.delete()
        except Exception:
            pass

        if not results:
            await msg.reply("\u274c No anime found.", parse_mode=enums.ParseMode.HTML)
            return

        # Cache results so the Back button can rebuild the list
        cache_key = f"al:search:anime:{msg.chat.id}"
        await self.db.set_cache(cache_key, {"q": q, "results": results[:8]}, ttl=1800)

        rows = self._al_search_rows(results[:8], "anime")
        await msg.reply(
            f"\U0001f50d <b>Results for \"{q}\"</b>\nTap a title to view details:",
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=enums.ParseMode.HTML,
        )

    async def _send_anime_card(self, target, r: dict, search_msg=None, al_type: str = "anime"):
        """Send anime detail card as a new message, deleting search_msg if provided."""
        t     = r.get("title", {})
        title = t.get("english") or t.get("romaji") or "Unknown"
        nat   = t.get("native") or ""
        sd    = r.get("startDate") or {}
        start = (
            f"{sd['year']}-{int(sd.get('month') or 0):02}-{int(sd.get('day') or 0):02}"
            if sd.get("year") else "?"
        )
        cap = (
            f"<b>{title}</b>" + (f" (<i>{nat}</i>)" if nat else "") + "\n\n"
            f"\U0001f4fa Episodes: {r.get('episodes') or '?'}\n"
            f"\U0001f4e1 Status: {al_status(r.get('status', ''))}\n"
            f"\u2b50 Score: {r.get('averageScore') or 'N/A'}/100\n"
            f"\U0001f3ad Genres: {', '.join(r.get('genres') or [])}\n"
            f"\U0001f3a8 Studio: "
            f"{', '.join(n['name'] for n in (r.get('studios') or {}).get('nodes', [])) or '?'}\n"
            f"\U0001f4c5 Started: {start}\n\n"
            f"{clean_html(r.get('description') or 'No description.')}\n\n"
            f"<a href=\"{r.get('siteUrl', '')}\">View on AniList</a>"
        )
        thumb = (r.get("coverImage") or {}).get("large")
        media_id = r.get("id", 0)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f517 AniList", url=r.get("siteUrl", ""))],
            [
                InlineKeyboardButton("\u25c0 Back", callback_data=f"al:back:{al_type}:{media_id}"),
                InlineKeyboardButton("\u274c Close", callback_data="al:close"),
            ],
        ])
        # Edit the search-list message in-place so Back can restore it.
        # Fall back to reply+delete if the message can't be edited (e.g. photo msg).
        edited = False
        if search_msg is not None and not thumb:
            try:
                await search_msg.edit_text(
                    cap, reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                edited = True
            except Exception:
                pass
        if not edited:
            if search_msg is not None:
                try:
                    await search_msg.delete()
                except Exception:
                    pass
            if thumb:
                try:
                    await target.reply_photo(
                        photo=thumb, caption=cap,
                        reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                    )
                    return
                except Exception:
                    pass
            await target.reply(cap, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    async def _send_manga_card(self, target, r: dict, search_msg=None, al_type: str = "manga"):
        """Send manga detail card as a new message, deleting search_msg if provided."""
        t     = r.get("title", {})
        title = t.get("english") or t.get("romaji") or "Unknown"
        nat   = t.get("native") or ""
        sd    = r.get("startDate") or {}
        start = (
            f"{sd['year']}-{int(sd.get('month') or 0):02}-{int(sd.get('day') or 0):02}"
            if sd.get("year") else "?"
        )
        staff = ", ".join(
            n["name"]["full"] for n in (r.get("staff") or {}).get("nodes", [])[:3]
        )
        cap = (
            f"<b>{title}</b>" + (f" (<i>{nat}</i>)" if nat else "") + "\n\n"
            f"\U0001f4d6 Chapters: {r.get('chapters') or '?'}\n"
            f"\U0001f4da Volumes: {r.get('volumes') or '?'}\n"
            f"\U0001f4e1 Status: {al_status(r.get('status', ''))}\n"
            f"\u2b50 Score: {r.get('averageScore') or 'N/A'}/100\n"
            f"\U0001f3ad Genres: {', '.join(r.get('genres') or [])}\n"
            f"\u270d Author: {staff or '?'}\n"
            f"\U0001f4c5 Started: {start}\n\n"
            f"{clean_html(r.get('description') or 'No description.')}\n\n"
            f"<a href=\"{r.get('siteUrl', '')}\">View on AniList</a>"
        )
        thumb = (r.get("coverImage") or {}).get("large")
        media_id = r.get("id", 0)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f517 AniList", url=r.get("siteUrl", ""))],
            [
                InlineKeyboardButton("\u25c0 Back", callback_data=f"al:back:{al_type}:{media_id}"),
                InlineKeyboardButton("\u274c Close", callback_data="al:close"),
            ],
        ])
        # Edit the search-list message in-place so Back can restore it.
        edited = False
        if search_msg is not None and not thumb:
            try:
                await search_msg.edit_text(
                    cap, reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                edited = True
            except Exception:
                pass
        if not edited:
            if search_msg is not None:
                try:
                    await search_msg.delete()
                except Exception:
                    pass
            if thumb:
                try:
                    await target.reply_photo(
                        photo=thumb, caption=cap,
                        reply_markup=kb, parse_mode=enums.ParseMode.HTML,
                    )
                    return
                except Exception:
                    pass
            await target.reply(cap, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    async def cmd_manga(self, _, msg: Message):
        if not await self._auth(msg):
            return
        q = " ".join(msg.command[1:]).strip()
        if not q:
            await msg.reply("Usage: /manga &lt;name&gt;", parse_mode=enums.ParseMode.HTML)
            return

        wait = await msg.reply(
            f"Searching manga: <i>{q}</i>\u2026", parse_mode=enums.ParseMode.HTML
        )
        results = await self.al_api.search_manga_list(q)
        try:
            await wait.delete()
        except Exception:
            pass

        if not results:
            await msg.reply("\u274c No manga found.", parse_mode=enums.ParseMode.HTML)
            return

        # Cache results so the Back button can rebuild the list
        cache_key = f"al:search:manga:{msg.chat.id}"
        await self.db.set_cache(cache_key, {"q": q, "results": results[:8]}, ttl=1800)

        rows = self._al_search_rows(results[:8], "manga")
        await msg.reply(
            f"\U0001f50d <b>Results for \"{q}\"</b>\nTap a title to view details:",
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_reload(self, _, msg: Message):
        if not await self._is_bot_admin(msg):
            return
        await msg.reply("\U0001f504 Reloading schedule\u2026")
        # Drop only schedule/timetable cache — preserve nyaa_seen and nyaa_watermark
        # to prevent re-sending old torrents after reload
        await self.db.cache.delete_many({
            "key": {"$not": {"$regex": "^nyaa"}}
        })
        await self.sched.build_all_jobs()
        await msg.reply("\u2705 Done.")

    async def cmd_stats(self, _, msg: Message):
        if not await self._is_bot_admin(msg):
            return
        s = await self.db.get_stats()
        await msg.reply(
            "<b>\U0001f4ca Bot Stats</b>\n\n"
            f"Token Set:        {'Yes' if AS_TOKEN else 'NO — set ANIMESCHEDULE_TOKEN!'}\n"
            f"Scrape Calls:     {s.get('scrape_calls', 0)}\n"
            f"Scrape Fallbacks: {s.get('scrape_fallbacks', 0)}\n"
            f"AL API Calls:     {s.get('al_api_calls', 0)}\n"
            f"Cache Hits:       {s.get('cache_hits', 0)}\n"
            f"Season Scrapes:   {s.get('season_scrape_calls', 0)}\n"
            f"Season Cache:     {s.get('season_cache_hits', 0)}\n"
            f"Sched Jobs:       {len(self.sched.scheduler.get_jobs())}\n"
            f"Auth Groups:      {await self.db.count_auth()}\n"
            f"Subscribed:       {await self.db.count('chats')}\n"
            f"Job Docs:         {await self.db.count('jobs')}\n"
            f"Cache Docs:       {await self.db.count('cache')}",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_broadcast(self, _, msg: Message):
        if not await self._is_bot_admin(msg):
            return
        text = " ".join(msg.command[1:]).strip()
        if not text:
            await msg.reply("Usage: /broadcast &lt;message&gt;", parse_mode=enums.ParseMode.HTML)
            return
        chats = await self.db.all_subscribed_chats()
        sent  = 0
        for cid in chats:
            try:
                await self.app.send_message(cid, text)
                sent += 1
            except Exception:
                pass
        await msg.reply(f"\U0001f4e2 Sent to {sent}/{len(chats)} chats.")

    async def cmd_addadmin(self, _, msg: Message):
        """
        /addadmin <user_id> — add a dynamic bot admin (env ADMIN_IDS only)
        """
        if not (msg.from_user and msg.from_user.id in ADMIN_IDS):
            await msg.reply("⛔ Only env-level admins can add new admins.")
            return
        args = msg.command[1:]
        if not args or not args[0].lstrip("-").isdigit():
            await msg.reply("Usage: <code>/addadmin &lt;user_id&gt;</code>", parse_mode=enums.ParseMode.HTML)
            return
        uid = int(args[0])
        await self.db.add_admin(uid)
        await msg.reply(f"✅ <code>{uid}</code> added as bot admin.", parse_mode=enums.ParseMode.HTML)

    async def cmd_remadmin(self, _, msg: Message):
        """
        /remadmin <user_id> — remove a dynamic bot admin
        """
        if not (msg.from_user and msg.from_user.id in ADMIN_IDS):
            await msg.reply("⛔ Only env-level admins can remove admins.")
            return
        args = msg.command[1:]
        if not args or not args[0].lstrip("-").isdigit():
            await msg.reply("Usage: <code>/remadmin &lt;user_id&gt;</code>", parse_mode=enums.ParseMode.HTML)
            return
        uid = int(args[0])
        await self.db.remove_admin(uid)
        await msg.reply(f"✅ <code>{uid}</code> removed from bot admins.", parse_mode=enums.ParseMode.HTML)

    async def cmd_admins(self, _, msg: Message):
        """
        /admins — list all bot admins (env + dynamic DB)
        """
        if not await self._is_bot_admin(msg):
            return
        env_admins = ADMIN_IDS
        db_admins  = await self.db.get_dynamic_admins()
        # Merge, marking source
        all_ids    = {uid: "env" for uid in env_admins}
        for uid in db_admins:
            if uid not in all_ids:
                all_ids[uid] = "db"
            else:
                all_ids[uid] = "env+db"

        if not all_ids:
            await msg.reply("No admins configured.")
            return

        lines = [f"<code>{uid}</code> — {src}" for uid, src in all_ids.items()]
        await msg.reply(
            f"🛡 <b>Bot Admins ({len(all_ids)})</b>\n\n" + "\n".join(lines),
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_grouplist(self, _, msg: Message):
        """
        /grouplist — list all authorized groups with their chat IDs
        """
        if not await self._is_bot_admin(msg):
            return
        groups = await self.db.all_authorized_groups()
        if not groups:
            await msg.reply("No authorized groups.")
            return
        lines = [f"<code>{g['chat_id']}</code>" for g in groups]
        await msg.reply(
            f"👥 <b>Authorized Groups ({len(groups)})</b>\n\n" + "\n".join(lines),
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_users(self, _, msg: Message):
        """
        /users — show total user and group counts
        """
        if not await self._is_bot_admin(msg):
            return
        total_chats  = await self.db.count("chats")
        subscribed   = len(await self.db.all_subscribed_chats())
        auth_groups  = len(await self.db.all_authorized_groups())
        private_users = await self.db.count_private_users()
        await msg.reply(
            "👤 <b>Users &amp; Groups</b>\n\n"
            f"Private users:     <b>{private_users}</b>\n"
            f"Auth groups:       <b>{auth_groups}</b>\n"
            f"Subscribed chats:  <b>{subscribed}</b>\n"
            f"Total chat docs:   <b>{total_chats}</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_restart(self, _, msg: Message):
        """
        /restart — gracefully restart the bot process
        """
        if not await self._is_bot_admin(msg):
            return
        await msg.reply("🔄 Restarting…")
        import os, sys
        # Re-exec the current process
        os.execv(sys.executable, [sys.executable] + sys.argv)

    async def cmd_filter(self, _, msg: Message):
        if not await self._auth(msg):
            return
        flt = await self.db.get_filter(msg.chat.id)
        await msg.reply(
            self._filter_text(flt),
            reply_markup=self._kb_filter(flt, section="main"),
            parse_mode=enums.ParseMode.HTML,
        )

    async def cmd_season(self, _, msg: Message):
        if not await self._auth(msg):
            return
        parts = msg.command[1:]
        if len(parts) < 2:
            await msg.reply(
                "Usage: /season &lt;year&gt; &lt;season&gt;\n"
                "Seasons: winter, spring, summer, fall\n"
                "Example: /season 2025 spring",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        try:
            year = int(parts[0])
        except ValueError:
            await msg.reply("❌ Invalid year.", parse_mode=enums.ParseMode.HTML)
            return
        season = parts[1].lower().strip()
        if season not in SeasonScraper.VALID_SEASONS:
            await msg.reply(
                f"❌ Invalid season. Choose from: {', '.join(SeasonScraper.VALID_SEASONS)}",
                parse_mode=enums.ParseMode.HTML,
            )
            return
        wait = await msg.reply(f"⏳ Fetching {season.capitalize()} {year} anime…")
        titles = await self.season.get_season(year, season)
        try:
            await wait.delete()
        except Exception:
            pass
        if not titles:
            await msg.reply("❌ No data found for that season.", parse_mode=enums.ParseMode.HTML)
            return
        text, total_pages = SeasonScraper.paginate(titles, year, season, page=0)
        kb = self._kb_season(year, season, 0, total_pages)
        await msg.reply(text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)
