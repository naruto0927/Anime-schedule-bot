# 🍃 Naruto Timekeeper

> A feature-rich Telegram bot that tracks every anime episode — raw, sub, and dub — the moment it airs.

**Stack:** Pyrogram · Motor (MongoDB) · APScheduler · httpx · AniList GraphQL · animeschedule.net

---

## ✨ Features

| Feature | Details |
|---|---|
| 📅 Weekly schedule | Browse by day, paginated, with Prev / Next week navigation |
| 🔔 Episode reminders | Fire at exact air time via APScheduler date jobs |
| 🗂 Forum topic routing | Assign Telegram forum topics to **Reminders** or **Nyaa** mode |
| 🎛 Per-topic filters | Each rem-topic independently toggles raw / sub / dub delivery |
| 🚫 Donghua filter | Hides Chinese anime from schedule display AND reminders |
| 🌐 Nyaa torrent alerts | Monitors varyg1001, ToonsHub, SubsPlease RSS (1080p) |
| 🔍 AniList search | `/anime` and `/manga` with inline detail cards and working ◀ Back |
| 🌸 Seasonal list | `/season <year> <season>` — MAL TV-New enriched with AniList English titles |

---

## 📁 Project Structure

```
naruto-timekeeper/
├── main.py                        Entry point — asyncio.run(AnimeBot().run())
├── config.py                      All env vars, constants, and logging setup
├── requirements.txt
├── .env.example
│
├── bot/
│   ├── core.py                    AnimeBot class — lifecycle, _register(), run()
│   ├── commands.py                All /command handlers
│   ├── callbacks.py               Inline-keyboard callback router
│   └── keyboards.py               All InlineKeyboardMarkup builders
│
├── scrapers/
│   ├── animeschedule.py           animeschedule.net v3 API + HTML fallback
│   ├── schedule_processor.py      Merge timetable entries → Telegram pages
│   ├── anilist.py                 AniList GraphQL client
│   ├── season.py                  MAL seasonal scraper + AniList enrichment
│   └── nyaa.py                    RSS torrent monitor (watermark dedup)
│
└── utils/
    ├── database.py                Motor async MongoDB data-access layer
    ├── filters.py                 ChatFilter model (air types, streams, donghua)
    ├── helpers.py                 parse_dt, fmt_time, clean_html, al_status
    ├── health.py                  aiohttp health-check server (GET / and /health)
    └── scheduler.py               APScheduler job manager + _send_reminder
```

---

## 🚀 Setup

### 1. Clone & install

```bash
git clone https://github.com/yourname/naruto-timekeeper.git
cd naruto-timekeeper
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Fill in all required values
```

| Variable | Required | Description |
|---|---|---|
| `BOT_TOKEN` | ✅ | From [@BotFather](https://t.me/BotFather) |
| `API_ID` | ✅ | From [my.telegram.org](https://my.telegram.org) |
| `API_HASH` | ✅ | From [my.telegram.org](https://my.telegram.org) |
| `MONGO_URI` | ✅ | MongoDB connection string |
| `ADMIN_IDS` | ✅ | Comma-separated Telegram user IDs |
| `ANIMESCHEDULE_TOKEN` | ✅ | Free — [animeschedule.net/users/settings/api](https://animeschedule.net) |
| `TIMEZONE` | ☑ | Default: `Asia/Kolkata` |
| `PORT` | ☑ | Health-check port. Default: `8000` |
| `BOT_IMAGE_URL` | ☑ | Banner image URL shown in /start |
| `NYAA_POLL_INTERVAL` | ☑ | Nyaa poll interval in seconds. Default: `300` |

### 3. Run

```bash
python main.py
```

---

## 🤖 Commands

### User Commands
| Command | Description |
|---|---|
| `/start` | Welcome screen |
| `/settings` | Full settings panel |
| `/anime <n>` | Search anime via AniList |
| `/manga <n>` | Search manga via AniList |
| `/filter` | Manage schedule filters |
| `/season <year> <season>` | Seasonal anime list (e.g. `/season 2025 spring`) |
| `/help` | Show help |

### Group Admin Commands
| Command | Description |
|---|---|
| `/auth` | Authorize this group |
| `/deauth` | Deauthorize this group |
| `/mode` | Open topic settings (run inside a forum topic) |
| `/mode <chat_id>\|<topic_id>` | Open topic settings by explicit ID |

### Bot Admin Commands
| Command | Description |
|---|---|
| `/reload` | Force schedule refresh |
| `/stats` | Usage statistics |
| `/broadcast <msg>` | Send to all subscribed chats |
| `/addadmin <id>` | Add bot admin |
| `/remadmin <id>` | Remove bot admin |
| `/admins` | List all admins |
| `/grouplist` | List authorized groups |
| `/users` | User & group counts |
| `/restart` | Restart bot process |

---

## 🗂 Forum Topic Modes

Run `/mode` inside a forum topic to assign it a role:

| Mode | Description |
|---|---|
| 📅 **Reminders** | Episode alerts. Per-topic raw / sub / dub toggles. Optional schedule filter. |
| 🌐 **Nyaa** | 1080p torrent alerts. Per-topic varyg1001 / ToonsHub / SubsPlease toggles. |

---

## 🏗 Architecture Notes

**Mixin inheritance** — `AnimeBot` inherits from `KeyboardsMixin`, `CommandsMixin`, `CallbacksMixin`. Each concern is isolated in its own file while `self.db`, `self.app`, etc. are shared naturally through the class.

**One entry per air type** — animeschedule.net returns separate entries for raw, sub, and dub. `ScheduleProcessor` merges raw+sub into one block per day; dub always gets its own slot.

**Watermark deduplication** — Nyaa RSS polling uses MongoDB-persisted pubDate watermarks. No torrent is ever sent twice across restarts. First run seeds silently (no backlog).

**Cache-first scraping** — Timetable and AniList data cached 6 hours in MongoDB. Stale cache served on network failure so the bot never crashes.

**Full-defaults merge on rem/nyaa configs** — All three toggle keys (`show_raw`, `show_sub`, `show_dub`) are always written together. Prevents partial-save bugs where a missing key silently defaults to "always send".

**Donghua filtering in three layers** — `ScheduleProcessor.process()` (data build), `day_pages()` (schedule display rendering), and `_send_reminder()` (reminder delivery). Toggling "Hide Donghua" suppresses those shows completely everywhere.

---

## 📄 License

MIT
