# 🍃 Naruto Timekeeper

> A Telegram bot for tracking weekly anime schedules — filter by type, set reminders, and never miss an episode.

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11+-blue?style=flat-square&logo=python)
![Pyrogram](https://img.shields.io/badge/Pyrogram-2.x-green?style=flat-square)
![MongoDB](https://img.shields.io/badge/MongoDB-Motor-brightgreen?style=flat-square&logo=mongodb)
![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)

</div>

---

## ✨ Features

- 📅 **Weekly Anime Schedule** — Browse anime airing by day of the week, powered by [AnimeSchedule.net](https://animeschedule.net)
- 🎨 **Filter Settings** — Show/hide Raw, Sub, or Dub. Control hide rules, time format, images, Donghua, and sort order
- 🎛 **Mode Override** — Quickly switch between Raw Only, Sub Only, Dub Only, or All Versions
- 🔔 **Episode Reminders** — Subscribe to get notified the moment an episode airs
- 🔍 **Anime & Manga Search** — Search AniList for info on any anime or manga
- 📊 **Weekly Overview** — See a count of airing anime for each day at a glance
- 🔐 **Group Authorization** — Bot only works in groups that a group admin has authorized
- 🛠 **Admin Tools** — Broadcast messages, view stats, reload the schedule cache

---

## 🤖 Commands

### User Commands
| Command | Description |
|---|---|
| `/start` | Welcome screen with quick buttons |
| `/help` | Full command reference |
| `/settings` | Open the settings panel |
| `/filter` | Edit schedule display filters |
| `/mode [raw\|sub\|dub\|all]` | Set version mode |
| `/anime <name>` | Search anime on AniList |
| `/manga <name>` | Search manga on AniList |

### Group Admin Commands
| Command | Description |
|---|---|
| `/auth` | Authorize this group to use the bot |
| `/deauth` | Remove group authorization |

### Bot Admin Commands
| Command | Description |
|---|---|
| `/reload` | Force refresh the anime schedule cache |
| `/stats` | View bot usage statistics |
| `/broadcast <message>` | Send a message to all subscribed chats |

---

## ⚙️ Environment Variables

Create a `.env` file or set these in your hosting platform (e.g. Heroku):

| Variable | Required | Description |
|---|---|---|
| `BOT_TOKEN` | ✅ | Your Telegram bot token from [@BotFather](https://t.me/BotFather) |
| `API_ID` | ✅ | Telegram API ID from [my.telegram.org](https://my.telegram.org) |
| `API_HASH` | ✅ | Telegram API Hash from [my.telegram.org](https://my.telegram.org) |
| `MONGO_URI` | ✅ | MongoDB connection string |
| `ADMIN_IDS` | ✅ | Comma-separated Telegram user IDs for bot admins (e.g. `123456,789012`) |
| `TIMEZONE` | ❌ | Timezone for schedule display (default: `Asia/Kolkata`) |
| `ANIMESCHEDULE_TOKEN` | ❌ | API token for [AnimeSchedule.net](https://animeschedule.net) (optional, increases rate limits) |
| `BOT_IMAGE_URL` | ❌ | URL of an image to show on `/start` and `/help` |
| `PORT` | ❌ | Health check server port (default: `8000`) |

---

## 🚀 Setup & Deployment

### Prerequisites
- Python 3.11+
- MongoDB instance (local or [MongoDB Atlas](https://www.mongodb.com/atlas))
- Telegram Bot Token, API ID & API Hash

### Install Dependencies

```bash
pip install pyrogram motor httpx aiohttp apscheduler pymongo
```

### Run Locally

```bash
# Clone the repo
git clone https://github.com/yourusername/naruto-timekeeper.git
cd naruto-timekeeper

# Set environment variables
export BOT_TOKEN="your_token"
export API_ID="your_api_id"
export API_HASH="your_api_hash"
export MONGO_URI="your_mongo_uri"
export ADMIN_IDS="your_telegram_id"

# Run
python bot.py
```

### Deploy to Heroku

```bash
heroku create your-app-name
heroku config:set BOT_TOKEN="..." API_ID="..." API_HASH="..." MONGO_URI="..." ADMIN_IDS="..."
git push heroku main
```

---

## 🎨 Filter System

The filter panel (accessible via `/filter` or Settings → Filter Settings) is split into two pages:

**Page 1 — Air & Hide Rules**
- **Air Type** — Toggle Raw / Sub / Dub visibility
- **Hide Raw When** — Hide raw if Sub or Dub exists
- **Hide Sub When** — Hide sub if Dub or Raw exists
- **Hide Dub When** — Hide dub if Sub or Raw exists

**Page 2 — Display Options**
- **Time Format** — 12H or 24H
- **Images** — Show or hide anime thumbnails in schedule
- **Donghua** — Show or hide Chinese anime
- **Sort By** — A-Z / Time / Popularity / Unaired

> ⚠️ If a **Mode** is active (e.g. Sub Only), it overrides the Air Type filter. Use `/mode all` to go back to manual filter control.

---

## 🗂 Project Structure

```
bot.py              # Main bot file (single-file architecture)
README.md           # This file
```

### Key Classes

| Class | Role |
|---|---|
| `Database` | MongoDB interface — auth, filters, modes, reminders, cache, stats |
| `AnimeScheduleAPI` | Fetches weekly timetables from animeschedule.net with caching |
| `AniListAPI` | GraphQL search for anime/manga info |
| `ScheduleProcessor` | Filters, deduplicates, sorts, and paginates anime entries |
| `ReminderScheduler` | APScheduler-based episode reminder system |
| `AnimeBot` | Main bot class — commands, callbacks, lifecycle |

---

## 📦 Dependencies

| Package | Purpose |
|---|---|
| `pyrogram` | Telegram MTProto client |
| `motor` | Async MongoDB driver |
| `httpx` | Async HTTP client for API calls |
| `aiohttp` | Health check web server |
| `apscheduler` | Episode reminder scheduling |
| `pymongo` | MongoDB bulk write operations |

---

## 📝 License

MIT License — free to use, modify and distribute.

---

## 💙 Credits

**Created & maintained by [@Naruto0927](https://t.me/Naruto0927)**

> *"I never go back on my word. That's my ninja way."* 🍥
