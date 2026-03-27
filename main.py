"""
main.py — Entry point for Naruto Timekeeper.

    python main.py
"""

import asyncio
from bot.core import AnimeBot


async def main() -> None:
    await AnimeBot().run()


if __name__ == "__main__":
    asyncio.run(main())
