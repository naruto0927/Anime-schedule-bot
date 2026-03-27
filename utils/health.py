"""
utils/health.py — Lightweight aiohttp health-check server.
Exposes GET / and GET /health for uptime monitors.
"""

from aiohttp import web
from config import PORT, logger

async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", lambda _: web.Response(text="OK"))
    app.router.add_get("/health", lambda _: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logger.info("Health server on port %s", PORT)
    return runner
