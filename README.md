# stream (provisional name)

Livestreams for PONS coins on Robinhood Chain — pump.fun-style live page for any coin launched on PONS.
The coin's deployer wallet signs in (personal_sign, no gas) and goes live from the browser (WebRTC/WHIP) or OBS (RTMPS key).
Viewers get a Cloudflare Stream player, live chat, and a market-cap candle chart built from the bonding-curve trades.

- `indexer/` — catalog of every PONS launch straight from the chain (public RPC for logs, Alchemy for multicall/batch/WSS), SQLite + FTS5 search by name.
- `server.py` — aiohttp app: auth, catalog API, Cloudflare live inputs, chat WebSocket, candles, image cache.
- `frontend/` — single-page UI.

Env: `ALCHEMY_KEY`, `CF_ACCOUNT_ID`, `CF_API_TOKEN`, `SESSION_SECRET`, `ADMIN_SECRET`, `DATA_DIR` (volume, default `/data`), optional `SITE_NAME`, `PRIVY_APP_ID`, `BACKFILL=0`.
