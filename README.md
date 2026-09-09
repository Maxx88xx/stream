# Motion

Livestream PONS coins in seconds. Every coin launched on PONS (Robinhood Chain) gets a page with the creator on camera,
the market-cap chart underneath and a wallet-signed chat. Only the coin's deployer wallet can go live (Privy sign-in,
one signed message, no gas); viewers need nothing. Broadcast from the browser (WebRTC) or OBS (RTMP key), playback is
sub-second via LiveKit.

- Catalog of all PONS coins, indexed from chain, new launches appear instantly (Alchemy WSS).
- Chat moderation like pump.fun: dev bans/unbans and appoints mods, holders-only mode.
- No buying or selling here; every coin page links to PONS.

Run locally: `.venv/bin/python server.py` (see `.claude/launch.json`). Deploy: Railway project `stream`, `railway up -d`.
Palette notes and rollback: `PALETTE.md`. Colour catalogue for the accent: `/preview`.
