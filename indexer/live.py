"""Instant launches: an Alchemy WebSocket subscription to the core's
TokenLaunched logs. Every event is inserted, named (multicall) and enriched
(launch calldata) at once, then handed to `on_launch` for the web to push to
open pages. No polling anywhere: on a dropped socket we reconnect and read the
missed span with one eth_getLogs from the last block we saw."""
from __future__ import annotations

import asyncio
import json
import time

import websockets

from . import chain, db


async def _enrich(c, rows: list) -> list:
    """Name + metadata for freshly launched tokens; returns the full rows."""
    toks = [r["address"] for r in rows]
    named = await asyncio.to_thread(chain.names_symbols, toks)
    inputs = await asyncio.to_thread(chain.fetch_tx_inputs, [r["tx"] for r in rows])

    def _write():
        db.set_names(c, named)
        for r in rows:
            db.set_meta(c, r["address"], chain.decode_launch_input(inputs.get(r["tx"], "")))
        c.commit()
        return [db.get_token(c, r["address"]) for r in rows]
    return await asyncio.to_thread(_write)


async def run(on_launch, stop: asyncio.Event | None = None) -> None:
    c = db.connect()
    stop = stop or asyncio.Event()
    while not stop.is_set():
        try:
            # catch-up: anything launched since the last block we processed
            last = int(db.get_progress(c, "live_block", 0) or 0)
            head = await asyncio.to_thread(chain.block_number)
            if last and head - last < 200_000:
                missed = await asyncio.to_thread(chain.launch_logs, last + 1, head)
                rows = [chain.parse_launch_log(lg) for lg in missed]
                if rows:
                    await asyncio.to_thread(lambda: (db.insert_launches(c, rows), c.commit()))
                    for t in await _enrich(c, rows):
                        await on_launch(t)
                    print(f"[live] caught up {len(rows)} launches from {last + 1}..{head}")
            await asyncio.to_thread(lambda: (db.set_progress(c, "live_block", head), c.commit()))

            async with websockets.connect(chain.ALCHEMY_WSS, open_timeout=20, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
                                          "params": ["logs", {"address": chain.PONS_CORE, "topics": [chain.LAUNCH_TOPIC]}]}))
                first = json.loads(await asyncio.wait_for(ws.recv(), 15))
                if "result" not in first:
                    raise RuntimeError(f"subscribe failed: {first}")
                print("[live] subscribed to TokenLaunched")
                while not stop.is_set():
                    msg = json.loads(await ws.recv())
                    lg = (msg.get("params") or {}).get("result")
                    if not lg or lg.get("removed"):
                        continue
                    row = chain.parse_launch_log(lg)
                    row["ts"] = int(time.time())
                    t0 = time.time()
                    if await asyncio.to_thread(lambda: (db.insert_launches(c, [row]), c.commit())[0]):
                        try:
                            enriched = await _enrich(c, [row])
                        except Exception as exc:  # noqa: BLE001   # 429 etc.: push it bare, the backfill sweep names it
                            print(f"[live] enrich failed for {row['address']}: {str(exc)[:80]}")
                            enriched = [await asyncio.to_thread(db.get_token, c, row["address"])]
                        for t in enriched:
                            await on_launch(t)
                        print(f"[live] {row['address']} block {row['block']} in {time.time() - t0:.1f}s")
                    await asyncio.to_thread(lambda: (db.set_progress(c, "live_block", row["block"]), c.commit()))
        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[live] socket lost ({type(exc).__name__}: {str(exc)[:80]}), reconnecting in 3s")
            await asyncio.sleep(3)


if __name__ == "__main__":
    async def _print(t):
        print("NEW:", t["symbol"], t["name"], t["address"], t["image"][:50])
    asyncio.run(run(_print))
