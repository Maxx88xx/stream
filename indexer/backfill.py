"""One-off / resumable catalog backfill:
  1. launches: TokenLaunched logs from START_BLOCK to head (public RPC, 100k
     chunks, halving on the 10k cap), inserted as they come, progress saved;
  2. names: ERC-20 name()/symbol() for every unnamed token (Alchemy multicall,
     600 per call), newest first so the visible part of the catalog fills first;
  3. meta: launch calldata (image/description/socials) for tokens without it
     (Alchemy batches of 50), newest first: a slow background pass.
Run: `python -m indexer.backfill [launches|names|meta|all]`. Idempotent.
"""
from __future__ import annotations

import sys
import time

from . import chain, db

START_BLOCK = 26_000_000        # first PonsV2 launches sit around block 27M (probed 2026-09-06)
CHUNK = 100_000


def launches(c) -> None:
    head = chain.block_number()
    frm = int(db.get_progress(c, "scanned_to", START_BLOCK - 1)) + 1
    print(f"[backfill] launches {frm} → {head} ({head - frm} blocks)")
    t0 = time.time()
    total = 0
    while frm <= head:
        to = min(head, frm + CHUNK - 1)
        try:
            logs = chain.launch_logs(frm, to)
        except chain.RpcError as exc:                     # the public RPC has bad minutes; wait it out
            print(f"[backfill]   {frm}-{to}: {str(exc)[:80]}: retrying in 15s")
            time.sleep(15)
            continue
        rows = [chain.parse_launch_log(lg) for lg in logs]
        n = db.insert_launches(c, rows)
        db.set_progress(c, "scanned_to", to)
        c.commit()
        total += n
        if logs:
            print(f"[backfill]   {frm}-{to}: {len(logs)} launches (+{n} new), total {total}, {time.time() - t0:.0f}s")
        frm = to + 1
        time.sleep(0.6)          # the public RPC 429s on bursts
    print(f"[backfill] launches done: +{total} in {time.time() - t0:.0f}s")


def names(c, batch: int = 600) -> None:
    t0 = time.time()
    done = 0
    while True:
        rows = c.execute("SELECT address FROM tokens WHERE names_done=0 ORDER BY block DESC LIMIT ?", (batch,)).fetchall()
        if not rows:
            break
        toks = [r["address"] for r in rows]
        named = chain.names_symbols(toks)
        db.set_names(c, named)
        c.commit()
        done += len(toks)
        print(f"[backfill]   names +{len(toks)} (total {done}, {time.time() - t0:.0f}s)")
        time.sleep(0.2)
    print(f"[backfill] names done: {done} in {time.time() - t0:.0f}s")


def meta(c, batch: int = 15, limit: int | None = None) -> None:
    """15 tx/s ≈ 255 CU/s: stays under Alchemy's free-tier 330 CU/s so the
    live loop and the web keep getting answers instead of 429s."""
    t0 = time.time()
    done = 0
    while limit is None or done < limit:
        rows = c.execute("SELECT address, tx FROM tokens WHERE meta_done=0 ORDER BY block DESC LIMIT ?", (batch,)).fetchall()
        if not rows:
            break
        t1 = time.time()
        inputs = chain.fetch_tx_inputs([r["tx"] for r in rows])
        for r in rows:
            db.set_meta(c, r["address"], chain.decode_launch_input(inputs.get(r["tx"], "")))
        c.commit()
        done += len(rows)
        time.sleep(max(0.0, 1.0 - (time.time() - t1)))
        if done % 3000 < batch:
            print(f"[backfill]   meta {done} ({time.time() - t0:.0f}s)")
    print(f"[backfill] meta done: {done} in {time.time() - t0:.0f}s")


def main() -> None:
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    c = db.connect()
    if what in ("launches", "all"):
        launches(c)
    if what in ("names", "all"):
        names(c)
    if what in ("meta", "all"):
        meta(c)
    print("[backfill] stats:", db.stats(c))


if __name__ == "__main__":
    main()
