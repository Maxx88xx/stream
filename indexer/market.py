"""Market data straight from the chain: curve reserves → price / market cap,
and CurveBuy/CurveSell logs → trades → candles for the chart.

Prices are in the launch's QUOTE asset (USDG = 6 dec ≈ $1, native ETH = 18
dec). USD needs only the ETH rate (Coinbase spot, cached 60 s)."""
from __future__ import annotations

import json
import time
import urllib.request

from . import chain, db

SUPPLY = 10 ** 27                           # PONS v2 launch config: 1e9 tokens, 18 dec
BUY_TOPIC = "0xec36bf571f136799e8dc0b0b8bea4b04d8bd3d43de838aab0d5fc21d4cbfc455"
SELL_TOPIC = "0x8113d738abdcb6b38357e9d53a54a7157861a09031b453651f0fe7fe151f59df"
BLOCK_SECONDS = 0.1008                      # measured: 857k blocks/day
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
QUOTE_DECIMALS = {USDG: 6, "": 18, "0x0000000000000000000000000000000000000000": 18}
_eth_usd = {"v": 0.0, "ts": 0.0}


def eth_usd() -> float:
    if time.time() - _eth_usd["ts"] < 60 and _eth_usd["v"]:
        return _eth_usd["v"]
    try:
        with urllib.request.urlopen("https://api.coinbase.com/v2/prices/ETH-USD/spot", timeout=8) as r:
            _eth_usd.update(v=float(json.load(r)["data"]["amount"]), ts=time.time())
    except Exception:  # noqa: BLE001
        pass
    return _eth_usd["v"]


_pair_cache: dict = {}          # pair_token → {"dec", "sym", "usd", "ts"}
_PAIR_TTL = 300


def _dexscreener_usd(token: str) -> float:
    """USD price of a Robinhood-chain token from its deepest USDG/ETH pool."""
    try:
        req = urllib.request.Request(f"https://api.dexscreener.com/latest/dex/tokens/{token}", headers={"User-Agent": "Mozilla/5.0 pons.live"})
        with urllib.request.urlopen(req, timeout=8) as r:
            pairs = (json.load(r).get("pairs") or [])
    except Exception:  # noqa: BLE001
        return 0.0
    best, liq = 0.0, -1.0
    for pr in pairs:
        if pr.get("chainId") != "robinhood" or (pr.get("baseToken") or {}).get("address", "").lower() != token:
            continue
        L = float((pr.get("liquidity") or {}).get("usd") or 0)
        if L > liq and pr.get("priceUsd"):
            best, liq = float(pr["priceUsd"]), L
    return best


def quote_info(pair_token: str) -> tuple[int, float, str]:
    """(decimals, usd_per_unit, symbol) for the quote asset of a launch.
    Native ETH → Coinbase; USDG → $1; anything else (Robinhood tokenized
    stocks: NVDA, SPCX, SPY…) → DexScreener price of that token, cached 5 min.
    Does network I/O — call it off the event loop."""
    p = (pair_token or "").lower()
    if p in ("", "0x0000000000000000000000000000000000000000"):
        return 18, eth_usd(), "ETH"
    if p == USDG:
        return 6, 1.0, "USDG"
    e = _pair_cache.get(p)
    if e and time.time() - e["ts"] < _PAIR_TTL:
        return e["dec"], e["usd"], e["sym"]
    if not e:
        try:
            sym = chain.names_symbols([p]).get(p, ("", "?"))[1] or "?"
            dec = chain.decimals(p)
        except Exception:  # noqa: BLE001
            sym, dec = "?", 18
        e = {"dec": dec, "sym": sym, "usd": 0.0, "ts": 0.0}
    usd = _dexscreener_usd(p)
    e.update(usd=usd or e["usd"], ts=time.time())
    _pair_cache[p] = e
    return e["dec"], e["usd"], e["sym"]


def enrich_market(rows: list) -> list:
    """Attach price_quote, mcap_usd, graduated to catalog rows (multicall)."""
    curves = [r["curve"] for r in rows]
    pairs = {(r.get("pair_token") or "").lower() for r in rows}
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(max(1, min(8, len(pairs)))) as ex:      # warm the quote cache for every distinct pair at once
        list(ex.map(quote_info, pairs))
    states = chain.curve_states(curves)
    for r in rows:
        q, t, grad = states.get(r["curve"], (0, 0, False))
        dec, usd, sym = quote_info(r.get("pair_token") or "")
        price = (q / 10 ** dec) / (t / 1e18) if q and t else 0.0
        r["price_quote"] = price
        r["quote_symbol"] = sym
        r["mcap_usd"] = price * 1e9 * usd
        r["graduated"] = bool(grad)
    return rows


INITIAL_SPAN = 2_600_000                    # first look at an old curve: last ~3 days of trades
CHUNK = 100_000                             # public RPC getLogs range cap


def trade_start(c, curve: str, head: int) -> int | None:
    """First block still unscanned for this curve (None = unknown curve)."""
    row = c.execute("SELECT block FROM tokens WHERE curve=?", (curve,)).fetchone()
    if not row:
        return None
    pr = c.execute("SELECT block FROM trades_progress WHERE curve=?", (curve,)).fetchone()
    return (pr["block"] + 1) if pr else max(row["block"], head - INITIAL_SPAN)


def fetch_trades(curve: str, frm: int, head: int, max_chunks: int = 12) -> tuple[list, int]:
    """CurveBuy/CurveSell logs in [frm, head] from the public RPC, at most
    `max_chunks` × 100k blocks per call (the caller comes back for the rest).
    Network only — no database handle, so it never blocks the web loop.
    Returns (rows, last_block_scanned)."""
    rows, to = [], frm - 1
    for _ in range(max_chunks):
        if frm > head:
            break
        to = min(head, frm + CHUNK - 1)
        logs = chain.rpc(chain.PUBLIC_RPC, "eth_getLogs", [{"address": curve, "topics": [[BUY_TOPIC, SELL_TOPIC]],
                                                            "fromBlock": hex(frm), "toBlock": hex(to)}])
        for lg in logs:
            d = lg["data"][2:]
            w0, w1 = int(d[0:64], 16), int(d[64:128], 16)
            side = "buy" if lg["topics"][0].lower() == BUY_TOPIC else "sell"
            quote, tokens = (w0, w1) if side == "buy" else (w1, w0)
            rows.append((curve, int(lg["blockNumber"], 16), int(lg["logIndex"], 16), side, str(quote), str(tokens)))
        frm = to + 1
    return rows, to


def store_trades(c, curve: str, rows: list, scanned_to: int) -> None:
    if rows:
        c.executemany("INSERT OR IGNORE INTO trades(curve,block,idx,side,quote,tokens) VALUES(?,?,?,?,?,?)", rows)
    c.execute("INSERT INTO trades_progress(curve,block) VALUES(?,?) ON CONFLICT(curve) DO UPDATE SET block=MAX(block, excluded.block)", (curve, scanned_to))
    c.commit()


def candles(c, curve: str, quote: tuple, tf: int = 60, head: int | None = None, limit: int = 300) -> list:
    """OHLC of market cap in USD per `tf` seconds, from stored trades; flat
    candles fill the gaps up to now so the chart shows a timeline, not a dot.
    `quote` = quote_info(pair_token), resolved by the caller (network)."""
    head = head or chain.block_number()
    dec, usd, _ = quote
    now = time.time()
    rows = c.execute("SELECT block, idx, side, quote, tokens FROM trades WHERE curve=? ORDER BY block, idx", (curve,)).fetchall()
    out = []
    for r in rows:
        q, t = int(r["quote"]), int(r["tokens"])
        if not q or not t:
            continue
        price = (q / 10 ** dec) / (t / 1e18) * 1e9 * usd          # market cap in USD
        ts = now - (head - r["block"]) * BLOCK_SECONDS
        bucket = int(ts // tf) * tf
        vol = q / 10 ** dec * usd
        if out and out[-1]["time"] == bucket:
            k = out[-1]
            k["high"] = max(k["high"], price); k["low"] = min(k["low"], price); k["close"] = price; k["volume"] += vol
        else:
            out.append({"time": bucket, "open": out[-1]["close"] if out else price, "high": price, "low": price, "close": price, "volume": vol})
    if out:                                                   # flat fill from the last trade to now
        last = out[-1]
        b = last["time"] + tf
        end = int(now // tf) * tf
        while b <= end and len(out) < limit * 2:
            out.append({"time": b, "open": last["close"], "high": last["close"], "low": last["close"], "close": last["close"], "volume": 0.0})
            b += tf
    return out[-limit:]
