"""Plink: livestreams for PONS coins (Robinhood Chain), pump.fun style.

One aiohttp process: the site, the JSON API, the WebSocket (launch feed, chat,
viewer counts, live flags), the Alchemy launch subscription (instant catalog),
the Cloudflare Stream live inputs (browser WHIP + OBS RTMPS), and the chain
market reads (multicall reserves, curve trades → candles).

Auth = wallet signature only (nonce → personal_sign → HMAC session). A stream
can be started only by the wallet that DEPLOYED the coin (from the launch log).
No private keys anywhere in this process.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import sys
import threading
import time
import urllib.request
from pathlib import Path

from aiohttp import WSMsgType, web
from eth_account import Account
from eth_account.messages import encode_defunct

import livekit_api as lk
import engine
from indexer import chain, db, images, live, market

BASE = Path(__file__).resolve().parent
FRONTEND = BASE / "frontend"
PORT = int(os.environ.get("PORT") or 8080)
CF_ACCOUNT = (os.environ.get("CF_ACCOUNT_ID") or "").strip()
CF_TOKEN = (os.environ.get("CF_API_TOKEN") or "").strip()
PRIVY_APP_ID = (os.environ.get("PRIVY_APP_ID") or "").strip()
ADMIN_SECRET = (os.environ.get("ADMIN_SECRET") or "").strip()
SITE_NAME = os.environ.get("SITE_NAME") or "Plink"
_ADDR = re.compile(r"^0x[0-9a-fA-F]{40}$")
NO_CACHE = {"Cache-Control": "no-cache"}
TOKEN_TTL = 7 * 86400

C = db.connect_web()
_lock = threading.Lock()      # sqlite handle shared by the loop + worker threads


def q(sql, args=()):
    with _lock:
        return [dict(r) for r in C.execute(sql, args).fetchall()]


def x(sql, args=()):
    with _lock:
        C.execute(sql, args)
        C.commit()


# ---- sessions (nonce → personal_sign → HMAC token) ----
_nonces: dict = {}


def _norm_addr(a) -> str | None:
    a = (a or "").strip().lower() if isinstance(a, str) else ""
    return a if _ADDR.match(a) else None


def _session_secret() -> bytes:
    env = (os.environ.get("SESSION_SECRET") or "").strip()
    if env:
        return env.encode()
    p = Path(db.DATA_DIR) / "session_secret"
    try:
        s = p.read_text().strip()
        if s:
            return s.encode()
    except OSError:
        pass
    s = secrets.token_hex(32)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(s)
    except OSError:
        pass
    return s.encode()


SECRET = _session_secret()


def _login_message(addr: str, nonce: str) -> str:
    return f"{SITE_NAME} sign-in\n\nwallet: {addr}\nnonce: {nonce}\n\nNo transaction, no gas, no approvals."


def _make_token(addr: str) -> str:
    payload = f"{addr}|{int(time.time()) + TOKEN_TTL}"
    return payload + "|" + hmac.new(SECRET, payload.encode(), hashlib.sha256).hexdigest()


def _check_token(tok: str) -> str | None:
    try:
        addr, exp, sig = tok.split("|")
        want = hmac.new(SECRET, f"{addr}|{exp}".encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, want) or int(exp) < time.time():
            return None
        return _norm_addr(addr)
    except Exception:  # noqa: BLE001
        return None


def _wallet(request) -> str | None:
    auth = request.headers.get("Authorization", "")
    return _check_token(auth[7:]) if auth.startswith("Bearer ") else None


async def h_nonce(request):
    body = await request.json()
    addr = _norm_addr(body.get("address"))
    if not addr:
        return web.json_response({"error": "bad address"}, status=400)
    nonce = secrets.token_hex(16)
    _nonces[addr] = (nonce, time.time() + 300)
    return web.json_response({"message": _login_message(addr, nonce)})


async def h_verify(request):
    body = await request.json()
    addr = _norm_addr(body.get("address"))
    sig = body.get("signature")
    entry = _nonces.pop(addr, None) if addr else None
    if not addr or not isinstance(sig, str) or not entry or entry[1] < time.time():
        return web.json_response({"error": "bad request"}, status=400)
    try:
        rec = Account.recover_message(encode_defunct(text=_login_message(addr, entry[0])), signature=sig)
    except Exception:  # noqa: BLE001
        return web.json_response({"error": "bad signature"}, status=400)
    if rec.lower() != addr:
        return web.json_response({"error": "address mismatch"}, status=401)
    return web.json_response({"token": _make_token(addr), "address": addr})


async def h_me(request):
    w = _wallet(request)
    return web.json_response({"address": w, "coins": [_pub(t) for t in db.by_deployer(C, w)] if w else []})


# ---- catalog ----

_head = {"n": 0}              # latest block, refreshed by the status poller (card ages)


def _pub(t: dict, stream: dict | None = None) -> dict:
    """Public shape of a token row (+ live stream state)."""
    out = {k: t.get(k) for k in ("address", "curve", "deployer", "pair_token", "block", "name", "symbol", "image",
                                  "description", "twitter", "telegram", "website", "price_quote", "quote_symbol",
                                  "mcap_usd", "graduated", "created_ts")}
    s = stream if stream is not None else _stream_row(t["address"])
    out["image"] = f"/img/{t['address']}" if t.get("image") else ""
    out["age"] = max(0, int((_head["n"] - (t.get("block") or 0)) * market.BLOCK_SECONDS)) if _head["n"] else None
    out["live"] = bool(s and s["live"])
    out["title"] = (s or {}).get("title", "")
    out["viewers"] = len(ROOMS.get(t["address"], set()))
    return out


def _stream_row(addr: str):
    r = q("SELECT * FROM streams WHERE address=?", (addr,))
    return r[0] if r else None


async def h_recent(request):
    offset = max(0, int(request.query.get("offset") or 0))
    rows = db.recent(C, max(1, min(48, int(request.query.get("limit") or 48))), offset)
    await asyncio.to_thread(market.enrich_market, rows)
    return web.json_response({"rows": [_pub(t) for t in rows], "stats": db.stats(C)}, headers=NO_CACHE)


async def h_search(request):
    rows = db.search(C, request.query.get("q", ""), max(1, min(30, int(request.query.get("limit") or 30))))
    if not request.query.get("fast"):                      # suggestions skip the on-chain market read
        await asyncio.to_thread(market.enrich_market, rows)
    return web.json_response({"rows": [_pub(t) for t in rows]}, headers=NO_CACHE)


async def h_token(request):
    addr = _norm_addr(request.match_info["addr"])
    t = db.get_token(C, addr) if addr else None
    if not t:
        return web.json_response({"error": "unknown coin"}, status=404)
    await asyncio.to_thread(market.enrich_market, [t])
    s = _stream_row(addr)
    out = _pub(t, s)
    if s:
        out["hls_url"], out["whep_url"], out["input_uid"], out["ingest"] = s["hls_url"], s["whep_url"], s["input_uid"], s["ingest"] if "ingest" in s.keys() else ""
        out["provider"] = _provider(s)
        out["started_ts"] = s["started_ts"] or 0
    return web.json_response(out, headers=NO_CACHE)


async def h_streams(request):
    rows = q("SELECT s.*, t.* FROM streams s JOIN tokens t ON t.address=s.address WHERE s.live=1 ORDER BY s.started_ts DESC")
    await asyncio.to_thread(market.enrich_market, rows)
    out = [_pub(t) for t in rows]
    out.sort(key=lambda r: -r["viewers"])
    return web.json_response({"rows": out}, headers=NO_CACHE)


_candle_locks: dict = {}


async def h_candles(request):
    addr = _norm_addr(request.match_info["addr"])
    t = db.get_token(C, addr) if addr else None
    if not t:
        return web.json_response({"error": "unknown coin"}, status=404)
    tf = int(request.query.get("tf") or 60)
    tf = tf if tf in (5, 15, 60, 300, 900, 3600) else 60
    limit = max(10, min(2000, int(request.query.get("limit") or 300)))
    lock = _candle_locks.setdefault(addr, asyncio.Lock())
    async with lock:
        head, quote = await asyncio.gather(asyncio.to_thread(chain.block_number) if not _head["n"] else asyncio.sleep(0, _head["n"]),
                                           asyncio.to_thread(market.quote_info, t.get("pair_token") or ""))
        with _lock:
            frm = market.trade_start(C, t["curve"], head)
        if frm is not None and frm <= head:
            try:
                rows, to = await asyncio.to_thread(market.fetch_trades, t["curve"], frm, head)   # no db lock while on the network
            except Exception as exc:  # noqa: BLE001
                print(f"[trades] {addr} fetch failed: {str(exc)[:120]}")
                rows, to = [], None
            if to is not None:
                with _lock:
                    market.store_trades(C, t["curve"], rows, to)
        with _lock:
            rows = market.candles(C, t["curve"], quote, tf, head, limit)
            trades = [dict(r) for r in C.execute("SELECT block, side, quote, tokens FROM trades WHERE curve=? ORDER BY block DESC, idx DESC LIMIT 30", (t["curve"],))]
    dec, usd, sym = quote
    for tr in trades:
        tr["quote_units"] = int(tr.pop("quote")) / 10 ** dec
        tr["tokens_units"] = int(tr.pop("tokens")) / 1e18
        tr["usd"] = tr["quote_units"] * usd
        tr["ago"] = int((head - tr["block"]) * market.BLOCK_SECONDS)
    return web.json_response({"candles": rows, "trades": trades, "quote": sym}, headers=NO_CACHE)


# ---- Cloudflare Stream live inputs ----

def cf(method: str, path: str, data=None):
    r = urllib.request.Request(f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT}/stream/live_inputs{path}",
                               method=method, data=json.dumps(data).encode() if data else None,
                               headers={"Authorization": "Bearer " + CF_TOKEN, "Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=30) as resp:
        body = json.load(resp)
    if not body.get("success"):
        raise RuntimeError(str(body.get("errors"))[:200])
    return body["result"]


def _provider(s) -> str:
    if "ingress_id" in s.keys() and s["ingress_id"]:
        return "livekit"
    return "livekit" if (lk.ENABLED and not s["input_uid"]) else "cf"


def _creds(s: dict) -> dict:
    return {"provider": _provider(s), "rtmps_url": s["rtmps_url"], "stream_key": s["stream_key"], "whip_url": s["whip_url"],
            "hls_url": s["hls_url"], "whep_url": s["whep_url"], "title": s["title"], "live": bool(s["live"])}


_start_locks: dict = {}


async def h_stream_start(request):
    w = _wallet(request)
    if not w:
        return web.json_response({"error": "sign in first"}, status=401)
    body = await request.json()
    addr = _norm_addr(body.get("token"))
    t = db.get_token(C, addr) if addr else None
    if not t:
        return web.json_response({"error": "unknown coin"}, status=404)
    if t["deployer"] != w:
        return web.json_response({"error": "only the wallet that launched this coin can stream it"}, status=403)
    if q("SELECT 1 FROM bans WHERE wallet=? AND room='*'", (w,)):
        return web.json_response({"error": "banned"}, status=403)
    if not (CF_ACCOUNT and CF_TOKEN):
        return web.json_response({"error": "streaming not configured"}, status=503)
    title = str(body.get("title") or "")[:80]
    async with _start_locks.setdefault(addr, asyncio.Lock()):      # one live input per coin, even under a click storm
        s = _stream_row(addr)
        mode = str(body.get("mode") or "rtmp")
        if lk.ENABLED:
            if not s:
                x("INSERT OR IGNORE INTO streams(address,input_uid,rtmps_url,stream_key,whip_url,hls_url,whep_url,title,wallet,created_ts) VALUES(?,?,?,?,?,?,?,?,?,?)",
                  (addr, "", "", "", "", "", "", title, w, int(time.time())))
                s = _stream_row(addr)
            if mode == "rtmp" and s["ingress_id"]:
                # the slot may have been reclaimed (cap cleanup, manual delete): a dead id would leave the creator with a dead key
                try:
                    alive = {i.get("ingress_id") for i in await asyncio.to_thread(lk.list_ingress)}
                    if s["ingress_id"] not in alive:
                        x("UPDATE streams SET ingress_id='', rtmps_url='', stream_key='' WHERE address=?", (addr,)); s = _stream_row(addr)
                except Exception as exc:  # noqa: BLE001
                    print(f"[lk] list_ingress failed: {exc}")
            if mode == "rtmp" and not s["ingress_id"]:
                try:
                    ing = await asyncio.to_thread(lk.create_ingress, addr, f"{t['symbol']} {t['name']}")
                except Exception as exc:  # noqa: BLE001
                    ing = None
                    if "resource_exhausted" in str(exc):
                        # the project's ingress cap is hit: drop dead slots (creators who stopped OBS) and try once more
                        freed = await asyncio.to_thread(lk.free_idle_ingress)
                        for fid in freed:
                            x("UPDATE streams SET ingress_id='', rtmps_url='', stream_key='' WHERE ingress_id=?", (fid,))
                        print(f"[lk] ingress cap hit, freed {len(freed)} idle")
                        if freed:
                            try:
                                ing = await asyncio.to_thread(lk.create_ingress, addr, f"{t['symbol']} {t['name']}")
                            except Exception as exc2:  # noqa: BLE001
                                exc = exc2
                    if not ing:
                        print(f"[lk] create_ingress failed: {exc}")
                        msg = "Every OBS slot is taken by a live stream right now. Try again when one ends, or go live from the browser" if "resource_exhausted" in str(exc) else "LiveKit refused the request: check LIVEKIT_* variables"
                        return web.json_response({"error": msg}, status=503)
                x("UPDATE streams SET ingress_id=?, rtmps_url=?, stream_key=? WHERE address=?", (ing["ingress_id"], ing["url"], ing["stream_key"], addr))
                s = _stream_row(addr)
        if not s:
            li = await asyncio.to_thread(cf, "POST", "", {"meta": {"name": f"{t['symbol']} {addr}"}, "recording": {"mode": "automatic"}, "preferLowLatency": False})
            x("INSERT OR IGNORE INTO streams(address,input_uid,rtmps_url,stream_key,whip_url,hls_url,whep_url,title,wallet,created_ts) VALUES(?,?,?,?,?,?,?,?,?,?)",
              (addr, li["uid"], li["rtmps"]["url"], li["rtmps"]["streamKey"], li["webRTC"]["url"], li["playback"]["hls"],
               li["webRTCPlayback"]["url"], title, w, int(time.time())))
            s = _stream_row(addr)
            if s["input_uid"] != li["uid"]:                          # lost a race anyway: drop the extra input
                asyncio.create_task(asyncio.to_thread(cf, "DELETE", f"/{li['uid']}"))
        else:                                                        # older inputs: switch them to Low-Latency HLS too
            asyncio.create_task(asyncio.to_thread(_ensure_ll, s["input_uid"]))
        x("UPDATE streams SET title=?, started_ts=?, ended_ts=0 WHERE address=?", (title, int(time.time()), addr))
        if "holders_only" in body:
            x("INSERT INTO room_settings(room,holders_only) VALUES(?,?) ON CONFLICT(room) DO UPDATE SET holders_only=excluded.holders_only", (addr, 1 if body.get("holders_only") else 0))
            asyncio.create_task(_broadcast_room_state(addr))
        s = _stream_row(addr)
    return web.json_response(_creds(s), headers=NO_CACHE)


_ll_done: set = set()


def _ensure_ll(uid: str) -> None:
    if uid in _ll_done:
        return
    try:
        cf("PUT", f"/{uid}", {"recording": {"mode": "automatic"}, "preferLowLatency": False})   # LL-HLS beta stalls with stock OBS (B-frames, 8 s GOP)
        _ll_done.add(uid)
    except Exception as exc:  # noqa: BLE001
        print(f"[cf] preferLowLatency update failed for {uid}: {exc}")


async def h_stream_creds(request):
    w = _wallet(request)
    addr = _norm_addr(request.query.get("token"))
    s = _stream_row(addr) if addr else None
    if not w or not s or s["wallet"] != w:
        return web.json_response({"error": "not yours"}, status=403)
    return web.json_response(_creds(s), headers=NO_CACHE)


async def h_stream_reset(request):
    """New RTMP credentials for a coin (the old key stops working)."""
    w = _wallet(request)
    body = await request.json()
    addr = _norm_addr(body.get("token"))
    s = _stream_row(addr) if addr else None
    if not w or not s or s["wallet"] != w:
        return web.json_response({"error": "not yours"}, status=403)
    if not (lk.ENABLED and s["ingress_id"]):
        return web.json_response({"error": "not a LiveKit stream"}, status=400)
    t = db.get_token(C, addr)
    async with _start_locks.setdefault(addr, asyncio.Lock()):
        try:
            await asyncio.to_thread(lk.delete_ingress, s["ingress_id"])
        except Exception as exc:  # noqa: BLE001
            print(f"[lk] delete_ingress failed: {str(exc)[:100]}")
        x("UPDATE streams SET ingress_id='', rtmps_url='', stream_key='' WHERE address=?", (addr,))
        try:
            ing = await asyncio.to_thread(lk.create_ingress, addr, f"{t['symbol']} {t['name']}")
        except Exception as exc:  # noqa: BLE001
            return web.json_response({"error": f"LiveKit: {str(exc)[:100]}"}, status=503)
        x("UPDATE streams SET ingress_id=?, rtmps_url=?, stream_key=? WHERE address=?", (ing["ingress_id"], ing["url"], ing["stream_key"], addr))
    return web.json_response(_creds(_stream_row(addr)), headers=NO_CACHE)


async def h_stream_stop(request):
    w = _wallet(request)
    body = await request.json()
    addr = _norm_addr(body.get("token"))
    s = _stream_row(addr) if addr else None
    if not w or not s or s["wallet"] != w:
        return web.json_response({"error": "not yours"}, status=403)
    x("UPDATE streams SET ended_ts=?, live=0 WHERE address=?", (int(time.time()), addr))
    if s["ingress_id"]:                                   # ingress objects are a scarce LiveKit resource: free it, next Go live mints a new key
        asyncio.create_task(asyncio.to_thread(_release_ingress, addr, s["ingress_id"]))
    await broadcast_all({"t": "live", "token": addr, "live": False})
    return web.json_response({"ok": True})


def _release_ingress(addr: str, ingress_id: str) -> None:
    try:
        lk.delete_ingress(ingress_id)
    except Exception as exc:  # noqa: BLE001
        print(f"[lk] delete_ingress failed: {str(exc)[:100]}")
    x("UPDATE streams SET ingress_id='', rtmps_url='', stream_key='' WHERE address=? AND ingress_id=?", (addr, ingress_id))


async def checkpoint_wal():
    """Every 2 min fold the WAL back into the main file and truncate it; with
    long-lived connections SQLite otherwise lets the WAL grow without bound."""
    while True:
        await asyncio.sleep(120)
        try:
            with _lock:
                r = C.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if r and r[0]:
                print(f"[db] checkpoint busy: {tuple(r)}")
        except Exception as exc:  # noqa: BLE001
            print(f"[db] checkpoint failed: {exc}")


async def track_head():
    """Latest block every 3 s: card ages and the candles endpoint read it instead of calling the RPC per request."""
    while True:
        try:
            _head["n"] = await asyncio.to_thread(chain.block_number)
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(3)


_miss: dict = {}


async def poll_stream_status(app):
    """Every 3 s: ask Cloudflare whether each started stream is actually
    receiving video; flip `live` and tell the pages."""
    while True:
        try:
            _head["n"] = await asyncio.to_thread(chain.block_number)
        except Exception:  # noqa: BLE001
            pass
        try:
            for s in q("SELECT address, input_uid, live, ingest, ingress_id FROM streams WHERE started_ts>0 AND ended_ts=0"):
                curst = {}
                if s["ingress_id"]:
                    try:
                        now_live, how = await asyncio.to_thread(lk.publishing, s["address"])
                    except Exception as exc:  # noqa: BLE001
                        print(f"[lk] status failed for {s['address']}: {str(exc)[:100]}")
                        continue
                    curst = {"ingestProtocol": how}
                else:
                    try:
                        st = await asyncio.to_thread(cf, "GET", f"/{s['input_uid']}")
                    except Exception as exc:  # noqa: BLE001
                        print(f"[cf] status failed for {s['address']}: {exc}")
                        continue
                    curst = ((st.get("status") or {}).get("current") or {})
                    now_live = curst.get("state") == "connected"
                if not now_live and s["live"] and _miss.get(s["address"], 0) < 2:      # one blip is not an outage
                    _miss[s["address"]] = _miss.get(s["address"], 0) + 1
                    continue
                if now_live:
                    _miss.pop(s["address"], None)
                    if not (s["ingest"] if "ingest" in s.keys() else ""):
                        x("UPDATE streams SET ingest=? WHERE address=?", (curst.get("ingestProtocol") or "", s["address"]))
                if now_live != bool(s["live"]):
                    x("UPDATE streams SET live=?, ingest=? WHERE address=?", (int(now_live), curst.get("ingestProtocol") or "", s["address"]))
                    await broadcast_all({"t": "live", "token": s["address"], "live": now_live})
                    print(f"[cf] {s['address']} live={now_live}")
        except Exception as exc:  # noqa: BLE001
            print(f"[cf] poll error: {exc}")
        await asyncio.sleep(2)


# ---- LiveKit: viewer / publisher tokens ----

async def h_lk_viewer(request):
    addr = _norm_addr(request.query.get("token"))
    s = _stream_row(addr) if addr else None
    if not s or not s["ingress_id"]:
        return web.json_response({"error": "no livekit stream"}, status=404)
    ident = "v-" + secrets.token_hex(6)
    return web.json_response({"url": lk.URL, "token": lk.viewer_token(addr, ident), "room": addr}, headers=NO_CACHE)


async def h_lk_publisher(request):
    w = _wallet(request)
    body = await request.json()
    addr = _norm_addr(body.get("token"))
    s = _stream_row(addr) if addr else None
    if not w or not s or s["wallet"] != w or not s["ingress_id"]:
        return web.json_response({"error": "not yours"}, status=403)
    return web.json_response({"url": lk.URL, "token": lk.publisher_token(addr, "creator-web", "creator"), "room": addr}, headers=NO_CACHE)


# ---- WebSocket: launch feed, chat rooms, viewer counts ----
SOCKETS: set = set()          # every open socket (launch feed)
ROOMS: dict = {}              # token -> set of sockets (chat + viewers)


async def _send(ws, msg: dict) -> None:
    try:
        await ws.send_str(json.dumps(msg))
    except Exception:  # noqa: BLE001
        pass


async def broadcast_all(msg: dict) -> None:
    await asyncio.gather(*(_send(ws, msg) for ws in list(SOCKETS)))


async def broadcast_room(room: str, msg: dict) -> None:
    await asyncio.gather(*(_send(ws, msg) for ws in list(ROOMS.get(room, set()))))


async def _viewers(room: str) -> None:
    await broadcast_room(room, {"t": "viewers", "token": room, "n": len(ROOMS.get(room, set()))})


def _roles(room: str) -> dict:
    """wallet -> 'dev' | 'mod' for a coin's chat. The deployer is dev even before the first stream."""
    out = {}
    t = db.get_token(C, room)
    if t:
        out[t["deployer"]] = "dev"
    for r in q("SELECT wallet FROM mods WHERE room=?", (room,)):
        out.setdefault(r["wallet"], "mod")
    return out


def _holders_only(room: str) -> bool:
    r = q("SELECT holders_only FROM room_settings WHERE room=?", (room,))
    return bool(r and r[0]["holders_only"])


_holder_cache: dict = {}


async def _is_holder(room: str, wallet: str) -> bool:
    key = (room, wallet)
    hit = _holder_cache.get(key)
    if hit and time.time() - hit[1] < 60:
        return hit[0]
    try:
        data = "0x70a08231" + wallet[2:].rjust(64, "0")
        r = await asyncio.to_thread(chain.rpc, chain.ALCHEMY_HTTP, "eth_call", [{"to": room, "data": data}, "latest"])
        ok = int(r, 16) > 0 if r and r != "0x" else False
    except Exception:  # noqa: BLE001
        ok = False
    _holder_cache[key] = (ok, time.time())
    return ok


async def _room_state(room: str, wallet: str | None) -> dict:
    roles = _roles(room)
    ho = _holders_only(room)
    you = roles.get(wallet or "", "")
    can = True
    if wallet and ho and not you:
        can = await _is_holder(room, wallet)
    if wallet and (q("SELECT 1 FROM room_bans WHERE room=? AND wallet=?", (room, wallet)) or q("SELECT 1 FROM bans WHERE wallet=? AND room='*'", (wallet,))):
        can = False
    return {"t": "room", "token": room, "holders_only": ho, "mods": [w for w, r in roles.items() if r == "mod"], "dev": next((w for w, r in roles.items() if r == "dev"), ""),
            "you": you, "can_chat": can, "banned": [r["wallet"] for r in q("SELECT wallet FROM room_bans WHERE room=?", (room,))] if you else []}


async def _broadcast_room_state(room: str) -> None:
    for ws in list(ROOMS.get(room, set())):
        await _send(ws, await _room_state(room, SOCK_WALLET.get(ws)))


SOCK_WALLET: dict = {}


def _short(w: str) -> str:
    return w[:4] + ".." + w[-4:]


async def h_ws(request):
    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)
    SOCKETS.add(ws)
    room = None
    wallet = None
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                m = json.loads(msg.data)
            except json.JSONDecodeError:
                continue
            t = m.get("t")
            if t == "auth":
                wallet = _check_token(str(m.get("token") or ""))
                SOCK_WALLET[ws] = wallet
                await _send(ws, {"t": "auth", "address": wallet})
                if room:
                    await _send(ws, await _room_state(room, wallet))
            elif t == "sub":
                new = _norm_addr(m.get("room"))
                if room and room != new:
                    ROOMS.get(room, set()).discard(ws)
                    await _viewers(room)
                room = new
                if room:
                    ROOMS.setdefault(room, set()).add(ws)
                    roles = _roles(room)
                    hist = q("SELECT wallet, text, ts FROM chat WHERE room=? ORDER BY id DESC LIMIT 100", (room,))
                    await _send(ws, {"t": "history", "rows": [{"who": _short(h["wallet"]), "wallet": h["wallet"], "text": h["text"], "ts": h["ts"], "role": roles.get(h["wallet"], "")} for h in reversed(hist)]})
                    await _send(ws, await _room_state(room, wallet))
                    await _viewers(room)
            elif t == "chat" and room and wallet:
                text = str(m.get("text") or "").strip()[:280]
                if not text:
                    continue
                if q("SELECT 1 FROM bans WHERE wallet=? AND room='*'", (wallet,)) or q("SELECT 1 FROM room_bans WHERE room=? AND wallet=?", (room, wallet)):
                    await _send(ws, {"t": "notice", "text": "You are banned from this chat"})
                    continue
                role = _roles(room).get(wallet, "")
                if not role and _holders_only(room) and not await _is_holder(room, wallet):
                    await _send(ws, {"t": "notice", "text": "Holders only: you need to hold this coin to chat"})
                    continue
                ts = int(time.time())
                x("INSERT INTO chat(room,wallet,text,ts) VALUES(?,?,?,?)", (room, wallet, text, ts))
                await broadcast_room(room, {"t": "chat", "who": _short(wallet), "wallet": wallet, "text": text, "ts": ts, "role": role})
            elif t in ("ban", "unban") and room and wallet:
                target = _norm_addr(m.get("wallet"))
                roles = _roles(room)
                me, them = roles.get(wallet, ""), roles.get(target or "", "")
                if not target or target == wallet or me not in ("dev", "mod") or them == "dev" or (me == "mod" and them == "mod"):
                    continue
                if t == "ban":
                    x("INSERT OR REPLACE INTO room_bans(room,wallet,by_wallet,ts) VALUES(?,?,?,?)", (room, target, wallet, int(time.time())))
                    x("DELETE FROM chat WHERE room=? AND wallet=?", (room, target))
                    await broadcast_room(room, {"t": "banned", "wallet": target, "who": _short(target)})
                else:
                    x("DELETE FROM room_bans WHERE room=? AND wallet=?", (room, target))
                    await broadcast_room(room, {"t": "unbanned", "wallet": target, "who": _short(target)})
                await _broadcast_room_state(room)
            elif t == "mod" and room and wallet:
                target = _norm_addr(m.get("wallet"))
                if not target or _roles(room).get(wallet) != "dev" or target == wallet:
                    continue
                if m.get("on"):
                    x("INSERT OR REPLACE INTO mods(room,wallet,by_wallet,ts) VALUES(?,?,?,?)", (room, target, wallet, int(time.time())))
                    x("DELETE FROM room_bans WHERE room=? AND wallet=?", (room, target))
                else:
                    x("DELETE FROM mods WHERE room=? AND wallet=?", (room, target))
                await _broadcast_room_state(room)
            elif t == "holders" and room and wallet:
                if _roles(room).get(wallet) != "dev":
                    continue
                x("INSERT INTO room_settings(room,holders_only) VALUES(?,?) ON CONFLICT(room) DO UPDATE SET holders_only=excluded.holders_only", (room, 1 if m.get("on") else 0))
                await _broadcast_room_state(room)
    finally:
        SOCKETS.discard(ws)
        SOCK_WALLET.pop(ws, None)
        if room:
            ROOMS.get(room, set()).discard(ws)
            await _viewers(room)
    return ws


async def on_launch(t: dict) -> None:
    await asyncio.to_thread(market.enrich_market, [t])
    await broadcast_all({"t": "launch", "coin": _pub(t)})
    if t.get("image"):
        asyncio.create_task(_prefetch_image(t["address"], t["image"]))


async def _prefetch_image(addr: str, url: str) -> None:
    """Fresh launches: the logo often is not on the IPFS gateways yet, so try
    now and again after 20 s, 60 s and 3 min before giving up."""
    for delay in (0, 20, 60, 180):
        if delay:
            await asyncio.sleep(delay)
        if await asyncio.to_thread(images.ensure, addr, url):
            _img_failed.pop(addr, None)
            return
    print(f"[img] gave up on {addr} {url[:60]}")


# ---- coin images: fetched once server-side, served as static thumbnails ----

_img_sem = asyncio.Semaphore(6)
_img_failed: dict = {}
IMG_HEADERS = {"Cache-Control": "public, max-age=86400"}


async def h_img(request):
    addr = _norm_addr(request.match_info["addr"])
    t = db.get_token(C, addr) if addr else None
    if not t or not t.get("image"):
        return web.Response(status=404)
    p = images.path_for(addr)
    if not os.path.exists(p):
        if time.time() - _img_failed.get(addr, 0) < 45:          # short negative cache: gateways catch up within a minute or two
            return web.Response(status=404)
        async with _img_sem:
            p = await asyncio.to_thread(images.ensure, addr, t["image"])
        if not p:
            _img_failed[addr] = time.time()
            return web.Response(status=404)
    return web.FileResponse(p, headers=IMG_HEADERS)


# ---- admin: global ban / kill a stream ----

def _admin(request) -> bool:
    return bool(ADMIN_SECRET) and hmac.compare_digest(request.headers.get("X-Admin-Secret", "").encode(), ADMIN_SECRET.encode())


async def h_fees(request):
    def load():
        c = db.connect()
        c.executescript(engine.SCHEMA)
        return engine.summary(c)
    return web.json_response(await asyncio.to_thread(load), headers=NO_CACHE)


async def h_admin(request):
    if not _admin(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await request.json()
    act = body.get("action")
    if act == "disk":
        def du(path):
            total, n = 0, 0
            for root, _, files in os.walk(path):
                for f in files:
                    try:
                        total += os.path.getsize(os.path.join(root, f)); n += 1
                    except OSError:
                        pass
            return {"mb": round(total / 1048576, 1), "files": n}
        st = os.statvfs(db.DATA_DIR)
        return web.json_response({"db_mb": round(sum(os.path.getsize(f) for f in (db.DB_PATH, db.DB_PATH + "-wal", db.DB_PATH + "-shm") if os.path.exists(f)) / 1048576, 1),
                                  "img": du(images.IMG_DIR) if os.path.isdir(images.IMG_DIR) else {"mb": 0, "files": 0},
                                  "free_mb": round(st.f_bavail * st.f_frsize / 1048576, 1), "total_mb": round(st.f_blocks * st.f_frsize / 1048576, 1)})
    if act == "set":                      # runtime settings: the Plink coin address (`ca`) and friends, no redeploy
        key = str(body.get("key") or "")
        if key not in ("ca",):
            return web.json_response({"error": "unknown key"}, status=400)
        val = _norm_addr(body.get("value")) if key == "ca" else str(body.get("value") or "")
        if key == "ca" and not val:
            return web.json_response({"error": "bad address"}, status=400)
        db.set_progress(C, key, val); C.commit()
        return web.json_response({"ok": True, key: val})
    if act == "ban":
        w = _norm_addr(body.get("wallet"))
        x("INSERT OR REPLACE INTO bans(wallet,room,by_wallet,ts) VALUES(?,'*','admin',?)", (w, int(time.time())))
        for s in q("SELECT address FROM streams WHERE wallet=? AND ended_ts=0", (w,)):
            x("UPDATE streams SET ended_ts=?, live=0 WHERE address=?", (int(time.time()), s["address"]))
            await broadcast_all({"t": "live", "token": s["address"], "live": False})
        return web.json_response({"ok": True, "banned": w})
    if act == "kill":
        addr = _norm_addr(body.get("token"))
        s = _stream_row(addr) if addr else None
        if s:
            try:
                await asyncio.to_thread(cf, "DELETE", f"/{s['input_uid']}")
            except Exception as exc:  # noqa: BLE001
                print(f"[admin] cf delete failed: {exc}")
            x("DELETE FROM streams WHERE address=?", (addr,))
            await broadcast_all({"t": "live", "token": addr, "live": False})
        return web.json_response({"ok": True})
    return web.json_response({"error": "unknown action"}, status=400)


# ---- static / SPA ----

def _asset_ver(name: str) -> str:
    try:
        return str(int((FRONTEND / "assets" / name).stat().st_mtime))
    except OSError:
        return "0"


async def h_index(request):
    html = (FRONTEND / "index.html").read_text()
    ca = db.get_progress(C, "ca") or ""
    html = html.replace("</head>", f"<script>window.CFG={json.dumps({'privyAppId': PRIVY_APP_ID, 'site': SITE_NAME, 'ca': ca})};</script></head>", 1)
    # browsers heuristically cache un-headed static files for days; version the stylesheet and
    # wordmark so a palette change lands on the next load instead of after a hard refresh
    origin = f"{request.scheme}://{request.host}"
    html = html.replace('content="/assets/og.png"', f'content="{origin}/assets/og.png"')
    for name in ("pons.css", "wordmark.png", "icon-32.png", "icon-192.png", "icon-180.png"):
        html = html.replace(f"/assets/{name}\"", f"/assets/{name}?v={_asset_ver(name)}\"")
    return web.Response(text=html, content_type="text/html", headers=NO_CACHE)


@web.middleware
async def asset_cache(request, handler):
    resp = await handler(request)
    if request.path.startswith("/assets/"):
        resp.headers["Cache-Control"] = "public, max-age=31536000, immutable" if "v" in request.query else "no-cache"
    return resp


def _backfill_thread():
    """Catalog maintenance, forever: initial launches/names/meta backfill,
    then a sweep every minute for stragglers (a launch whose enrichment hit
    a 429 in the live loop gets its name/metadata here). Never dies: an RPC
    error just means a pause."""
    from indexer import backfill
    c = db.connect()
    while True:
        try:
            backfill.launches(c)
            backfill.names(c)
            backfill.meta(c)
        except Exception as exc:  # noqa: BLE001
            print(f"[backfill] error, retrying in 30s: {str(exc)[:120]}", file=sys.stderr)
            time.sleep(30)
            continue
        time.sleep(60)


def make_app() -> web.Application:
    app = web.Application(client_max_size=64 * 1024, middlewares=[asset_cache])
    r = app.router
    for p in ("/", "/explore", "/live", "/coin/{addr}", "/search", "/privacy", "/terms", "/preview"):
        r.add_get(p, h_index)
    r.add_post("/api/auth/nonce", h_nonce)
    r.add_post("/api/auth/verify", h_verify)
    r.add_get("/api/me", h_me)
    r.add_get("/api/recent", h_recent)
    r.add_get("/api/search", h_search)
    r.add_get("/api/token/{addr}", h_token)
    r.add_get("/api/streams", h_streams)
    r.add_get("/api/candles/{addr}", h_candles)
    r.add_post("/api/stream/start", h_stream_start)
    r.add_post("/api/stream/stop", h_stream_stop)
    r.add_post("/api/stream/reset", h_stream_reset)
    r.add_get("/api/lk/viewer", h_lk_viewer)
    r.add_post("/api/lk/publisher", h_lk_publisher)
    r.add_get("/api/stream/creds", h_stream_creds)
    r.add_post("/api/admin", h_admin)
    r.add_get("/api/fees", h_fees)
    r.add_get("/ws", h_ws)
    r.add_get("/img/{addr}", h_img)
    r.add_static("/assets", FRONTEND / "assets")

    async def on_start(app):
        app["tasks"] = [asyncio.create_task(live.run(on_launch)), asyncio.create_task(poll_stream_status(app)), asyncio.create_task(track_head()), asyncio.create_task(checkpoint_wal())]
        if (os.environ.get("BACKFILL") or "1") == "1":
            threading.Thread(target=_backfill_thread, daemon=True).start()
        engine.start()

    async def on_stop(app):
        for t in app.get("tasks", []):
            t.cancel()
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_stop)
    return app


if __name__ == "__main__":
    print(f"{SITE_NAME} on :{PORT} (data {db.DATA_DIR}, catalog {db.stats(C)})", flush=True)
    web.run_app(make_app(), port=PORT, print=None)
