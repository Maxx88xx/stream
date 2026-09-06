"""pons.live — livestreams for PONS coins (Robinhood Chain), pump.fun style.

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

from indexer import chain, db, images, live, market

BASE = Path(__file__).resolve().parent
FRONTEND = BASE / "frontend"
PORT = int(os.environ.get("PORT") or 8080)
CF_ACCOUNT = (os.environ.get("CF_ACCOUNT_ID") or "").strip()
CF_TOKEN = (os.environ.get("CF_API_TOKEN") or "").strip()
PRIVY_APP_ID = (os.environ.get("PRIVY_APP_ID") or "").strip()
ADMIN_SECRET = (os.environ.get("ADMIN_SECRET") or "").strip()
SITE_NAME = os.environ.get("SITE_NAME") or "pons.live"
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
    return web.json_response({"address": w, "coins": db.by_deployer(C, w) if w else []})


# ---- catalog ----

def _pub(t: dict, stream: dict | None = None) -> dict:
    """Public shape of a token row (+ live stream state)."""
    out = {k: t.get(k) for k in ("address", "curve", "deployer", "pair_token", "block", "name", "symbol", "image",
                                  "description", "twitter", "telegram", "website", "price_quote", "quote_symbol",
                                  "mcap_usd", "graduated", "created_ts")}
    s = stream if stream is not None else _stream_row(t["address"])
    out["image"] = f"/img/{t['address']}" if t.get("image") else ""
    out["live"] = bool(s and s["live"])
    out["title"] = (s or {}).get("title", "")
    out["viewers"] = len(ROOMS.get(t["address"], set()))
    return out


def _stream_row(addr: str):
    r = q("SELECT * FROM streams WHERE address=?", (addr,))
    return r[0] if r else None


async def h_recent(request):
    offset = max(0, int(request.query.get("offset") or 0))
    rows = db.recent(C, 48, offset)
    await asyncio.to_thread(market.enrich_market, rows)
    return web.json_response({"rows": [_pub(t) for t in rows], "stats": db.stats(C)}, headers=NO_CACHE)


async def h_search(request):
    rows = db.search(C, request.query.get("q", ""), 30)
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
        out["hls_url"], out["whep_url"], out["input_uid"] = s["hls_url"], s["whep_url"], s["input_uid"]
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
    tf = tf if tf in (60, 300, 900, 3600) else 60
    lock = _candle_locks.setdefault(addr, asyncio.Lock())
    async with lock:
        head = await asyncio.to_thread(chain.block_number)
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
            rows = market.candles(C, t["curve"], t.get("pair_token") or "", tf, head)
            trades = [dict(r) for r in C.execute("SELECT block, side, quote, tokens FROM trades WHERE curve=? ORDER BY block DESC, idx DESC LIMIT 30", (t["curve"],))]
    dec, usd, sym = market.quote_info(t.get("pair_token") or "")
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


def _creds(s: dict) -> dict:
    return {"rtmps_url": s["rtmps_url"], "stream_key": s["stream_key"], "whip_url": s["whip_url"],
            "hls_url": s["hls_url"], "whep_url": s["whep_url"], "title": s["title"], "live": bool(s["live"])}


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
    s = _stream_row(addr)
    if not s:
        li = await asyncio.to_thread(cf, "POST", "", {"meta": {"name": f"{t['symbol']} {addr}"}, "recording": {"mode": "automatic"}})
        x("INSERT INTO streams(address,input_uid,rtmps_url,stream_key,whip_url,hls_url,whep_url,title,wallet,created_ts) VALUES(?,?,?,?,?,?,?,?,?,?)",
          (addr, li["uid"], li["rtmps"]["url"], li["rtmps"]["streamKey"], li["webRTC"]["url"], li["playback"]["hls"],
           li["webRTCPlayback"]["url"], title, w, int(time.time())))
        s = _stream_row(addr)
    x("UPDATE streams SET title=?, started_ts=?, ended_ts=0 WHERE address=?", (title, int(time.time()), addr))
    s = _stream_row(addr)
    return web.json_response(_creds(s), headers=NO_CACHE)


async def h_stream_creds(request):
    w = _wallet(request)
    addr = _norm_addr(request.query.get("token"))
    s = _stream_row(addr) if addr else None
    if not w or not s or s["wallet"] != w:
        return web.json_response({"error": "not yours"}, status=403)
    return web.json_response(_creds(s), headers=NO_CACHE)


async def h_stream_stop(request):
    w = _wallet(request)
    body = await request.json()
    addr = _norm_addr(body.get("token"))
    s = _stream_row(addr) if addr else None
    if not w or not s or s["wallet"] != w:
        return web.json_response({"error": "not yours"}, status=403)
    x("UPDATE streams SET ended_ts=?, live=0 WHERE address=?", (int(time.time()), addr))
    await broadcast_all({"t": "live", "token": addr, "live": False})
    return web.json_response({"ok": True})


async def poll_stream_status(app):
    """Every 10 s: ask Cloudflare whether each started stream is actually
    receiving video; flip `live` and tell the pages."""
    while True:
        try:
            for s in q("SELECT address, input_uid, live FROM streams WHERE started_ts>0 AND ended_ts=0"):
                try:
                    st = await asyncio.to_thread(cf, "GET", f"/{s['input_uid']}")
                except Exception as exc:  # noqa: BLE001
                    print(f"[cf] status failed for {s['address']}: {exc}")
                    continue
                cur = ((st.get("status") or {}).get("current") or {}).get("state")
                now_live = cur == "connected"
                if now_live != bool(s["live"]):
                    x("UPDATE streams SET live=? WHERE address=?", (int(now_live), s["address"]))
                    await broadcast_all({"t": "live", "token": s["address"], "live": now_live})
                    print(f"[cf] {s['address']} live={now_live}")
        except Exception as exc:  # noqa: BLE001
            print(f"[cf] poll error: {exc}")
        await asyncio.sleep(10)


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
                await _send(ws, {"t": "auth", "address": wallet})
            elif t == "sub":
                new = _norm_addr(m.get("room"))
                if room and room != new:
                    ROOMS.get(room, set()).discard(ws)
                    await _viewers(room)
                room = new
                if room:
                    ROOMS.setdefault(room, set()).add(ws)
                    hist = q("SELECT wallet, text, ts FROM chat WHERE room=? ORDER BY id DESC LIMIT 100", (room,))
                    await _send(ws, {"t": "history", "rows": [{"who": _short(h["wallet"]), "wallet": h["wallet"], "text": h["text"], "ts": h["ts"]} for h in reversed(hist)]})
                    await _viewers(room)
            elif t == "chat" and room and wallet:
                text = str(m.get("text") or "").strip()[:280]
                if not text or q("SELECT 1 FROM bans WHERE wallet=? AND room IN ('*', ?)", (wallet, room)):
                    continue
                ts = int(time.time())
                x("INSERT INTO chat(room,wallet,text,ts) VALUES(?,?,?,?)", (room, wallet, text, ts))
                await broadcast_room(room, {"t": "chat", "who": _short(wallet), "wallet": wallet, "text": text, "ts": ts})
            elif t == "ban" and room and wallet:
                target = _norm_addr(m.get("wallet"))
                s = _stream_row(room)
                if target and s and s["wallet"] == wallet and target != wallet:
                    x("INSERT OR REPLACE INTO bans(wallet,room,by_wallet,ts) VALUES(?,?,?,?)", (target, room, wallet, int(time.time())))
                    await broadcast_room(room, {"t": "banned", "wallet": target})
    finally:
        SOCKETS.discard(ws)
        if room:
            ROOMS.get(room, set()).discard(ws)
            await _viewers(room)
    return ws


async def on_launch(t: dict) -> None:
    await asyncio.to_thread(market.enrich_market, [t])
    await broadcast_all({"t": "launch", "coin": _pub(t)})
    if t.get("image"):
        asyncio.create_task(asyncio.to_thread(images.ensure, t["address"], t["image"]))


# ---- coin images: fetched once server-side, served as static thumbnails ----

_img_sem = asyncio.Semaphore(12)
_img_failed: dict = {}
IMG_HEADERS = {"Cache-Control": "public, max-age=86400"}


async def h_img(request):
    addr = _norm_addr(request.match_info["addr"])
    t = db.get_token(C, addr) if addr else None
    if not t or not t.get("image"):
        return web.Response(status=404)
    p = images.path_for(addr)
    if not os.path.exists(p):
        if time.time() - _img_failed.get(addr, 0) < 600:
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


async def h_admin(request):
    if not _admin(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await request.json()
    act = body.get("action")
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

async def h_index(request):
    html = (FRONTEND / "index.html").read_text()
    html = html.replace("</head>", f"<script>window.CFG={json.dumps({'privyAppId': PRIVY_APP_ID, 'site': SITE_NAME})};</script></head>", 1)
    return web.Response(text=html, content_type="text/html", headers=NO_CACHE)


def _backfill_thread():
    try:
        from indexer import backfill
        c = db.connect()
        backfill.launches(c)
        backfill.names(c)
        backfill.meta(c)
    except Exception as exc:  # noqa: BLE001
        print(f"[backfill] thread died: {exc}", file=sys.stderr)


def make_app() -> web.Application:
    app = web.Application(client_max_size=64 * 1024)
    r = app.router
    for p in ("/", "/live", "/coin/{addr}", "/search"):
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
    r.add_get("/api/stream/creds", h_stream_creds)
    r.add_post("/api/admin", h_admin)
    r.add_get("/ws", h_ws)
    r.add_get("/img/{addr}", h_img)
    r.add_static("/assets", FRONTEND / "assets")

    async def on_start(app):
        app["tasks"] = [asyncio.create_task(live.run(on_launch)), asyncio.create_task(poll_stream_status(app))]
        if (os.environ.get("BACKFILL") or "1") == "1":
            threading.Thread(target=_backfill_thread, daemon=True).start()

    async def on_stop(app):
        for t in app.get("tasks", []):
            t.cancel()
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_stop)
    return app


if __name__ == "__main__":
    print(f"{SITE_NAME} on :{PORT} (data {db.DATA_DIR}, catalog {db.stats(C)})", flush=True)
    web.run_app(make_app(), port=PORT, print=None)
