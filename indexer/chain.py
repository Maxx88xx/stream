"""Chain access for the PONS catalog indexer (Robinhood Chain, chainId 4663).

Two endpoints, each used for what it is good at (measured 2026-09-06):
  * PUBLIC RPC  — eth_getLogs over wide ranges (100k blocks / 10k logs per call)
                  but 429s under bursts → paced, with backoff.
  * ALCHEMY     — eth_call (multicall3: 600 tokens' name+symbol in one call),
                  batched eth_getTransactionByHash (50 per HTTP request), and
                  the WebSocket log subscription for instant launches. Its free
                  tier caps eth_getLogs at 10 blocks, so it never serves logs.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

from eth_abi import decode as abi_decode

PUBLIC_RPC = os.environ.get("PUBLIC_RPC", "https://rpc.mainnet.chain.robinhood.com")
ALCHEMY_KEY = (os.environ.get("ALCHEMY_KEY") or "").strip()
ALCHEMY_HTTP = f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}"
ALCHEMY_WSS = f"wss://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}"

PONS_CORE = "0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e"
LAUNCH_TOPIC = "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607"
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
SEL_NAME, SEL_SYMBOL = "0x06fdde03", "0x95d89b41"
SEL_RESERVES, SEL_GRADUATED = "0x0902f1ac", "0xe7c2b772"
# PonsV2 launcher 0xe33e…2948: launch((name,symbol,logo,description,socials(5),feeWallet),…)
LAUNCH_SELECTOR = "0xf85f8e41"
LAUNCH_TUPLE = "(string,string,string,string,(string,string,string,string,string),address)"

UA = {"Content-Type": "application/json", "User-Agent": "pons-stream-indexer/1.0"}


class RpcError(Exception):
    pass


def _post(url: str, body, tries: int = 6, timeout: int = 120):
    last = None
    for i in range(tries):
        req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=UA)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}: {e.read()[:160].decode(errors='replace')}"
            if e.code not in (429, 500, 502, 503, 504):
                break
        except Exception as exc:  # noqa: BLE001
            last = f"{type(exc).__name__}: {exc}"
        time.sleep(min(20, 1.5 * (2 ** i)))
    raise RpcError(str(last))


def rpc(url: str, method: str, params) -> object:
    res = _post(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if "error" in res:
        raise RpcError(str(res["error"])[:200])
    return res["result"]


def rpc_batch(url: str, calls: list) -> list:
    """[(method, params), …] → results in order (None for a failed item)."""
    if not calls:
        return []
    body = [{"jsonrpc": "2.0", "id": i, "method": m, "params": p} for i, (m, p) in enumerate(calls)]
    res = _post(url, body)
    if isinstance(res, dict):
        raise RpcError(str(res.get("error"))[:200])
    out = [None] * len(calls)
    for item in res:
        try:
            out[int(item["id"])] = item.get("result")
        except (KeyError, ValueError, TypeError):
            pass
    return out


def block_number() -> int:
    return int(rpc(PUBLIC_RPC, "eth_blockNumber", []), 16)


def launch_logs(frm: int, to: int) -> list:
    """TokenLaunched logs on the core in [frm, to] (public RPC; the caller
    keeps the span under the 10k-log cap and halves on overflow)."""
    try:
        return rpc(PUBLIC_RPC, "eth_getLogs", [{"address": PONS_CORE, "topics": [LAUNCH_TOPIC],
                                                "fromBlock": hex(frm), "toBlock": hex(to)}])
    except RpcError as exc:
        if to > frm and any(k in str(exc) for k in ("exceeds limit", "timed out", "timeout", "too many", "429")):
            mid = (frm + to) // 2
            return launch_logs(frm, mid) + launch_logs(mid + 1, to)
        raise


def parse_launch_log(lg: dict) -> dict:
    """topics = [sig, token, curve, deployer]; data = (pairToken, launchConfigId, graduationThreshold)."""
    d = lg["data"][2:]
    words = [d[i:i + 64] for i in range(0, len(d), 64)]
    return {
        "address": "0x" + lg["topics"][1][-40:],
        "curve": "0x" + lg["topics"][2][-40:],
        "deployer": "0x" + lg["topics"][3][-40:],
        "pair_token": ("0x" + words[0][-40:]) if words else None,
        "block": int(lg["blockNumber"], 16),
        "tx": lg["transactionHash"],
    }


# ---- multicall3 aggregate3((address,bool,bytes)[]) ----

def _enc_agg3(calls: list) -> str:
    n = len(calls)
    offs, body = [], b""
    for tgt, data in calls:
        offs.append(32 * n + len(body))
        d = bytes.fromhex(data[2:])
        body += (int(tgt, 16).to_bytes(32, "big") + (1).to_bytes(32, "big") + (96).to_bytes(32, "big")
                 + len(d).to_bytes(32, "big") + d + b"\0" * ((32 - len(d) % 32) % 32))
    arr = n.to_bytes(32, "big") + b"".join(o.to_bytes(32, "big") for o in offs) + body
    return "0x82ad56cb" + (32).to_bytes(32, "big").hex() + arr.hex()


def _dec_agg3(hexres: str) -> list:
    raw = bytes.fromhex(hexres[2:])
    w = lambda i: int.from_bytes(raw[i:i + 32], "big")  # noqa: E731
    base = w(0)
    n = w(base)
    out = []
    for k in range(n):
        off = base + 32 + w(base + 32 + 32 * k)
        ok = w(off)
        doff = off + w(off + 32)
        ln = w(doff)
        out.append((bool(ok), raw[doff + 32:doff + 32 + ln]))
    return out


def multicall(calls: list, url: str = ALCHEMY_HTTP) -> list:
    """[(to, data), …] → [(ok, returndata_bytes), …] in one eth_call."""
    if not calls:
        return []
    res = rpc(url, "eth_call", [{"to": MULTICALL3, "data": _enc_agg3(calls)}, "latest"])
    return _dec_agg3(res)


def abi_string(b: bytes) -> str:
    try:
        if len(b) == 32:                       # legacy bytes32 symbol
            return b.rstrip(b"\0").decode(errors="replace")
        off = int.from_bytes(b[:32], "big")
        ln = int.from_bytes(b[off:off + 32], "big")
        return b[off + 32:off + 32 + ln].decode(errors="replace")
    except Exception:  # noqa: BLE001
        return ""


def names_symbols(tokens: list) -> dict:
    """{token: (name, symbol)} for up to ~600 tokens per multicall."""
    out = {}
    for i in range(0, len(tokens), 600):
        chunk = tokens[i:i + 600]
        res = multicall([(t, SEL_NAME) for t in chunk] + [(t, SEL_SYMBOL) for t in chunk])
        n = len(chunk)
        for k, t in enumerate(chunk):
            name = abi_string(res[k][1]) if res[k][0] else ""
            sym = abi_string(res[n + k][1]) if res[n + k][0] else ""
            out[t] = (name[:80], sym[:32])
    return out


def decimals(token: str) -> int:
    r = rpc(ALCHEMY_HTTP, "eth_call", [{"to": token, "data": "0x313ce567"}, "latest"])
    return int(r, 16) if r and r != "0x" else 18


_state_cache: dict = {}
STATE_TTL = 4.0


def curve_states(curves: list) -> dict:
    """{curve: (quote_reserve, token_reserve, graduated)} via multicall; a
    4 s cache so page hops and polls do not repeat the same RPC round-trip."""
    now = time.time()
    out = {c: _state_cache[c][0] for c in curves if c in _state_cache and now - _state_cache[c][1] < STATE_TTL}
    todo = [c for c in curves if c not in out]
    for i in range(0, len(todo), 400):
        chunk = todo[i:i + 400]
        res = multicall([(c, SEL_RESERVES) for c in chunk] + [(c, SEL_GRADUATED) for c in chunk])
        n = len(chunk)
        for k, c in enumerate(chunk):
            ok, b = res[k]
            gok, gb = res[n + k]
            if ok and len(b) >= 64:
                q, t = int.from_bytes(b[:32], "big"), int.from_bytes(b[32:64], "big")
                grad = bool(int.from_bytes(gb[-32:], "big")) if gok and gb else False
                out[c] = (q, t, grad)
                _state_cache[c] = (out[c], now)
    return out


# ---- launch calldata → metadata ----

def _ipfs_url(ref: str) -> str:
    ref = (ref or "").strip()
    if not ref:
        return ""
    if ref.startswith("ipfs://"):
        ref = ref[7:]
    if ref.startswith("http://") or ref.startswith("https://"):
        return ref
    cid = ref.split("/")[0]
    tail = ref[len(cid):]
    if cid.startswith("baf"):                  # CIDv1 → subdomain gateway (the one that answers)
        return f"https://{cid}.ipfs.dweb.link{tail}"
    return f"https://ipfs.io/ipfs/{ref}"


def decode_launch_input(inp: str) -> dict:
    """Metadata from the launch tx calldata. The PonsV2 launcher's selector is
    decoded exactly; other launchers (bots, third-party UIs) get a best-effort
    scan for an image ref among the calldata strings."""
    meta = {"image": "", "description": "", "twitter": "", "telegram": "", "discord": "", "website": ""}
    if not inp or len(inp) < 10:
        return meta
    data = bytes.fromhex(inp[10:])
    if inp[:10].lower() == LAUNCH_SELECTOR:
        try:
            name, symbol, logo, desc, socials, _fee = abi_decode([LAUNCH_TUPLE], data, strict=False)[0]
            tw, tg, dc, web, _fc = socials
            return {"image": _ipfs_url(logo), "description": desc[:600], "twitter": tw[:200],
                    "telegram": tg[:200], "discord": dc[:200], "website": web[:200]}
        except Exception:  # noqa: BLE001
            pass
    import re
    text = data.decode("latin-1")
    urls = re.findall(r"https?://[\x21-\x7e]+", text)
    ipfs = re.findall(r"ipfs://[\x21-\x7e]+|\bbaf[a-z2-7]{45,}[\x21-\x7e]*|\bQm[1-9A-HJ-NP-Za-km-z]{44}", text)
    if ipfs:
        meta["image"] = _ipfs_url(ipfs[0])
    for u in urls:
        u = u.rstrip("\x00")
        if re.search(r"\.(png|jpe?g|webp|gif)(\?|$)", u, re.I) and not meta["image"]:
            meta["image"] = u[:300]
        elif "x.com/" in u or "twitter.com/" in u:
            meta["twitter"] = meta["twitter"] or u[:200]
        elif "t.me/" in u:
            meta["telegram"] = meta["telegram"] or u[:200]
        elif "discord" in u:
            meta["discord"] = meta["discord"] or u[:200]
        elif "ipfs" not in u and "sslip.io" not in u:
            meta["website"] = meta["website"] or u[:200]
    return meta


def fetch_tx_inputs(hashes: list) -> dict:
    """{tx_hash: input} via Alchemy batches of 50."""
    out = {}
    for i in range(0, len(hashes), 50):
        chunk = hashes[i:i + 50]
        res = rpc_batch(ALCHEMY_HTTP, [("eth_getTransactionByHash", [h]) for h in chunk])
        for h, r in zip(chunk, res):
            if r and r.get("input"):
                out[h] = r["input"]
    return out
