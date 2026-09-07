"""Coin images, served from our own origin.

Public IPFS gateways answer a browser <img> hot-link with a Cloudflare
challenge (403 + CORP same-origin → the browser drops the image), but they
answer a plain server-side GET fine. So the first request for a coin's image
fetches it once (trying several gateways), shrinks it to a 320px JPEG on the
volume, and every later request is a static file. Some "logo" URIs are really
pump.fun-style metadata JSON with an `image` field — followed one level."""
from __future__ import annotations

import io
import json
import os
import re
import threading
import urllib.request

from PIL import Image

from . import db

IMG_DIR = os.path.join(db.DATA_DIR, "img")
CACHE_CAP = int(os.environ.get("IMG_CACHE_MB") or 150) * 1024 * 1024   # thumbnails are a cache: keep the volume for the catalog
MAX_BYTES = 12 * 1024 * 1024
SIZE = 256
UA = {"User-Agent": "pons.live/1.0 (+catalog thumbnails)"}
GATEWAYS = ("https://{cid}.ipfs.nftstorage.link{tail}", "https://{cid}.ipfs.dweb.link{tail}",
            "https://ipfs.io/ipfs/{cid}{tail}", "https://gateway.pinata.cloud/ipfs/{cid}{tail}")
_SUB = re.compile(r"^https?://([a-z0-9]{40,})\.ipfs\.[^/]+(/.*)?$", re.I)
_PATH = re.compile(r"^https?://[^/]+/ipfs/([A-Za-z0-9]{40,})(/.*)?$")


def path_for(addr: str) -> str:
    return os.path.join(IMG_DIR, addr.lower() + ".jpg")


def _candidates(url: str) -> list[str]:
    url = (url or "").strip()
    if re.fullmatch(r"(Qm[1-9A-HJ-NP-Za-km-z]{44}|baf[a-z0-9]{50,})(/.*)?", url):
        url = "ipfs://" + url                    # bare CID
    if url.startswith("ipfs://"):
        ref = url[7:]
        cid, _, tail = ref.partition("/")
        return [g.format(cid=cid, tail=("/" + tail) if tail else "") for g in GATEWAYS]
    m = _SUB.match(url) or _PATH.match(url)
    if m:
        cid, tail = m.group(1), m.group(2) or ""
        return [g.format(cid=cid, tail=tail) for g in GATEWAYS]
    return [url] if url.startswith(("http://", "https://")) else []


def _get(url: str, timeout: float) -> tuple[bytes, str] | None:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
            return r.read(MAX_BYTES + 1), (r.headers.get("Content-Type") or "").lower()
    except Exception:  # noqa: BLE001
        return None


def fetch(url: str, depth: int = 0) -> bytes | None:
    """Raw image bytes for a logo URI. IPFS candidates are raced in parallel
    (the first gateway that answers wins), one JSON metadata hop allowed."""
    cands = _candidates(url)
    if not cands:
        return None
    timeout = 8 if depth == 0 else 10
    got = None
    if len(cands) == 1:
        got = _get(cands[0], timeout)
    else:
        import concurrent.futures as cf
        with cf.ThreadPoolExecutor(len(cands)) as ex:
            futs = [ex.submit(_get, c, timeout) for c in cands]
            for f in cf.as_completed(futs):
                r = f.result()
                if r and r[0] and len(r[0]) <= MAX_BYTES:
                    got = r
                    for o in futs:
                        o.cancel()
                    break
    if not got:
        return None
    data, ctype = got
    if "json" in ctype or data[:1] in (b"{", b"["):
        if depth:
            return None
        try:
            meta = json.loads(data.decode("utf-8", "replace"))
            inner = meta.get("image") or meta.get("image_url") or meta.get("logo") if isinstance(meta, dict) else ""
        except Exception:  # noqa: BLE001
            inner = ""
        return fetch(inner, depth + 1) if inner else None
    return data


def thumb(data: bytes) -> bytes:
    im = Image.open(io.BytesIO(data))
    im.seek(0)
    if im.mode in ("RGBA", "LA", "P"):
        im = im.convert("RGBA")
        bg = Image.new("RGB", im.size, (15, 19, 23))
        bg.paste(im, mask=im.split()[-1])
        im = bg
    else:
        im = im.convert("RGB")
    im.thumbnail((SIZE, SIZE), Image.LANCZOS)
    out = io.BytesIO()
    im.save(out, "JPEG", quality=82, optimize=True)
    return out.getvalue()


def ensure(addr: str, url: str) -> str | None:
    """Path of the cached thumbnail, fetching + shrinking on first use."""
    p = path_for(addr)
    if os.path.exists(p):
        return p
    if not url:
        return None
    data = fetch(url)
    if not data:
        return None
    try:
        jpg = thumb(data)
    except Exception:  # noqa: BLE001
        return None
    os.makedirs(IMG_DIR, exist_ok=True)
    _trim_cache()
    tmp = f"{p}.{os.getpid()}.{threading.get_ident()}.tmp"      # unique: concurrent requests for one coin
    try:
        with open(tmp, "wb") as f:
            f.write(jpg)
        os.replace(tmp, p)
    except OSError:
        return p if os.path.exists(p) else None
    return p


_trim_lock = threading.Lock()
_trim_at = {"ts": 0.0}


def _trim_cache() -> None:
    """Every few minutes: if the thumbnail dir exceeds CACHE_CAP, drop the
    least recently used files until it is 20% under the cap."""
    import time
    if time.time() - _trim_at["ts"] < 300 or not _trim_lock.acquire(blocking=False):
        return
    try:
        _trim_at["ts"] = time.time()
        files = []
        total = 0
        with os.scandir(IMG_DIR) as it:
            for e in it:
                if e.is_file() and e.name.endswith(".jpg"):
                    st = e.stat()
                    files.append((st.st_atime, st.st_size, e.path))
                    total += st.st_size
        if total <= CACHE_CAP:
            return
        files.sort()
        target = CACHE_CAP * 0.8
        for _, size, path in files:
            if total <= target:
                break
            try:
                os.remove(path)
                total -= size
            except OSError:
                pass
        print(f"[img] cache trimmed to {total // 1024 // 1024} MB")
    finally:
        _trim_lock.release()
