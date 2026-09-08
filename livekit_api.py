"""LiveKit Cloud from the server side: access tokens (HS256 JWT, no library)
and the Twirp JSON endpoints we need: RTMP ingress per coin (OBS: server +
stream key → WebRTC room, sub-second) and room participants (live detection)."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request

URL = (os.environ.get("LIVEKIT_URL") or "").strip().rstrip("/")
KEY = (os.environ.get("LIVEKIT_API_KEY") or "").strip()
SECRET = (os.environ.get("LIVEKIT_API_SECRET") or "").strip()
HTTP = URL.replace("wss://", "https://").replace("ws://", "http://")
ENABLED = bool(URL and KEY and SECRET)


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def token(identity: str, video: dict, ttl: int = 6 * 3600, name: str = "") -> str:
    now = int(time.time())
    payload = {"iss": KEY, "sub": identity, "nbf": now - 10, "exp": now + ttl, "video": video}
    if name:
        payload["name"] = name
    head = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    body = _b64(json.dumps(payload, separators=(",", ":")).encode())
    sig = _b64(hmac.new(SECRET.encode(), f"{head}.{body}".encode(), hashlib.sha256).digest())
    return f"{head}.{body}.{sig}"


def viewer_token(room: str, identity: str) -> str:
    return token(identity, {"roomJoin": True, "room": room, "canSubscribe": True, "canPublish": False, "canPublishData": False})


def publisher_token(room: str, identity: str, name: str) -> str:
    return token(identity, {"roomJoin": True, "room": room, "canPublish": True, "canSubscribe": True, "canPublishData": False}, name=name)


def _twirp(service: str, method: str, body: dict, grant: dict) -> dict:
    req = urllib.request.Request(f"{HTTP}/twirp/livekit.{service}/{method}", data=json.dumps(body).encode(), method="POST",
                                 headers={"Authorization": "Bearer " + token("server", grant, ttl=60), "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"livekit {service}.{method} HTTP {e.code}: {e.read()[:200].decode(errors='replace')}") from None


def create_ingress(room: str, name: str) -> dict:
    """RTMP ingress bound to `room`; returns {ingress_id, url, stream_key}. The
    creator publishes as participant 'creator': that is what live detection looks for.
    LiveKit Cloud caps ingress objects and counts just-deleted ones for a while,
    so a resource_exhausted answer is retried a few times."""
    for attempt in range(4):
        try:
            r = _twirp("Ingress", "CreateIngress", {"input_type": "RTMP_INPUT", "name": name[:64], "room_name": room,
                                                     "participant_identity": "creator", "participant_name": name[:64],
                                                     "enable_transcoding": True}, {"ingressAdmin": True})
            return {"ingress_id": r["ingress_id"], "url": r["url"], "stream_key": r["stream_key"]}
        except RuntimeError as exc:
            if "resource_exhausted" not in str(exc) or attempt == 3:
                raise
            time.sleep(2.5 * (attempt + 1))
    raise RuntimeError("unreachable")


def list_ingress() -> list:
    return _twirp("Ingress", "ListIngress", {}, {"ingressAdmin": True}).get("items", [])


def free_idle_ingress(keep: set = frozenset()) -> list:
    """LiveKit Cloud caps the number of ingress objects per project. When the cap
    is hit, drop every ingress that is not publishing right now (a creator who
    stopped OBS keeps a dead slot otherwise). Returns the freed ingress ids."""
    freed = []
    for i in list_ingress():
        st = (i.get("state") or {}).get("status")
        if st == "ENDPOINT_PUBLISHING" or i.get("ingress_id") in keep:
            continue
        try:
            delete_ingress(i["ingress_id"]); freed.append(i["ingress_id"])
        except Exception as exc:  # noqa: BLE001
            print(f"[lk] free ingress {i.get('ingress_id')} failed: {exc}")
    return freed


def delete_ingress(ingress_id: str) -> None:
    _twirp("Ingress", "DeleteIngress", {"ingress_id": ingress_id}, {"ingressAdmin": True})


def publishing(room: str) -> tuple[bool, str]:
    """(is anyone publishing video in the room, 'rtmp'|'browser'|'')."""
    try:
        r = _twirp("RoomService", "ListParticipants", {"room": room}, {"roomAdmin": True, "room": room})
    except RuntimeError as exc:
        if "not found" in str(exc).lower() or "404" in str(exc):
            return False, ""
        raise
    for p in r.get("participants") or []:
        ident = p.get("identity") or ""
        if ident.startswith("creator") and any((t.get("type") or "").upper() == "VIDEO" for t in p.get("tracks") or []):
            return True, ("rtmp" if p.get("kind") == "INGRESS" else "browser")
    return False, ""
