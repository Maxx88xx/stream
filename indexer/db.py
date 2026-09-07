"""SQLite catalog of every PONS launch (WAL, FTS5 search on name/symbol).
Single-writer: the indexer process. The web process opens it read-only."""
from __future__ import annotations

import os
import sqlite3
import time

DATA_DIR = os.environ.get("DATA_DIR") or ("/data" if os.path.isdir("/data") else os.path.join(os.path.dirname(os.path.dirname(__file__)), "data"))
DB_PATH = os.path.join(DATA_DIR, "catalog.sqlite")

SCHEMA = """
CREATE TABLE IF NOT EXISTS tokens (
  address     TEXT PRIMARY KEY,
  curve       TEXT NOT NULL,
  deployer    TEXT NOT NULL,
  pair_token  TEXT,
  block       INTEGER NOT NULL,
  tx          TEXT NOT NULL,
  name        TEXT DEFAULT '',
  symbol      TEXT DEFAULT '',
  image       TEXT DEFAULT '',
  description TEXT DEFAULT '',
  twitter     TEXT DEFAULT '',
  telegram    TEXT DEFAULT '',
  discord     TEXT DEFAULT '',
  website     TEXT DEFAULT '',
  names_done  INTEGER DEFAULT 0,
  meta_done   INTEGER DEFAULT 0,
  quote_reserve TEXT DEFAULT '0',
  token_reserve TEXT DEFAULT '0',
  graduated   INTEGER DEFAULT 0,
  market_ts   INTEGER DEFAULT 0,
  created_ts  INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS tokens_block ON tokens(block DESC);
CREATE INDEX IF NOT EXISTS tokens_deployer ON tokens(deployer);
CREATE INDEX IF NOT EXISTS tokens_names_done ON tokens(names_done, block DESC);
CREATE INDEX IF NOT EXISTS tokens_meta_done ON tokens(meta_done, block DESC);
CREATE VIRTUAL TABLE IF NOT EXISTS tokens_fts USING fts5(address UNINDEXED, name, symbol, tokenize='unicode61');
CREATE TABLE IF NOT EXISTS progress (key TEXT PRIMARY KEY, value TEXT);
"""


def connect(readonly: bool = False) -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    if readonly:
        c = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10, check_same_thread=False)
    else:
        c = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA journal_size_limit=67108864")      # WAL never lingers above 64 MB after a checkpoint
        c.executescript(SCHEMA)
        _migrate(c)
    c.row_factory = sqlite3.Row
    return c


def _migrate(c) -> None:
    """One-time: rebuild the FTS index keyed by tokens.rowid (v2)."""
    r = c.execute("SELECT value FROM progress WHERE key='fts_v2'").fetchone()
    if r:
        return
    c.execute("DELETE FROM tokens_fts")
    c.execute("INSERT INTO tokens_fts(rowid,address,name,symbol) SELECT rowid,address,name,symbol FROM tokens WHERE names_done=1")
    c.execute("INSERT OR REPLACE INTO progress(key,value) VALUES('fts_v2','1')")
    c.commit()


def get_progress(c, key: str, default=None):
    r = c.execute("SELECT value FROM progress WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default


def set_progress(c, key: str, value) -> None:
    c.execute("INSERT INTO progress(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))


def insert_launches(c, rows: list) -> int:
    """rows from chain.parse_launch_log; ignores duplicates. Returns inserted count."""
    before = c.total_changes
    c.executemany(
        "INSERT OR IGNORE INTO tokens(address,curve,deployer,pair_token,block,tx,created_ts) VALUES(?,?,?,?,?,?,?)",
        [(r["address"].lower(), r["curve"].lower(), r["deployer"].lower(), (r.get("pair_token") or "").lower(),
          r["block"], r["tx"], r.get("ts", 0)) for r in rows])
    return c.total_changes - before


def set_names(c, named: dict) -> None:
    """{address: (name, symbol)} → tokens + FTS."""
    for addr, (name, sym) in named.items():
        c.execute("UPDATE tokens SET name=?, symbol=?, names_done=1 WHERE address=?", (name, sym, addr))
        # FTS rowid == tokens.rowid: delete/insert by rowid is O(log n); by `address` it was a full FTS scan
        c.execute("DELETE FROM tokens_fts WHERE rowid=(SELECT rowid FROM tokens WHERE address=?)", (addr,))
        c.execute("INSERT INTO tokens_fts(rowid,address,name,symbol) SELECT rowid,?,?,? FROM tokens WHERE address=?", (addr, name, sym, addr))


def set_meta(c, addr: str, meta: dict) -> None:
    c.execute("UPDATE tokens SET image=?, description=?, twitter=?, telegram=?, discord=?, website=?, meta_done=1 WHERE address=?",
              (meta.get("image", ""), meta.get("description", ""), meta.get("twitter", ""), meta.get("telegram", ""),
               meta.get("discord", ""), meta.get("website", ""), addr))


def set_market(c, states: dict, curve_to_addr: dict) -> None:
    now = int(time.time())
    for curve, (q, t, grad) in states.items():
        addr = curve_to_addr.get(curve)
        if addr:
            c.execute("UPDATE tokens SET quote_reserve=?, token_reserve=?, graduated=?, market_ts=? WHERE address=?",
                      (str(q), str(t), int(grad), now, addr))


def search(c, q: str, limit: int = 30) -> list:
    q = (q or "").strip()
    if not q:
        return []
    if q.lower().startswith("0x") and len(q) >= 6:
        return [dict(r) for r in c.execute("SELECT * FROM tokens WHERE address LIKE ? ORDER BY block DESC LIMIT ?", (q.lower() + "%", limit))]
    # prefix match on every term, newest first among the matches
    terms = " ".join(f'"{t}"*' for t in q.replace('"', " ").split() if t)
    rows = c.execute(
        "SELECT t.* FROM tokens_fts f JOIN tokens t ON t.address=f.address WHERE tokens_fts MATCH ? ORDER BY t.block DESC LIMIT ?",
        (terms, limit)).fetchall()
    return [dict(r) for r in rows]


def recent(c, limit: int = 60, offset: int = 0) -> list:
    return [dict(r) for r in c.execute("SELECT * FROM tokens ORDER BY block DESC LIMIT ? OFFSET ?", (limit, offset))]


def get_token(c, addr: str):
    r = c.execute("SELECT * FROM tokens WHERE address=?", (addr.lower(),)).fetchone()
    return dict(r) if r else None


def by_deployer(c, deployer: str, limit: int = 100) -> list:
    return [dict(r) for r in c.execute("SELECT * FROM tokens WHERE deployer=? ORDER BY block DESC LIMIT ?", (deployer.lower(), limit))]


def stats(c) -> dict:
    n = c.execute("SELECT COUNT(*) AS n FROM tokens").fetchone()["n"]
    named = c.execute("SELECT COUNT(*) AS n FROM tokens WHERE names_done=1").fetchone()["n"]
    meta = c.execute("SELECT COUNT(*) AS n FROM tokens WHERE meta_done=1").fetchone()["n"]
    return {"tokens": n, "named": named, "meta": meta,
            "scanned_to": int(get_progress(c, "scanned_to", 0) or 0)}


# ---- streams / chat / bans (written by the web process) ----

EXTRA_SCHEMA = """
CREATE TABLE IF NOT EXISTS streams (
  address    TEXT PRIMARY KEY,
  input_uid  TEXT NOT NULL,
  rtmps_url  TEXT NOT NULL,
  stream_key TEXT NOT NULL,
  whip_url   TEXT NOT NULL,
  hls_url    TEXT NOT NULL,
  whep_url   TEXT NOT NULL,
  title      TEXT DEFAULT '',
  live       INTEGER DEFAULT 0,
  started_ts INTEGER DEFAULT 0,
  ended_ts   INTEGER DEFAULT 0,
  wallet     TEXT NOT NULL,
  created_ts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS chat (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  room    TEXT NOT NULL,
  wallet  TEXT NOT NULL,
  text    TEXT NOT NULL,
  ts      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS chat_room ON chat(room, id DESC);
CREATE TABLE IF NOT EXISTS bans (wallet TEXT PRIMARY KEY, room TEXT NOT NULL DEFAULT '*', by_wallet TEXT, ts INTEGER);
CREATE TABLE IF NOT EXISTS room_bans (room TEXT NOT NULL, wallet TEXT NOT NULL, by_wallet TEXT, ts INTEGER, PRIMARY KEY (room, wallet));
CREATE TABLE IF NOT EXISTS mods (room TEXT NOT NULL, wallet TEXT NOT NULL, by_wallet TEXT, ts INTEGER, PRIMARY KEY (room, wallet));
CREATE TABLE IF NOT EXISTS room_settings (room TEXT PRIMARY KEY, holders_only INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS trades (
  curve  TEXT NOT NULL,
  block  INTEGER NOT NULL,
  idx    INTEGER NOT NULL,
  side   TEXT NOT NULL,
  quote  TEXT NOT NULL,
  tokens TEXT NOT NULL,
  PRIMARY KEY (curve, block, idx)
);
CREATE INDEX IF NOT EXISTS trades_curve ON trades(curve, block);
CREATE TABLE IF NOT EXISTS trades_progress (curve TEXT PRIMARY KEY, block INTEGER NOT NULL);
"""


def connect_web() -> sqlite3.Connection:
    """The web process's read-write handle: catalog read, streams/chat write."""
    c = connect()
    c.executescript(EXTRA_SCHEMA)
    cols = {r[1] for r in c.execute("PRAGMA table_info(streams)")}
    for col in ("ingest", "ingress_id"):
        if col not in cols:
            c.execute(f"ALTER TABLE streams ADD COLUMN {col} TEXT DEFAULT ''")
    c.commit()
    return c
