"""Creator-fee engine.

A share of the creator fees that land in the fee wallet buys the coin with the
biggest volume among the coins streaming on Plink, and the bought tokens are handed
out pro-rata to holders of the Plink coin (the "main" coin, set via admin `ca`).

Money rules (same canon as the Solana engines):
- the private key comes only from the FEE_WALLET_KEY variable, never from code or files;
- every balance read that decides money is fail-CLOSED: an RPC error aborts the cycle;
- fees = wallet balance delta since the last cycle, nothing is trusted from any API;
- one buy never exceeds MAX_BUY, whatever accumulated;
- curve/launcher/core/contract addresses never receive payouts.
"""
from __future__ import annotations

import os
import threading
import time
import traceback

from web3 import Web3
from eth_account import Account

from indexer import chain, db, market

SHARE = float(os.environ.get("FEE_SHARE") or 0.20)
MAX_BUY = float(os.environ.get("MAX_BUY_ETH") or 0.05)           # one buy, in quote units (ETH or the pair token)
MIN_BUY = float(os.environ.get("MIN_BUY_ETH") or 0.002)
GAS_RESERVE = float(os.environ.get("GAS_RESERVE_ETH") or 0.01)    # native ETH kept for gas
CYCLE = int(os.environ.get("FEE_CYCLE_SEC") or 600)
MAX_RECIPIENTS = int(os.environ.get("FEE_MAX_RECIPIENTS") or 300)
MIN_SHARE = float(os.environ.get("FEE_MIN_SHARE") or 0.0005)     # holders below this share are skipped (dust vs gas)
LAUNCHER = "0xe33e9e479df8802cb0866d5d05258bec4cf62948"
FEE_ESCROW = (os.environ.get("FEE_ESCROW") or "0xd3AFEB2a57f70eF218Aa82451c51B2fb0416Ac9e")   # PONS v2 fee escrow: creator fees wait here until claimed
CLAIM_GAS_MIN = 0.001
ZERO = "0x" + "0" * 40
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

ERC20 = [
    {"name": "balanceOf", "type": "function", "stateMutability": "view", "inputs": [{"name": "a", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}]},
    {"name": "transfer", "type": "function", "stateMutability": "nonpayable", "inputs": [{"name": "to", "type": "address"}, {"name": "v", "type": "uint256"}], "outputs": [{"name": "", "type": "bool"}]},
    {"name": "approve", "type": "function", "stateMutability": "nonpayable", "inputs": [{"name": "s", "type": "address"}, {"name": "v", "type": "uint256"}], "outputs": [{"name": "", "type": "bool"}]},
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "uint8"}]},
]
CURVE = [
    {"name": "buy", "type": "function", "stateMutability": "payable",
     "inputs": [{"name": "quoteIn", "type": "uint256"}, {"name": "minOut", "type": "uint256"}, {"name": "to", "type": "address"}], "outputs": []},
    {"name": "getReserves", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"name": "q", "type": "uint256"}, {"name": "t", "type": "uint256"}]},
    {"name": "graduated", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "bool"}]},
]

ESCROW = [
    {"name": "balanceOf", "type": "function", "stateMutability": "view", "inputs": [{"name": "a", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}]},
    {"name": "balanceOfToken", "type": "function", "stateMutability": "view", "inputs": [{"name": "a", "type": "address"}, {"name": "t", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}]},
    {"name": "claim", "type": "function", "stateMutability": "nonpayable", "inputs": [{"name": "amount", "type": "uint256"}], "outputs": []},
    {"name": "claimToken", "type": "function", "stateMutability": "nonpayable", "inputs": [{"name": "t", "type": "address"}, {"name": "amount", "type": "uint256"}], "outputs": []},
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS fee_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, status TEXT NOT NULL, note TEXT DEFAULT '',
  quote TEXT DEFAULT '', fees_new TEXT DEFAULT '0', spent TEXT DEFAULT '0', target TEXT DEFAULT '', target_symbol TEXT DEFAULT '',
  bought TEXT DEFAULT '0', recipients INTEGER DEFAULT 0, tx_buy TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS fee_payouts (run_id INTEGER NOT NULL, wallet TEXT NOT NULL, amount TEXT NOT NULL, tx TEXT DEFAULT '', ts INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS main_holders (token TEXT NOT NULL, wallet TEXT NOT NULL, balance TEXT NOT NULL, PRIMARY KEY (token, wallet));
"""

_log = lambda *a: print("[fees]", *a, flush=True)


def _w3() -> Web3:
    url = chain.ALCHEMY_HTTP if chain.ALCHEMY_KEY else chain.PUBLIC_RPC
    return Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 60, "headers": {"User-Agent": "Plink/1.0"}}))


def _key() -> str:
    return (os.environ.get("FEE_WALLET_KEY") or "").strip()


def enabled() -> bool:
    return bool(_key())


def wallet_address() -> str:
    return Account.from_key(_key()).address if enabled() else ""


def main_coin(c) -> dict | None:
    ca = (db.get_progress(c, "ca") or "").lower()
    if not ca:
        return None
    r = c.execute("SELECT * FROM tokens WHERE address=?", (ca,)).fetchone()
    return dict(r) if r else None


def _is_contract(w3: Web3, addr: str, cache: dict) -> bool:
    if addr not in cache:
        cache[addr] = len(w3.eth.get_code(Web3.to_checksum_address(addr))) > 0
    return cache[addr]


def sync_holders(c, w3: Web3, token: str, launch_block: int, head: int) -> None:
    """Incremental Transfer scan of the main coin → main_holders balances."""
    key = "holders_block:" + token
    frm = int(db.get_progress(c, key) or launch_block)
    bal: dict = {}
    for r in c.execute("SELECT wallet, balance FROM main_holders WHERE token=?", (token,)):
        bal[r["wallet"]] = int(r["balance"])
    CH = 100_000
    while frm <= head:
        to = min(head, frm + CH - 1)
        logs = chain.rpc(chain.PUBLIC_RPC, "eth_getLogs", [{"address": token, "topics": [TRANSFER_TOPIC], "fromBlock": hex(frm), "toBlock": hex(to)}])
        for lg in logs:
            a = "0x" + lg["topics"][1][-40:]
            b = "0x" + lg["topics"][2][-40:]
            v = int(lg["data"], 16)
            bal[a] = bal.get(a, 0) - v
            bal[b] = bal.get(b, 0) + v
        frm = to + 1
    c.executemany("INSERT OR REPLACE INTO main_holders(token,wallet,balance) VALUES(?,?,?)", [(token, w, str(v)) for w, v in bal.items()])
    db.set_progress(c, key, head)
    c.commit()


def holder_shares(c, w3: Web3, main: dict, excluded: set) -> list:
    rows = [(r["wallet"], int(r["balance"])) for r in c.execute("SELECT wallet, balance FROM main_holders WHERE token=?", (main["address"],))]
    code_cache: dict = {}
    keep = []
    for w, b in rows:
        if b <= 0 or w in excluded:
            continue
        if _is_contract(w3, w, code_cache):          # pools, curves, routers: never paid
            continue
        keep.append((w, b))
    total = sum(b for _, b in keep)
    if not total:
        return []
    keep.sort(key=lambda x: -x[1])
    out = [(w, b / total) for w, b in keep if b / total >= MIN_SHARE][:MAX_RECIPIENTS]
    s = sum(x[1] for x in out)
    return [(w, sh / s) for w, sh in out]        # renormalised over who actually gets paid


def pick_target(c, quote: str, head: int) -> dict | None:
    """Live streams first, else streams of the last 24 h, same quote token as the
    main coin; the one with the largest 24 h buy+sell volume wins."""
    now = int(time.time())
    live = c.execute("SELECT t.* FROM streams s JOIN tokens t ON t.address=s.address WHERE s.live=1 AND t.pair_token=? AND t.curve!='' AND t.graduated=0", (quote,)).fetchall()
    cands = live or c.execute("SELECT t.* FROM streams s JOIN tokens t ON t.address=s.address WHERE s.ended_ts>? AND t.pair_token=? AND t.curve!='' AND t.graduated=0", (now - 86400, quote)).fetchall()
    if not cands:
        return None
    since = head - int(86400 / market.BLOCK_SECONDS)
    best, best_vol = None, -1
    for t in cands:
        try:
            frm = market.trade_start(c, t["curve"], head)
            if frm is not None and frm <= head:
                rows, to = market.fetch_trades(t["curve"], max(frm, since), head, max_chunks=3)
                market.store_trades(c, t["curve"], rows, to)
        except Exception as e:  # noqa: BLE001
            _log("volume scan skipped", t["symbol"], e)
        vol = c.execute("SELECT COALESCE(SUM(CAST(quote AS REAL)),0) AS v FROM trades WHERE curve=? AND block>=?", (t["curve"], since)).fetchone()["v"]
        if vol > best_vol:
            best, best_vol = dict(t), vol
    return best


def _send(w3: Web3, acct, tx: dict) -> str:
    tx.setdefault("chainId", 4663)
    tx["nonce"] = w3.eth.get_transaction_count(acct.address, "pending")
    if "maxFeePerGas" not in tx and "gasPrice" not in tx:      # build_transaction already priced it (EIP-1559); only legacy txs need gasPrice
        tx["gasPrice"] = int(w3.eth.gas_price * 1.2)
    if "gas" not in tx:
        tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.3)
    signed = acct.sign_transaction(tx)
    h = w3.eth.send_raw_transaction(signed.raw_transaction)
    rc = w3.eth.wait_for_transaction_receipt(h, timeout=180)
    if rc["status"] != 1:
        raise RuntimeError("tx reverted " + h.hex())
    return h.hex()


def cycle(c) -> None:
    w3 = _w3()
    acct = Account.from_key(_key())
    me = acct.address
    main = main_coin(c)
    if not main:
        _log("no main coin set (admin ca)"); return
    quote = (main["pair_token"] or ZERO).lower()
    native = quote == ZERO
    head = w3.eth.block_number

    # 0. claim what PONS holds for us in the fee escrow (needs a little ETH for gas)
    esc = w3.eth.contract(Web3.to_checksum_address(FEE_ESCROW), abi=ESCROW)
    claimable = esc.functions.balanceOf(me).call() if native else esc.functions.balanceOfToken(me, Web3.to_checksum_address(quote)).call()
    if claimable > 0:
        if w3.eth.get_balance(me) < CLAIM_GAS_MIN * 1e18:
            _log(f"claimable {claimable / 1e18:.6f} but no ETH for gas"); return
        fn = esc.functions.claim(claimable) if native else esc.functions.claimToken(Web3.to_checksum_address(quote), claimable)
        txh = _send(w3, acct, fn.build_transaction({"from": me}))
        _log(f"claimed {claimable / (10 ** (18 if native else chain.decimals(quote))):.6f} from escrow tx {txh}")

    # 1. fees = balance delta, fail-closed
    if native:
        bal = w3.eth.get_balance(me)
    else:
        bal = w3.eth.contract(Web3.to_checksum_address(quote), abi=ERC20).functions.balanceOf(me).call()
    qdec = 18 if native else chain.decimals(quote)
    unit = 10 ** qdec
    seen = db.get_progress(c, "fee_seen:" + quote)
    seen = int(seen) if seen is not None else 0
    new = max(0, bal - seen)
    pool = int(db.get_progress(c, "fee_pool:" + quote) or 0) + int(new * SHARE)
    db.set_progress(c, "fee_seen:" + quote, bal)
    db.set_progress(c, "fee_pool:" + quote, pool)
    c.commit()
    _log(f"balance {bal / unit:.6f} new {new / unit:.6f} pool {pool / unit:.6f}")
    if pool < MIN_BUY * unit:
        return
    amount = min(pool, int(MAX_BUY * unit))
    eth_bal = w3.eth.get_balance(me)
    if native and eth_bal - amount < GAS_RESERVE * 1e18:
        _log("holding: would eat the gas reserve"); return
    if not native and eth_bal < GAS_RESERVE * 1e18:
        _log("holding: no ETH for gas"); return

    # 2. target
    target = pick_target(c, quote, head)
    if not target:
        _log("no streaming coin with this quote yet; pool kept"); return
    curve = w3.eth.contract(Web3.to_checksum_address(target["curve"]), abi=CURVE)
    if curve.functions.graduated().call():
        _log("target graduated, skipping", target["symbol"]); return
    q_res, t_res = curve.functions.getReserves().call()
    expect = t_res * amount // (q_res + amount) if q_res + amount else 0
    min_out = expect * 90 // 100
    tok = w3.eth.contract(Web3.to_checksum_address(target["address"]), abi=ERC20)
    before = tok.functions.balanceOf(me).call()

    run_id = c.execute("INSERT INTO fee_runs(ts,status,quote,fees_new,spent,target,target_symbol) VALUES(?,?,?,?,?,?,?)",
                       (int(time.time()), "buying", quote, str(new), str(amount), target["address"], target["symbol"])).lastrowid
    c.commit()
    try:
        # 3. buy on the curve
        if native:
            txh = _send(w3, acct, curve.functions.buy(amount, min_out, me).build_transaction({"from": me, "value": amount}))
        else:
            qc = w3.eth.contract(Web3.to_checksum_address(quote), abi=ERC20)
            _send(w3, acct, qc.functions.approve(Web3.to_checksum_address(target["curve"]), amount).build_transaction({"from": me}))
            txh = _send(w3, acct, curve.functions.buy(amount, min_out, me).build_transaction({"from": me, "value": 0}))
        bought = tok.functions.balanceOf(me).call() - before
        if bought <= 0:
            raise RuntimeError("buy landed but no tokens arrived")
        pool -= amount
        db.set_progress(c, "fee_pool:" + quote, pool)
        c.execute("UPDATE fee_runs SET status='distributing', bought=?, tx_buy=? WHERE id=?", (str(bought), txh, run_id))
        c.commit()
        _log(f"bought {bought} {target['symbol']} for {amount / unit:.6f} tx {txh}")

        # 4. holders of the main coin
        sync_holders(c, w3, main["address"], int(main["block"]), head)
        excluded = {ZERO, me.lower(), main["curve"].lower(), chain.PONS_CORE.lower(), LAUNCHER, target["curve"].lower()}
        shares = holder_shares(c, w3, main, excluded)
        if not shares:
            raise RuntimeError("no eligible holders")
        paid = 0
        for w, sh in shares:
            amt = int(bought * sh)
            if amt <= 0:
                continue
            h = _send(w3, acct, tok.functions.transfer(Web3.to_checksum_address(w), amt).build_transaction({"from": me}))
            c.execute("INSERT INTO fee_payouts(run_id,wallet,amount,tx,ts) VALUES(?,?,?,?,?)", (run_id, w, str(amt), h, int(time.time())))
            c.commit()
            paid += 1
        c.execute("UPDATE fee_runs SET status='done', recipients=? WHERE id=?", (paid, run_id))
        c.commit()
        _log(f"paid {paid} holders")
    except Exception as e:  # noqa: BLE001
        c.execute("UPDATE fee_runs SET status='error', note=? WHERE id=?", (str(e)[:300], run_id))
        c.commit()
        raise
    finally:
        # what we spent (and the gas) must not read as "new fees" next time
        try:
            now_bal = w3.eth.get_balance(me) if native else w3.eth.contract(Web3.to_checksum_address(quote), abi=ERC20).functions.balanceOf(me).call()
            db.set_progress(c, "fee_seen:" + quote, now_bal); c.commit()
        except Exception:  # noqa: BLE001
            pass


def summary(c) -> dict:
    runs = [dict(r) for r in c.execute("SELECT id,ts,status,note,quote,spent,target,target_symbol,bought,recipients,tx_buy FROM fee_runs ORDER BY id DESC LIMIT 12")]
    tot = c.execute("SELECT COUNT(*) AS n, COALESCE(SUM(recipients),0) AS r FROM fee_runs WHERE status='done'").fetchone()
    main = main_coin(c)
    return {"enabled": enabled(), "wallet": wallet_address(), "main": main["address"] if main else "", "main_symbol": main["symbol"] if main else "",
            "runs": runs, "done": tot["n"], "recipients": tot["r"]}


def thread() -> None:
    if not enabled():
        _log("disabled: FEE_WALLET_KEY not set"); return
    c = db.connect()
    c.executescript(SCHEMA)
    _log("engine up, wallet", wallet_address(), f"share {SHARE} max {MAX_BUY} cycle {CYCLE}s")
    while True:
        try:
            cycle(c)
        except Exception:  # noqa: BLE001
            traceback.print_exc()
        time.sleep(CYCLE)


def start() -> None:
    threading.Thread(target=thread, daemon=True, name="fee-engine").start()
