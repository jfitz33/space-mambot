import sqlite3, json, time
from typing import Tuple, List, Dict, Any
from core.state import AppState

def db_init(state: AppState):
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_collection (
            user_id     TEXT NOT NULL,
            card_name   TEXT NOT NULL,
            card_qty    INTEGER NOT NULL DEFAULT 0,
            card_rarity TEXT NOT NULL,
            card_set    TEXT NOT NULL,
            card_code   TEXT,
            card_id     TEXT,
            PRIMARY KEY (user_id, card_name, card_rarity, card_set, card_code, card_id)
        );
        """)

def db_add_cards(state: AppState, user_id: int, items: list[dict], pack_name: str):
    from collections import Counter
    key = lambda it: (it.get("name",""), it.get("rarity","").lower(), it.get("card_code",""), it.get("card_id",""))
    counts = Counter(key(it) for it in items)
    with sqlite3.connect(state.db_path) as conn, conn:
        for (name, rarity, code, cid), qty in counts.items():
            conn.execute("""
            INSERT INTO user_collection (user_id, card_name, card_qty, card_rarity, card_set, card_code, card_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, card_name, card_rarity, card_set, card_code, card_id)
            DO UPDATE SET card_qty = card_qty + excluded.card_qty;
            """, (str(user_id), name, qty, rarity, pack_name, code, cid))

def db_get_collection(state: AppState, user_id: int):
    with sqlite3.connect(state.db_path) as conn:
        c = conn.cursor()
        c.execute("""
        SELECT card_name, card_qty, card_rarity, card_set, COALESCE(card_code,''), COALESCE(card_id,'')
        FROM user_collection
        WHERE user_id = ?
        ORDER BY
          CASE LOWER(card_rarity)
            WHEN 'secret' THEN 1 WHEN 'ultra' THEN 2 WHEN 'super' THEN 3
            WHEN 'rare' THEN 4 WHEN 'uncommon' THEN 5 WHEN 'common' THEN 6
            ELSE 999 END,
          card_name COLLATE NOCASE ASC, card_set COLLATE NOCASE ASC;
        """, (str(user_id),))
        return c.fetchall()

def db_clear_collection(state: AppState, user_id: int) -> int:
    with sqlite3.connect(state.db_path) as conn, conn:
        cur = conn.execute("DELETE FROM user_collection WHERE user_id=?", (str(user_id),))
        return cur.rowcount

# --- Admin helpers ---
def db_admin_add_card(state: AppState, user_id: int, *, name: str, rarity: str, card_set: str, card_code: str, card_id: str, qty: int) -> int:
    rarity = (rarity or "").strip().lower()
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
        INSERT INTO user_collection (user_id, card_name, card_qty, card_rarity, card_set, card_code, card_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, card_name, card_rarity, card_set, card_code, card_id)
        DO UPDATE SET card_qty = card_qty + excluded.card_qty;
        """, (str(user_id), name, max(1, int(qty)), rarity, card_set, card_code or "", card_id or ""))
        cur = conn.execute("""
        SELECT card_qty FROM user_collection
        WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
          AND COALESCE(card_code,'')=COALESCE(?,'') AND COALESCE(card_id,'')=COALESCE(?,'');
        """, (str(user_id), name, rarity, card_set, card_code or "", card_id or ""))
        row = cur.fetchone()
        return int(row[0]) if row else qty

def db_admin_remove_card(state: AppState, user_id: int, *, name: str, rarity: str, card_set: str, card_code: str, card_id: str, qty: int) -> Tuple[int,int]:
    rarity = (rarity or "").strip().lower()
    with sqlite3.connect(state.db_path) as conn, conn:
        cur = conn.execute("""
        SELECT card_qty FROM user_collection
        WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
          AND COALESCE(card_code,'')=COALESCE(?,'') AND COALESCE(card_id,'')=COALESCE(?,'');
        """, (str(user_id), name, rarity, card_set, card_code or "", card_id or ""))
        row = cur.fetchone()
        if not row:
            return (0,0)
        current = int(row[0]); new_qty = current - max(1, int(qty))
        if new_qty > 0:
            conn.execute("""
            UPDATE user_collection SET card_qty=?
            WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
              AND COALESCE(card_code,'')=COALESCE(?,'') AND COALESCE(card_id,'')=COALESCE(?,'');
            """, (new_qty, str(user_id), name, rarity, card_set, card_code or "", card_id or ""))
            return (current - new_qty, new_qty)
        else:
            conn.execute("""
            DELETE FROM user_collection
            WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
              AND COALESCE(card_code,'')=COALESCE(?,'') AND COALESCE(card_id,'')=COALESCE(?,'');
            """, (str(user_id), name, rarity, card_set, card_code or "", card_id or ""))
            return (current, 0)

# --- Trades: table + migration ---
def db_init_trades(state: AppState):
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            proposer_id TEXT NOT NULL,
            receiver_id TEXT NOT NULL,
            status      TEXT NOT NULL,
            give_json   TEXT NOT NULL,
            get_json    TEXT NOT NULL,
            created_ts  INTEGER NOT NULL,
            note        TEXT,
            confirm_proposer INTEGER NOT NULL DEFAULT 0,
            confirm_receiver INTEGER NOT NULL DEFAULT 0,
            dm_chan_prop TEXT,
            dm_msg_prop  TEXT,
            dm_chan_recv TEXT,
            dm_msg_recv  TEXT,
            public_chan_id TEXT,   -- NEW
            public_msg_id  TEXT    -- NEW
        );
        """)
        for col, ddl in [
            ("confirm_proposer", "ALTER TABLE trades ADD COLUMN confirm_proposer INTEGER NOT NULL DEFAULT 0"),
            ("confirm_receiver", "ALTER TABLE trades ADD COLUMN confirm_receiver INTEGER NOT NULL DEFAULT 0"),
            ("dm_chan_prop",     "ALTER TABLE trades ADD COLUMN dm_chan_prop TEXT"),
            ("dm_msg_prop",      "ALTER TABLE trades ADD COLUMN dm_msg_prop TEXT"),
            ("dm_chan_recv",     "ALTER TABLE trades ADD COLUMN dm_chan_recv TEXT"),
            ("dm_msg_recv",      "ALTER TABLE trades ADD COLUMN dm_msg_recv TEXT"),
            ("public_chan_id",   "ALTER TABLE trades ADD COLUMN public_chan_id TEXT"),
            ("public_msg_id",    "ALTER TABLE trades ADD COLUMN public_msg_id TEXT"),
        ]:
            try:
                conn.execute(f"SELECT {col} FROM trades LIMIT 1")
            except sqlite3.OperationalError:
                conn.execute(ddl)

def db_trade_create(state: AppState, proposer_id: int, receiver_id: int, give_items: list[dict], note: str="") -> int:
    with sqlite3.connect(state.db_path) as conn, conn:
        cur = conn.execute("""
            INSERT INTO trades (proposer_id, receiver_id, status, give_json, get_json, created_ts, note,
                                confirm_proposer, confirm_receiver)
            VALUES (?, ?, 'awaiting_receiver', ?, '[]', ?, ?, 0, 0)
        """, (str(proposer_id), str(receiver_id), json.dumps(give_items), int(time.time()), note))
        return cur.lastrowid

def db_trade_get(state: AppState, trade_id: int) -> dict | None:
    with sqlite3.connect(state.db_path) as conn:
        c = conn.cursor()
        c.execute("""
          SELECT trade_id, proposer_id, receiver_id, status, give_json, get_json,
                 created_ts, note, confirm_proposer, confirm_receiver,
                 dm_chan_prop, dm_msg_prop, dm_chan_recv, dm_msg_recv,
                 public_chan_id, public_msg_id
          FROM trades WHERE trade_id=?
        """, (trade_id,))
        r = c.fetchone()
        if not r: return None
        return {
            "trade_id": r[0], "proposer_id": r[1], "receiver_id": r[2], "status": r[3],
            "give": json.loads(r[4] or "[]"), "get": json.loads(r[5] or "[]"),
            "created_ts": r[6], "note": r[7],
            "confirm_proposer": int(r[8]) == 1, "confirm_receiver": int(r[9]) == 1,
            "dm_chan_prop": r[10], "dm_msg_prop": r[11],
            "dm_chan_recv": r[12], "dm_msg_recv": r[13],
            "public_chan_id": r[14], "public_msg_id": r[15],
        }

def db_trade_store_public_message(state: AppState, trade_id: int, chan_id: int, msg_id: int):
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
          UPDATE trades SET public_chan_id=?, public_msg_id=? WHERE trade_id=?
        """, (str(chan_id), str(msg_id), trade_id))

def db_trade_get_active_for_user(state: AppState, user_id: int):
    with sqlite3.connect(state.db_path) as conn:
        c = conn.cursor()
        c.execute("""
          SELECT trade_id FROM trades
          WHERE status IN ('awaiting_receiver','awaiting_confirm')
            AND (proposer_id=? OR receiver_id=?)
          ORDER BY created_ts DESC
          LIMIT 1
        """, (str(user_id), str(user_id)))
        row = c.fetchone()
        if not row: return None
        return db_trade_get(state, int(row[0]))

def db_trade_set_receiver_offer(state: AppState, trade_id: int, receiver_id: int, get_items: list[dict]):
    with sqlite3.connect(state.db_path) as conn, conn:
        # ensure receiver matches and status is awaiting_receiver
        cur = conn.execute("SELECT receiver_id, status FROM trades WHERE trade_id=?", (trade_id,))
        r = cur.fetchone()
        if not r: raise ValueError("Trade not found")
        if str(r[0]) != str(receiver_id): raise PermissionError("Only the receiver can offer")
        if r[1] != "awaiting_receiver": raise ValueError("Trade not awaiting receiver offer")
        conn.execute("""
            UPDATE trades
            SET get_json=?, status='awaiting_confirm', confirm_proposer=0, confirm_receiver=0
            WHERE trade_id=?
        """, (json.dumps(get_items), trade_id))

def db_trade_set_confirm(state: AppState, trade_id: int, user_id: int) -> bool:
    with sqlite3.connect(state.db_path) as conn, conn:
        cur = conn.execute("SELECT proposer_id, receiver_id, confirm_proposer, confirm_receiver, status FROM trades WHERE trade_id=?", (trade_id,))
        r = cur.fetchone()
        if not r: raise ValueError("Trade not found")
        if r[4] != "awaiting_confirm": raise ValueError("Trade not awaiting confirmation")
        proposer_id, receiver_id, cp, cr, _ = r
        if str(user_id) == str(proposer_id):
            conn.execute("UPDATE trades SET confirm_proposer=1 WHERE trade_id=?", (trade_id,))
        elif str(user_id) == str(receiver_id):
            conn.execute("UPDATE trades SET confirm_receiver=1 WHERE trade_id=?", (trade_id,))
        else:
            raise PermissionError("Only participants can confirm")
        cur2 = conn.execute("SELECT confirm_proposer, confirm_receiver FROM trades WHERE trade_id=?", (trade_id,))
        cp2, cr2 = cur2.fetchone()
        return int(cp2) == 1 and int(cr2) == 1

def db_trade_set_status(state: AppState, trade_id: int, status: str):
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("UPDATE trades SET status=? WHERE trade_id=?", (status, trade_id))

def db_trade_cancel(state: AppState, trade_id: int):
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("UPDATE trades SET status='canceled' WHERE trade_id=?", (trade_id,))

# --- Validate availability for items (exact row key match) ---
def db_user_has_items(state: AppState, user_id: int, items: List[dict]) -> tuple[bool, str]:
    with sqlite3.connect(state.db_path) as conn:
        for it in items:
            name = it["name"]; rarity = it["rarity"]; cset = it["card_set"]
            code = it.get("card_code","") or ""; cid = it.get("card_id","") or ""
            qty  = int(it.get("qty", 0))
            cur = conn.execute("""
              SELECT card_qty FROM user_collection
              WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                AND COALESCE(card_code,'')=? AND COALESCE(card_id,'')=?;
            """, (str(user_id), name, rarity, cset, code, cid))
            row = cur.fetchone()
            have = int(row[0]) if row else 0
            if have < qty:
                return (False, f"{name} ({rarity}, {cset}) need {qty}, have {have}")
    return (True, "ok")

# --- Atomic swap (same as earlier pattern) ---
def db_apply_trade_atomic(state: AppState, trade: dict) -> tuple[bool, str]:
    proposer = trade["proposer_id"]; receiver = trade["receiver_id"]
    give = trade["give"]; get = trade["get"]
    try:
        conn = sqlite3.connect(state.db_path)
        conn.isolation_level = None
        conn.execute("BEGIN IMMEDIATE;")

        # re-check availability
        ok, msg = db_user_has_items(state, proposer, give)
        if not ok: conn.execute("ROLLBACK;"); conn.close(); return (False, f"Proposer lacks items: {msg}")
        ok, msg = db_user_has_items(state, receiver, get)
        if not ok: conn.execute("ROLLBACK;"); conn.close(); return (False, f"Receiver lacks items: {msg}")

        def dec(user, items):
            for it in items:
                name, rarity, cset = it["name"], it["rarity"], it["card_set"]
                code = it.get("card_code","") or ""; cid = it.get("card_id","") or ""
                qty  = int(it["qty"])
                cur = conn.execute("""
                  SELECT card_qty FROM user_collection
                  WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                    AND COALESCE(card_code,'')=? AND COALESCE(card_id,'')=?;
                """, (str(user), name, rarity, cset, code, cid))
                row = cur.fetchone()
                have = int(row[0]) if row else 0
                newq = have - qty
                if newq < 0:
                    raise RuntimeError("Race: negative quantity")
                if newq == 0:
                    conn.execute("""
                      DELETE FROM user_collection
                      WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                        AND COALESCE(card_code,'')=? AND COALESCE(card_id,'')=?;
                    """, (str(user), name, rarity, cset, code, cid))
                else:
                    conn.execute("""
                      UPDATE user_collection SET card_qty=?
                      WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                        AND COALESCE(card_code,'')=? AND COALESCE(card_id,'')=?;
                    """, (newq, str(user), name, rarity, cset, code, cid))

        def inc(user, items):
            for it in items:
                name, rarity, cset = it["name"], it["rarity"], it["card_set"]
                code = it.get("card_code","") or ""; cid = it.get("card_id","") or ""
                qty  = int(it["qty"])
                conn.execute("""
                  INSERT INTO user_collection (user_id, card_name, card_qty, card_rarity, card_set, card_code, card_id)
                  VALUES (?, ?, ?, ?, ?, ?, ?)
                  ON CONFLICT(user_id, card_name, card_rarity, card_set, card_code, card_id)
                  DO UPDATE SET card_qty = card_qty + excluded.card_qty;
                """, (str(user), name, qty, rarity, cset, code, cid))

        # proposer -> receiver
        dec(proposer, give); inc(receiver, give)
        # receiver -> proposer
        dec(receiver, get); inc(proposer, get)

        conn.execute("COMMIT;")
        conn.close()
        return (True, "ok")
    except Exception as e:
        try:
            conn.execute("ROLLBACK;"); conn.close()
        except: pass
        return (False, f"Trade failed: {e}")

