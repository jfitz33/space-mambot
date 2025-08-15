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

# --- Trades table skeleton (init only, for future) ---
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
            note        TEXT
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);")
