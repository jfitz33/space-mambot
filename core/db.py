import sqlite3, json, time, asyncio
from typing import Tuple, List, Dict, Any, Optional, Iterable
from core.state import AppState
from core.util_norm import normalize_rarity, normalize_set_name, blank_to_none

def conn(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(path)
    c.execute("PRAGMA foreign_keys = ON;")
    return c

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
        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_wishlist (
            user_id     TEXT NOT NULL,
            card_name   TEXT NOT NULL,
            desired_qty INTEGER NOT NULL DEFAULT 0,
            card_rarity TEXT NOT NULL,
            card_set    TEXT NOT NULL,
            card_code   TEXT,
            card_id     TEXT,
            PRIMARY KEY (user_id, card_name, card_rarity, card_set, card_code, card_id)
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_binder (
            user_id     TEXT NOT NULL,
            card_name   TEXT NOT NULL,
            qty         INTEGER NOT NULL DEFAULT 0,
            card_rarity TEXT NOT NULL,
            card_set    TEXT NOT NULL,
            card_code   TEXT,
            card_id     TEXT,
            PRIMARY KEY (user_id, card_name, card_rarity, card_set, card_code, card_id)
        );
        """)

DEBUG_COLLECTION = False  # set True while testing

def db_add_cards(
    state,
    user_id: int,
    cards: Iterable[dict],
    default_set: Optional[str] = None,
) -> int:
    """
    Upsert cards into user_collection.
    Each card dict may contain: name/cardname, rarity/cardrarity, set/cardset, code/cardcode, id/cardid, qty/cardq.
    If set is missing, use default_set.
    Returns total quantity added.
    """
    total_added = 0
    user_id_s = str(user_id)
    with sqlite3.connect(state.db_path) as conn, conn:
        for raw in cards:
            name  = (raw.get("name") or raw.get("cardname") or "").strip()
            if not name:
                continue

            rarity = normalize_rarity(raw.get("rarity") or raw.get("cardrarity"))
            cset = normalize_set_name(
                (raw.get("set") or raw.get("cardset") or default_set or "")
            )
            code = blank_to_none(raw.get("code") or raw.get("cardcode"))
            cid  = blank_to_none(raw.get("id")   or raw.get("cardid"))

            # qty handling: prefer 'qty' then 'cardq', default to 1
            try:
                qty = int(raw.get("qty") if raw.get("qty") is not None else raw.get("cardq") or 1)
            except Exception:
                qty = 1
            if qty <= 0:
                continue

            if DEBUG_COLLECTION:
                print(f"[db_add_cards] user={user_id_s} name={name} rarity={rarity} set={cset} code={code} id={cid} qty={qty}")

            conn.execute(
                """
                INSERT INTO user_collection
                  (user_id, card_name, card_rarity, card_set, card_code, card_id, card_qty)
                VALUES (?,       ?,         ?,           ?,        ?,         ?,       ?)
                ON CONFLICT(user_id, card_name, card_rarity, card_set, card_code, card_id)
                DO UPDATE SET card_qty = card_qty + excluded.card_qty;
                """,
                (user_id_s, name, rarity, cset, code, cid, qty),
            )
            total_added += qty
    return total_added

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

def db_collection_clear(state, user_id: int) -> int:
    """Delete all collection rows for a user. Returns number of rows deleted."""
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("DELETE FROM user_collection WHERE user_id = ?", (str(user_id),))
        conn.execute("DELETE FROM user_binder WHERE user_id = ?", (str(user_id),))
        # sqlite3 total_changes counts all changes in this transaction (here, just the DELETE)
        return conn.total_changes

#def db_clear_collection(state: AppState, user_id: int) -> int:
#    with sqlite3.connect(state.db_path) as conn, conn:
#        cur = conn.execute("DELETE FROM user_collection WHERE user_id=?", (str(user_id),))
#        return cur.rowcount

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
    code_norm = blank_to_none(card_code)
    id_norm = blank_to_none(card_id)
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
            removed = current - new_qty
        else:
            conn.execute("""
            DELETE FROM user_collection
            WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
              AND COALESCE(card_code,'')=COALESCE(?,'') AND COALESCE(card_id,'')=COALESCE(?,'');
            """, (str(user_id), name, rarity, card_set, card_code or "", card_id or ""))
            removed = current
            new_qty = 0
        if removed > 0:
            _binder_reduce_with_conn(conn, user_id, name, rarity, card_set, code_norm, id_norm, removed)
        return (removed, new_qty)

# --- Helper functions for selling to shop ---

def _nullish(x) -> bool:
    return (x is None) or (isinstance(x, str) and x.strip() == "")

# --- Get cards in collection for autocomplet ---
def db_collection_list_owned_prints(state, user_id: int, name_filter: str | None = None, limit: int = 50):
    """
    Return owned rows grouped by exact printing (name, rarity, set, code, id).
    Each row: dict(name, rarity, set, code, id, qty)
    """
    q = """
    SELECT card_name, card_rarity, card_set, card_code, card_id, SUM(card_qty) AS qty
      FROM user_collection
     WHERE user_id = ?
    """
    params = [str(user_id)]
    if name_filter:
        q += " AND LOWER(card_name) LIKE ?"
        params.append(f"%{name_filter.strip().lower()}%")
    q += """
     GROUP BY card_name, card_rarity, card_set, card_code, card_id
     HAVING SUM(card_qty) > 0
     ORDER BY card_name ASC
     LIMIT ?
    """
    params.append(int(limit))
    out = []
    with sqlite3.connect(state.db_path) as conn:
        for row in conn.execute(q, params):
            out.append({
                "name":   row[0],
                "rarity": row[1],
                "set":    row[2],
                "code":   row[3],
                "id":     row[4],
                "qty":    int(row[5] or 0),
            })
    return out

def db_collection_total_by_name_and_rarity(state, user_id: int, card_name: str, rarity: str) -> int:
    """Optional helper (not strictly required by the shop)."""
    with sqlite3.connect(state.db_path) as conn:
        cur = conn.execute(
            """SELECT COALESCE(SUM(card_qty), 0)
                 FROM user_collection
                WHERE user_id = ?
                  AND LOWER(card_name)=LOWER(?)
                  AND LOWER(card_rarity)=LOWER(?)""",
            (str(user_id), card_name, rarity),
        )
        (total,) = cur.fetchone()
        return int(total or 0)

def _normalize_card_identity(card: dict | None, *, name: str | None = None,
                              rarity: str | None = None, card_set: str | None = None,
                              card_code: str | None = None, card_id: str | None = None) -> tuple[str, str, str, str, str]:
    """Return trimmed string fields for card identity."""
    src = card or {}
    nm = (name if name is not None else src.get("name") or src.get("card_name") or src.get("cardname") or "").strip()
    rt = (rarity if rarity is not None else src.get("rarity") or src.get("card_rarity") or src.get("cardrarity") or "").strip()
    st = (card_set if card_set is not None else src.get("card_set") or src.get("set") or src.get("cardset") or "").strip()
    cd = blank_to_none(card_code if card_code is not None else src.get("card_code") or src.get("code") or src.get("cardcode"))
    cid = blank_to_none(card_id if card_id is not None else src.get("card_id") or src.get("id") or src.get("cardid"))
    return (nm, rt, st, cd or "", cid or "")


def db_wishlist_add(state, user_id: int, card: dict, qty: int) -> int:
    qty = max(1, int(qty or 1))
    name, rarity, cset, code, cid = _normalize_card_identity(card)
    user_id_s = str(user_id)
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute(
            """
            INSERT INTO user_wishlist (user_id, card_name, desired_qty, card_rarity, card_set, card_code, card_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, card_name, card_rarity, card_set, card_code, card_id)
            DO UPDATE SET desired_qty = desired_qty + excluded.desired_qty;
            """,
            (user_id_s, name, qty, rarity, cset, code or None, cid or None),
        )
        row = conn.execute(
            """
            SELECT desired_qty FROM user_wishlist
             WHERE user_id=?
               AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
               AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
               AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
               AND (card_code IS ? OR card_code=?)
               AND (card_id IS ? OR card_id=?);
            """,
            (user_id_s, name, rarity, cset, code or None, code or None, cid or None, cid or None),
        ).fetchone()
        return int(row[0]) if row else qty


def db_wishlist_remove(state, user_id: int, card: dict, qty: int | None = None) -> tuple[int, int]:
    amount = 1 if qty is None else max(1, int(qty))
    name, rarity, cset, code, cid = _normalize_card_identity(card)
    user_id_s = str(user_id)
    with sqlite3.connect(state.db_path) as conn, conn:
        row = conn.execute(
            """
            SELECT desired_qty FROM user_wishlist
             WHERE user_id=?
               AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
               AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
               AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
               AND (card_code IS ? OR card_code=?)
               AND (card_id IS ? OR card_id=?);
            """,
            (user_id_s, name, rarity, cset, code or None, code or None, cid or None, cid or None),
        ).fetchone()
        if not row:
            return (0, 0)
        current = int(row[0] or 0)
        take = min(current, amount)
        remaining = current - take
        if remaining > 0:
            conn.execute(
                """
                UPDATE user_wishlist SET desired_qty=?
                 WHERE user_id=?
                   AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
                   AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
                   AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
                   AND (card_code IS ? OR card_code=?)
                   AND (card_id IS ? OR card_id=?);
                """,
                (remaining, user_id_s, name, rarity, cset, code or None, code or None, cid or None, cid or None),
            )
        else:
            conn.execute(
                """
                DELETE FROM user_wishlist
                 WHERE user_id=?
                   AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
                   AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
                   AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
                   AND (card_code IS ? OR card_code=?)
                   AND (card_id IS ? OR card_id=?);
                """,
                (user_id_s, name, rarity, cset, code or None, code or None, cid or None, cid or None),
            )
        return (take, max(0, remaining))


def db_wishlist_list(state, user_id: int) -> List[dict]:
    out: List[dict] = []
    with sqlite3.connect(state.db_path) as conn:
        cur = conn.execute(
            """
            SELECT card_name, desired_qty, card_rarity, card_set, card_code, card_id
              FROM user_wishlist
             WHERE user_id = ?
             ORDER BY card_set COLLATE NOCASE ASC, card_name COLLATE NOCASE ASC;
            """,
            (str(user_id),),
        )
        for name, qty, rarity, cset, code, cid in cur.fetchall():
            out.append({
                "card_name": name,
                "qty": int(qty or 0),
                "card_rarity": rarity,
                "card_set": cset,
                "card_code": code,
                "card_id": cid,
            })
    return out


def db_wishlist_clear(state, user_id: int) -> int:
    with sqlite3.connect(state.db_path) as conn, conn:
        cur = conn.execute("DELETE FROM user_wishlist WHERE user_id=?", (str(user_id),))
        return cur.rowcount or 0


def db_wishlist_holders(state, card: dict) -> List[dict]:
    """Return users and desired quantities for a specific card on wishlists."""
    name, rarity, cset, code, cid = _normalize_card_identity(card)
    out: List[dict] = []
    with sqlite3.connect(state.db_path) as conn:
        cur = conn.execute(
            """
            SELECT user_id, desired_qty
              FROM user_wishlist
             WHERE LOWER(TRIM(card_name))   = LOWER(TRIM(?))
               AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
               AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
               AND (card_code IS ? OR card_code=?)
               AND (card_id   IS ? OR card_id=?);
            """,
            (name, rarity, cset, code or None, code or None, cid or None, cid or None),
        )
        for user_id, qty in cur.fetchall():
            try:
                qty_int = int(qty or 0)
            except Exception:
                qty_int = 0
            if qty_int <= 0:
                continue
            out.append({"user_id": str(user_id), "qty": qty_int})
    return out


def db_binder_add(state, user_id: int, card: dict, qty: int) -> int:
    qty = max(1, int(qty or 1))
    name, rarity, cset, code, cid = _normalize_card_identity(card)
    user_id_s = str(user_id)
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute(
            """
            INSERT INTO user_binder (user_id, card_name, qty, card_rarity, card_set, card_code, card_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, card_name, card_rarity, card_set, card_code, card_id)
            DO UPDATE SET qty = qty + excluded.qty;
            """,
            (user_id_s, name, qty, rarity, cset, code or None, cid or None),
        )
        row = conn.execute(
            """
            SELECT qty FROM user_binder
             WHERE user_id=?
               AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
               AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
               AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
               AND (card_code IS ? OR card_code=?)
               AND (card_id IS ? OR card_id=?);
            """,
            (user_id_s, name, rarity, cset, code or None, code or None, cid or None, cid or None),
        ).fetchone()
        return int(row[0]) if row else qty


def _binder_reduce_with_conn(conn: sqlite3.Connection, user_id: int, name: str, rarity: str,
                             card_set: str, code: str | None, cid: str | None, amount: int) -> int:
    if amount <= 0:
        return 0
    user_id_s = str(user_id)
    code_norm = blank_to_none(code)
    cid_norm = blank_to_none(cid)
    row = conn.execute(
        """
        SELECT qty FROM user_binder
         WHERE user_id=?
           AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
           AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
           AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
           AND (card_code IS ? OR card_code=?)
           AND (card_id IS ? OR card_id=?);
        """,
        (user_id_s, name, rarity, card_set, code_norm, code_norm, cid_norm, cid_norm),
    ).fetchone()
    if not row:
        return 0
    current = int(row[0] or 0)
    if current <= 0:
        conn.execute(
            """
            DELETE FROM user_binder
             WHERE user_id=?
               AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
               AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
               AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
               AND (card_code IS ? OR card_code=?)
               AND (card_id IS ? OR card_id=?);
            """,
            (user_id_s, name, rarity, card_set, code_norm, code_norm, cid_norm, cid_norm),
        )
        return 0
    take = min(current, int(amount))
    remaining = current - take
    if remaining > 0:
        conn.execute(
            """
            UPDATE user_binder SET qty=?
             WHERE user_id=?
               AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
               AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
               AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
               AND (card_code IS ? OR card_code=?)
               AND (card_id IS ? OR card_id=?);
            """,
            (remaining, user_id_s, name, rarity, card_set, code_norm, code_norm, cid_norm, cid_norm),
        )
    else:
        conn.execute(
            """
            DELETE FROM user_binder
             WHERE user_id=?
               AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
               AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
               AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
               AND (card_code IS ? OR card_code=?)
               AND (card_id IS ? OR card_id=?);
            """,
            (user_id_s, name, rarity, card_set, code_norm, code_norm, cid_norm, cid_norm),
        )
    return take


def db_binder_reduce_for_card(state, user_id: int, name: str, rarity: str, card_set: str,
                              card_code: str | None, card_id: str | None, amount: int,
                              *, connection: sqlite3.Connection | None = None) -> int:
    if amount <= 0:
        return 0
    if connection is not None:
        return _binder_reduce_with_conn(connection, user_id, name, rarity, card_set, card_code, card_id, amount)
    with sqlite3.connect(state.db_path) as conn, conn:
        return _binder_reduce_with_conn(conn, user_id, name, rarity, card_set, card_code, card_id, amount)


def db_binder_remove(state, user_id: int, card: dict, qty: int | None = None) -> tuple[int, int]:
    amount = 1 if qty is None else max(1, int(qty))
    name, rarity, cset, code, cid = _normalize_card_identity(card)
    with sqlite3.connect(state.db_path) as conn, conn:
        taken = _binder_reduce_with_conn(conn, user_id, name, rarity, cset, code or None, cid or None, amount)
        if taken <= 0:
            return (0, 0)
        row = conn.execute(
            """
            SELECT qty FROM user_binder
             WHERE user_id=?
               AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
               AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
               AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
               AND (card_code IS ? OR card_code=?)
               AND (card_id IS ? OR card_id=?);
            """,
            (str(user_id), name, rarity, cset, code or None, code or None, cid or None, cid or None),
        ).fetchone()
        remaining = int(row[0]) if row else 0
        return (taken, remaining)


def db_binder_list(state, user_id: int) -> List[dict]:
    out: List[dict] = []
    with sqlite3.connect(state.db_path) as conn:
        cur = conn.execute(
            """
            SELECT card_name, qty, card_rarity, card_set, card_code, card_id
              FROM user_binder
             WHERE user_id = ?
             ORDER BY card_set COLLATE NOCASE ASC, card_name COLLATE NOCASE ASC;
            """,
            (str(user_id),),
        )
        for name, qty, rarity, cset, code, cid in cur.fetchall():
            out.append({
                "card_name": name,
                "qty": int(qty or 0),
                "card_rarity": rarity,
                "card_set": cset,
                "card_code": code,
                "card_id": cid,
            })
    return out


def db_binder_clear(state, user_id: int) -> int:
    with sqlite3.connect(state.db_path) as conn, conn:
        cur = conn.execute("DELETE FROM user_binder WHERE user_id=?", (str(user_id),))
        return cur.rowcount or 0


def db_binder_holders(state, card: dict) -> List[dict]:
    """Return users and binder quantities for a specific card."""
    name, rarity, cset, code, cid = _normalize_card_identity(card)
    out: List[dict] = []
    with sqlite3.connect(state.db_path) as conn:
        cur = conn.execute(
            """
            SELECT user_id, qty
              FROM user_binder
             WHERE LOWER(TRIM(card_name))   = LOWER(TRIM(?))
               AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
               AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
               AND (card_code IS ? OR card_code=?)
               AND (card_id   IS ? OR card_id=?);
            """,
            (name, rarity, cset, code or None, code or None, cid or None, cid or None),
        )
        for user_id, qty in cur.fetchall():
            try:
                qty_int = int(qty or 0)
            except Exception:
                qty_int = 0
            if qty_int <= 0:
                continue
            out.append({"user_id": str(user_id), "qty": qty_int})
    return out

def _blank_to_none(s):
    return None if s is None or str(s).strip() == "" else str(s).strip()

def db_collection_debug_dump(state, user_id: int, name: str, rarity: str, card_set: str):
    import sqlite3
    out = []
    with sqlite3.connect(state.db_path) as conn:
        for row in conn.execute(
            """
            SELECT rowid, card_name, card_rarity, card_set,
                   COALESCE(card_code,''), COALESCE(card_id,''), card_qty
            FROM user_collection
            WHERE user_id = ?
              AND LOWER(TRIM(card_name))   = LOWER(TRIM(?))
              AND LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))
              AND LOWER(TRIM(card_set))    = LOWER(TRIM(?))
            ORDER BY rowid ASC
            """,
            (str(user_id), name, rarity.lower().strip(), card_set),
        ):
            out.append(row)
    return out

def db_collection_remove_exact_print(
    state,
    user_id: int,
    *,
    card_name: str,
    card_rarity: str,
    card_set: str,
    card_code: str | None,
    card_id: str | None,
    amount: int = 1,
) -> int:
    """
    Remove up to `amount` copies of a printing from user_collection.
    - Matches by user_id + (name, rarity, set).
    - If card_code / card_id are provided, they are matched exactly (case/space-insensitive).
    - If either is blank (None/''), that field is *ignored* for matching, and we prefer rows
      where that field is blank in the DB, falling back to any matching row otherwise.
    Uses SQLite rowid to update/delete exactly 1 row.
    """
    if amount <= 0:
        return 0

    user_id_s = str(user_id)
    name   = (card_name or "").strip()
    rarity = (card_rarity or "").strip().lower()
    cset   = (card_set or "").strip()
    code_in = _blank_to_none(card_code)
    id_in   = _blank_to_none(card_id)

    # Build the probe query
    base_where = [
        "user_id = ?",
        "LOWER(TRIM(card_name))   = LOWER(TRIM(?))",
        "LOWER(TRIM(card_rarity)) = LOWER(TRIM(?))",
        "LOWER(TRIM(card_set))    = LOWER(TRIM(?))",
    ]
    params = [user_id_s, name, rarity, cset]

    # Only constrain by code/id if they were provided
    if code_in is not None:
        base_where.append("LOWER(TRIM(card_code)) = LOWER(TRIM(?))")
        params.append(code_in)
    if id_in is not None:
        base_where.append("LOWER(TRIM(card_id)) = LOWER(TRIM(?))")
        params.append(id_in)

    where_sql = " AND ".join(base_where)

    # If caller left code/id blank, prefer blank rows first
    order_bits = []
    if id_in is None:
        order_bits.append("CASE WHEN TRIM(COALESCE(card_id,'')) = '' THEN 0 ELSE 1 END")
    if code_in is None:
        order_bits.append("CASE WHEN TRIM(COALESCE(card_code,'')) = '' THEN 0 ELSE 1 END")
    order_sql = (" ORDER BY " + ", ".join(order_bits)) if order_bits else ""

    with sqlite3.connect(state.db_path) as conn, conn:
        row = conn.execute(
            f"""
            SELECT rowid, card_qty, card_code, card_id
              FROM user_collection
             WHERE {where_sql}
             {order_sql}
             LIMIT 1;
            """,
            params,
        ).fetchone()

        if not row:
            return 0

        rowid, cur_qty, db_code, db_id = int(row[0]), int(row[1] or 0), row[2], row[3]
        if cur_qty <= 0:
            return 0

        take = min(amount, cur_qty)
        if take == cur_qty:
            conn.execute("DELETE FROM user_collection WHERE rowid = ?;", (rowid,))
        else:
            conn.execute("UPDATE user_collection SET card_qty = card_qty - ? WHERE rowid = ?;", (take, rowid))
        _binder_reduce_with_conn(conn, user_id, name, rarity, cset, db_code, db_id, take)
        return take
    
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
def db_user_has_items(state, user_id: int, items: list[dict]) -> tuple[bool, str]:
    """
    Returns (ok, message). Supports both card items and shard entries:
      - Card: {name, rarity, card_set, card_code, card_id, qty}
      - Shards: {"kind":"shards", "set_id": int, "amount": int}
    """
    import sqlite3
    from core.db import db_shards_get

    # Check shards first
    for it in items:
        if (it or {}).get("kind") == "shards":
            sid = int(it.get("set_id", 0) or 0)
            amt = int(it.get("amount", 0) or 0)
            if sid <= 0 or amt <= 0:
                return (False, "invalid shards entry")
            have = db_shards_get(state, user_id, sid)
            if have < amt:
                return (False, f"needs {amt} {sid}, has {have}")

    # Check cards (exact print rows)
    with sqlite3.connect(state.db_path) as conn:
        for it in items:
            if (it or {}).get("kind") == "shards":
                continue
            name = it["name"]; rarity = it["rarity"]; cset = it["card_set"]
            code = it.get("card_code") or None
            cid  = it.get("card_id") or None
            need = int(it["qty"])
            row = conn.execute("""
                SELECT COALESCE(card_qty,0) FROM user_collection
                 WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                   AND (card_code IS ? OR card_code=?) AND (card_id IS ? OR card_id=?)
            """, (str(user_id), name, rarity, cset, code, code, cid, cid)).fetchone()
            have = int(row[0] if row else 0)
            if have < need:
                return (False, f"needs x{need} {name} [{rarity}] in {cset}, has {have}")
    return (True, "")


# --- Atomic swap for trades ---
def db_apply_trade_atomic(state, t: dict) -> tuple[bool, str]:
    """
    Applies a trade atomically. Input `t` is a full trade row dict with:
      t["proposer_id"], t["receiver_id"], t["give"] (list), t["get"] (list).
    Card rows: {name, rarity, card_set, card_code, card_id, qty}
    Shard rows: {"kind":"shards","set_id":int,"amount":int}
    """
    import sqlite3
    from core.db import db_shards_add

    A = str(t["proposer_id"])
    B = str(t["receiver_id"])
    give = t.get("give", []) or []
    get  = t.get("get", []) or []

    try:
        with sqlite3.connect(state.db_path) as conn, conn:
            # 1) Remove A's card items; credit to B
            for it in give:
                if (it or {}).get("kind") == "shards":
                    continue
                # remove from A
                conn.execute("""
                    UPDATE user_collection
                       SET card_qty = card_qty - ?
                     WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                       AND (card_code IS ? OR card_code=?) AND (card_id IS ? OR card_id=?)
                       AND card_qty >= ?;
                """, (int(it["qty"]), A, it["name"], it["rarity"], it["card_set"],
                      it.get("card_code") or None, it.get("card_code") or None,
                      it.get("card_id") or None, it.get("card_id") or None,
                      int(it["qty"])))
                if conn.total_changes <= 0:
                    raise RuntimeError(f"proposer missing {it['name']} x{it['qty']}")
                _binder_reduce_with_conn(conn, int(A), it["name"], it["rarity"], it["card_set"], it.get("card_code"), it.get("card_id"), int(it["qty"]))

                # add to B (upsert)
                row = conn.execute("""
                    SELECT card_qty FROM user_collection
                     WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                       AND (card_code IS ? OR card_code=?) AND (card_id IS ? OR card_id=?)
                """, (B, it["name"], it["rarity"], it["card_set"],
                      it.get("card_code") or None, it.get("card_code") or None,
                      it.get("card_id") or None, it.get("card_id") or None)).fetchone()
                if row:
                    conn.execute("""
                        UPDATE user_collection
                           SET card_qty = card_qty + ?
                         WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                           AND (card_code IS ? OR card_code=?) AND (card_id IS ? OR card_id=?)
                    """, (int(it["qty"]), B, it["name"], it["rarity"], it["card_set"],
                          it.get("card_code") or None, it.get("card_code") or None,
                          it.get("card_id") or None, it.get("card_id") or None))
                else:
                    conn.execute("""
                        INSERT INTO user_collection (user_id, card_name, card_rarity, card_set, card_code, card_id, card_qty)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (B, it["name"], it["rarity"], it["card_set"],
                          it.get("card_code") or None, it.get("card_id") or None, int(it["qty"])))

            # 2) Remove B's card items; credit to A
            for it in get:
                if (it or {}).get("kind") == "shards":
                    continue
                conn.execute("""
                    UPDATE user_collection
                       SET card_qty = card_qty - ?
                     WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                       AND (card_code IS ? OR card_code=?) AND (card_id IS ? OR card_id=?)
                       AND card_qty >= ?;
                """, (int(it["qty"]), B, it["name"], it["rarity"], it["card_set"],
                      it.get("card_code") or None, it.get("card_code") or None,
                      it.get("card_id") or None, it.get("card_id") or None,
                      int(it["qty"])))
                if conn.total_changes <= 0:
                    raise RuntimeError(f"receiver missing {it['name']} x{it['qty']}")
                _binder_reduce_with_conn(conn, int(B), it["name"], it["rarity"], it["card_set"], it.get("card_code"), it.get("card_id"), int(it["qty"]))

                row = conn.execute("""
                    SELECT card_qty FROM user_collection
                     WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                       AND (card_code IS ? OR card_code=?) AND (card_id IS ? OR card_id=?)
                """, (A, it["name"], it["rarity"], it["card_set"],
                      it.get("card_code") or None, it.get("card_code") or None,
                      it.get("card_id") or None, it.get("card_id") or None)).fetchone()
                if row:
                    conn.execute("""
                        UPDATE user_collection
                           SET card_qty = card_qty + ?
                         WHERE user_id=? AND card_name=? AND card_rarity=? AND card_set=?
                           AND (card_code IS ? OR card_code=?) AND (card_id IS ? OR card_id=?)
                    """, (int(it["qty"]), A, it["name"], it["rarity"], it["card_set"],
                          it.get("card_code") or None, it.get("card_code") or None,
                          it.get("card_id") or None, it.get("card_id") or None))
                else:
                    conn.execute("""
                        INSERT INTO user_collection (user_id, card_name, card_rarity, card_set, card_code, card_id, card_qty)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (A, it["name"], it["rarity"], it["card_set"],
                          it.get("card_code") or None, it.get("card_id") or None, int(it["qty"])))

        # shards (non-card) are simpler and don’t need to be in the same SQL txn as cards
        # but you can also move them inside the same `with conn, conn:` if you prefer strict atomicity
        for it in give:
            if (it or {}).get("kind") == "shards":
                db_shards_add(state, int(A), int(it["set_id"]), -int(it["amount"]))
                db_shards_add(state, int(B), int(it["set_id"]),  int(it["amount"]))
        for it in get:
            if (it or {}).get("kind") == "shards":
                db_shards_add(state, int(B), int(it["set_id"]), -int(it["amount"]))
                db_shards_add(state, int(A), int(it["set_id"]),  int(it["amount"]))

        return (True, "")
    except Exception as e:
        return (False, str(e))
    
# ---- Wallet: schema + helpers ----------------------------------------------

def db_init_wallet(state):
    """Create wallet table if needed."""
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet (
            user_id   TEXT PRIMARY KEY,
            fitzcoin  INTEGER NOT NULL DEFAULT 0,
            mambucks  INTEGER NOT NULL DEFAULT 0,
            updated_ts INTEGER NOT NULL
        );
        """)

def db_wallet_get(state, user_id: int) -> dict:
    """Return {'fitzcoin': int, 'mambucks': int} (zeros if none)."""
    with sqlite3.connect(state.db_path) as conn:
        c = conn.cursor()
        c.execute("SELECT fitzcoin, mambucks FROM wallet WHERE user_id=?", (str(user_id),))
        row = c.fetchone()
        if not row:
            return {"fitzcoin": 0, "mambucks": 0}
        return {"fitzcoin": int(row[0] or 0), "mambucks": int(row[1] or 0)}

def db_wallet_set(state, user_id: int, fitzcoin: int | None = None, mambucks: int | None = None):
    """Set absolute values (only the ones provided)."""
    now = int(time.time())
    current = db_wallet_get(state, user_id)
    fz = current["fitzcoin"] if fitzcoin is None else int(fitzcoin)
    mb = current["mambucks"] if mambucks is None else int(mambucks)
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
        INSERT INTO wallet (user_id, fitzcoin, mambucks, updated_ts)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          fitzcoin=excluded.fitzcoin,
          mambucks=excluded.mambucks,
          updated_ts=excluded.updated_ts;
        """, (str(user_id), fz, mb, now))

def db_wallet_add(state, user_id: int, d_fitzcoin: int = 0, d_mambucks: int = 0) -> dict:
    """Increment balances (can be negative). Returns new balances dict."""
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn, conn:
        # Upsert row first
        conn.execute("""
        INSERT INTO wallet (user_id, fitzcoin, mambucks, updated_ts)
        VALUES (?, 0, 0, ?)
        ON CONFLICT(user_id) DO NOTHING;
        """, (str(user_id), now))
        # Apply delta
        conn.execute("""
        UPDATE wallet
           SET fitzcoin = fitzcoin + ?,
               mambucks = mambucks + ?,
               updated_ts = ?
         WHERE user_id = ?;
        """, (int(d_fitzcoin), int(d_mambucks), now, str(user_id)))
    return db_wallet_get(state, user_id)

def db_wallet_try_spend_mambucks(state, user_id: int, amount: int) -> dict | None:
    """
    Atomically spend 'amount' mambucks if user has enough.
    Returns updated balances dict on success, or None if insufficient funds.
    """
    assert amount >= 0
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn, conn:
        # Ensure a row exists
        conn.execute("""
            INSERT INTO wallet (user_id, fitzcoin, mambucks, updated_ts)
            VALUES (?, 0, 0, ?)
            ON CONFLICT(user_id) DO NOTHING;
        """, (str(user_id), now))

        # Atomic conditional spend (mirrors your fitzcoin version)
        cur = conn.execute("""
            UPDATE wallet
               SET mambucks = mambucks - ?,
                   updated_ts = ?
             WHERE user_id = ?
               AND mambucks >= ?;
        """, (int(amount), now, str(user_id), int(amount)))

        if cur.rowcount == 0:
            return None  # not enough funds

    return db_wallet_get(state, user_id)

async def db_wallet_migrate_to_mambucks_and_shards_per_set(state) -> None:
    """
    One-time migration (idempotent via app_migrations):
      - Create wallet (legacy), wallet_shards (new), app_migrations (marker)
      - Move legacy wallet.mambucks -> wallet_shards(set_id=1)  [Elemental Shards]
      - Fold wallet.fitzcoin -> wallet.mambucks                  [mambucks = pack currency]
    """
    def work():
        with conn(state.db_path) as c, c:
            # migration marker
            c.execute("""
                CREATE TABLE IF NOT EXISTS app_migrations (
                    key TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
            """)
            if c.execute("SELECT 1 FROM app_migrations WHERE key=?", ("wallet_mambucks_shards_v1",)).fetchone():
                return  # already migrated

            # legacy wallet (if absent)
            c.execute("""
                CREATE TABLE IF NOT EXISTS wallet (
                    user_id   TEXT PRIMARY KEY,
                    fitzcoin  INTEGER NOT NULL DEFAULT 0,
                    mambucks  INTEGER NOT NULL DEFAULT 0
                );
            """)

            # per-set shards
            c.execute("""
                CREATE TABLE IF NOT EXISTS wallet_shards (
                    user_id TEXT NOT NULL,
                    set_id  INTEGER NOT NULL,
                    shards  INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, set_id)
                );
            """)

            # 1) seed shards for Set 1 from legacy mambucks
            c.execute("""
                INSERT OR IGNORE INTO wallet_shards(user_id, set_id, shards)
                SELECT user_id, 1, COALESCE(mambucks,0) FROM wallet;
            """)

            # 2) fold fitzcoin into mambucks (pack currency)
            c.execute("""
                UPDATE wallet
                   SET mambucks = COALESCE(mambucks,0) + COALESCE(fitzcoin,0);
            """)

            # mark done
            c.execute("INSERT OR REPLACE INTO app_migrations(key) VALUES (?)", ("wallet_mambucks_shards_v1",))
    await asyncio.to_thread(work)

def db_shards_get(state, user_id: int, set_id: int) -> int:
    with conn(state.db_path) as c:
        row = c.execute(
            "SELECT shards FROM wallet_shards WHERE user_id=? AND set_id=?",
            (str(user_id), int(set_id))
        ).fetchone()
    return int(row[0]) if row else 0

def db_shards_add(state, user_id: int, set_id: int, d_shards: int) -> None:
    d = int(d_shards or 0)
    if d == 0:
        return
    with conn(state.db_path) as c, c:
        c.execute(
            "INSERT OR IGNORE INTO wallet_shards(user_id,set_id,shards) VALUES (?,?,0)",
            (str(user_id), int(set_id))
        )
        c.execute(
            "UPDATE wallet_shards SET shards = shards + ? WHERE user_id=? AND set_id=?",
            (d, str(user_id), int(set_id))
        )

def db_collection_list_for_bulk_fragment(
    state,
    user_id: int,
    pack_name: str,
    rarity: str,
    keep: int
) -> List[Dict]:
    """
    Returns rows of exact prints to fragment, each:
      {
        "name": str,
        "qty": int,
        "to_frag": int,  # qty - keep, clamped >= 0
        "rarity": str,
        "set": str,
        "code": str|None,
        "id": str|None,
      }
    Only includes rows where to_frag > 0.
    NOTE: Starlight is handled at the caller level; this function does not enforce rarity rules.
    """
    out: List[Dict] = []
    with sqlite3.connect(state.db_path) as conn:
        cur = conn.execute(
            """
            SELECT card_name, card_qty, card_rarity, card_set, card_code, card_id
              FROM user_collection
             WHERE user_id = ?
               AND card_set = ?
               AND LOWER(card_rarity) = ?
            """,
            (str(user_id), pack_name, rarity.lower().strip())
        )
        for name, qty, r, cset, code, cid in cur.fetchall():
            qty = int(qty or 0)
            to_frag = max(0, qty - int(keep))
            if to_frag <= 0:
                continue
            out.append({
                "name": name,
                "qty": qty,
                "to_frag": to_frag,
                "rarity": rarity.lower().strip(),
                "set": cset,
                "code": (code or None),
                "id": (cid or None),
            })
    return out

def db_init_shard_overrides(state):
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS shard_overrides (
            oid           INTEGER PRIMARY KEY AUTOINCREMENT,
            card_name     TEXT NOT NULL,
            card_set      TEXT NOT NULL,
            card_rarity   TEXT,
            card_code     TEXT,
            card_id       TEXT,
            yield_override INTEGER NOT NULL,    -- absolute shards per copy
            starts_at     INTEGER NOT NULL,     -- epoch seconds
            ends_at       INTEGER NOT NULL,     -- epoch seconds
            reason        TEXT,
            created_at    INTEGER NOT NULL
        );
        """)
        # Helpful indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shard_overrides_key ON shard_overrides(card_name, card_set);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shard_overrides_time ON shard_overrides(ends_at, starts_at);")

def db_shard_override_set(
    state,
    *,
    card_name: str,
    card_set: str,
    yield_override: int,
    duration_seconds: int,
    starts_at: Optional[int] = None,
    card_rarity: Optional[str] = None,
    card_code: Optional[str] = None,
    card_id: Optional[str] = None,
    reason: Optional[str] = None,
) -> int:
    """Create a new timed override. Returns row id."""
    now = int(time.time())
    start = int(starts_at or now)
    end = start + max(1, int(duration_seconds))
    with sqlite3.connect(state.db_path) as conn, conn:
        cur = conn.execute("""
            INSERT INTO shard_overrides
                (card_name, card_set, card_rarity, card_code, card_id,
                 yield_override, starts_at, ends_at, reason, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            card_name, card_set, (card_rarity or None),
            (card_code or None), (card_id or None),
            int(yield_override), start, end, (reason or None), now
        ))
        return int(cur.lastrowid)

def db_shard_override_clear(
    state,
    *,
    card_name: str,
    card_set: str,
    card_code: Optional[str] = None,
    card_id: Optional[str] = None,
) -> int:
    """
    Remove overrides for a specific printing if code/id provided;
    otherwise remove any overrides for (name+set). Returns rows deleted.
    """
    with sqlite3.connect(state.db_path) as conn, conn:
        if card_code or card_id:
            cur = conn.execute("""
                DELETE FROM shard_overrides
                WHERE card_name=? AND card_set=? AND (card_code IS ? OR card_code=?) AND (card_id IS ? OR card_id=?)
            """, (card_name, card_set, None if not card_code else None, card_code or None,
                  None if not card_id else None, card_id or None))
        else:
            cur = conn.execute("""
                DELETE FROM shard_overrides
                WHERE card_name=? AND card_set=?
            """, (card_name, card_set))
        return int(cur.rowcount)

def db_shard_override_list_active(state) -> list[Dict[str, Any]]:
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("""
            SELECT * FROM shard_overrides
            WHERE starts_at<=? AND ends_at>=?
            ORDER BY ends_at ASC
        """, (now, now))
        return [dict(r) for r in cur.fetchall()]

def db_shard_override_match_for_print(state, *, name: str, set_name: str,
                                      rarity: Optional[str], code: Optional[str], cid: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Return the most specific active override for this printing (if any):
      1) exact code+id
      2) exact code
      3) exact id
      4) name+set+rarity
      5) name+set
    """
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn:
        conn.row_factory = sqlite3.Row
        # Try in order of specificity
        queries = [
            ("code+id",  "card_code IS ? AND card_id IS ?"),
            ("code",     "card_code IS ?"),
            ("id",       "card_id IS ?"),
            ("n+s+r",    "card_rarity IS ?"),
            ("n+s",      "1=1"),
        ]
        params_sets = [
            (code or None, cid or None),
            (code or None,),
            (cid or None,),
            ((rarity or None),),
            tuple(),
        ]
        base = """
            SELECT * FROM shard_overrides
             WHERE card_name=? AND card_set=?
               AND starts_at<=? AND ends_at>=?
               AND {where}
             ORDER BY ends_at DESC LIMIT 1
        """
        for (_, where), p in zip(queries, params_sets):
            cur = conn.execute(base.format(where=where),
                               (name, set_name, now, now, *p))
            row = cur.fetchone()
            if row:
                return dict(row)
    return None

def db_fragment_yield_for_card(state, card: dict, set_name: str) -> tuple[int, Optional[Dict[str, Any]]]:
    """
    Compute the per-copy shard yield for this printing, honoring any active override.
    Returns (yield_each, override_row_or_None).
    """
    from core.constants import SHARD_YIELD_BY_RARITY  # your canonical map
    from core.cards_shop import is_starter_set

    if is_starter_set(set_name):
        return (0, None)
    rarity = (card.get("rarity") or card.get("cardrarity") or "").strip().lower()
    if rarity == "starlight":
        return (0, None)  # not fragmentable

    base = int(SHARD_YIELD_BY_RARITY.get(rarity, 0))
    name = (card.get("name") or card.get("cardname") or "").strip()
    code = (card.get("code") or card.get("cardcode")) or None
    cid  = (card.get("id")   or card.get("cardid"))   or None

    ov = db_shard_override_match_for_print(state,
                                           name=name, set_name=set_name,
                                           rarity=rarity, code=code, cid=cid)
    if ov:
        return (int(ov["yield_override"]), ov)
    return (base, None)

# --- User stats helpers ---
def db_init_user_stats(state):
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id    TEXT PRIMARY KEY,
            wins       INTEGER NOT NULL DEFAULT 0,
            losses     INTEGER NOT NULL DEFAULT 0,
            games      INTEGER NOT NULL DEFAULT 0,
            updated_ts INTEGER NOT NULL DEFAULT 0
        );
        """)

def db_stats_get(state, user_id: int) -> dict:
    with sqlite3.connect(state.db_path) as conn:
        row = conn.execute(
            "SELECT wins, losses, games FROM user_stats WHERE user_id=?",
            (str(user_id),)
        ).fetchone()
    if not row:
        return {"wins": 0, "losses": 0, "games": 0}
    return {"wins": int(row[0] or 0), "losses": int(row[1] or 0), "games": int(row[2] or 0)}

def db_stats_record_loss(state, loser_id: int, winner_id: int) -> tuple[dict, dict]:
    """
    Record a single match where `loser_id` lost to `winner_id`.
    Updates user_stats and appends to match_log atomically.
    Returns (loser_stats_after, winner_stats_after).
    """
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn, conn:
        # ensure rows exist
        conn.execute("INSERT OR IGNORE INTO user_stats (user_id, updated_ts) VALUES (?,?)", (str(loser_id), now))
        conn.execute("INSERT OR IGNORE INTO user_stats (user_id, updated_ts) VALUES (?,?)", (str(winner_id), now))

        # update aggregates
        conn.execute("""
            UPDATE user_stats
               SET losses = losses + 1,
                   games  = games  + 1,
                   updated_ts = ?
             WHERE user_id = ?;""", (now, str(loser_id)))
        conn.execute("""
            UPDATE user_stats
               SET wins   = wins   + 1,
                   games  = games  + 1,
                   updated_ts = ?
             WHERE user_id = ?;""", (now, str(winner_id)))

        # log the match
        conn.execute(
            "INSERT INTO match_log (ts, winner_id, loser_id) VALUES (?,?,?)",
            (now, str(winner_id), str(loser_id)),
        )

        lrow = conn.execute("SELECT wins, losses, games FROM user_stats WHERE user_id=?", (str(loser_id),)).fetchone()
        wrow = conn.execute("SELECT wins, losses, games FROM user_stats WHERE user_id=?", (str(winner_id),)).fetchone()

    loser = {"wins": int(lrow[0]), "losses": int(lrow[1]), "games": int(lrow[2])}
    winner = {"wins": int(wrow[0]), "losses": int(wrow[1]), "games": int(wrow[2])}
    return loser, winner

def db_init_match_log(state):
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS match_log (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        INTEGER NOT NULL,
            winner_id TEXT NOT NULL,
            loser_id  TEXT NOT NULL
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_match_log_pair ON match_log(winner_id, loser_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_match_log_ts ON match_log(ts);")

def db_match_log_insert(state, winner_id: int, loser_id: int):
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute(
            "INSERT INTO match_log (ts, winner_id, loser_id) VALUES (?,?,?)",
            (now, str(winner_id), str(loser_id)),
        )

def db_match_h2h(state, a_id: int, b_id: int) -> dict:
    """Return {'a_wins': int, 'b_wins': int, 'games': int} for A vs B."""
    with sqlite3.connect(state.db_path) as conn:
        (a_wins,) = conn.execute(
            "SELECT COUNT(*) FROM match_log WHERE winner_id=? AND loser_id=?",
            (str(a_id), str(b_id)),
        ).fetchone()
        (b_wins,) = conn.execute(
            "SELECT COUNT(*) FROM match_log WHERE winner_id=? AND loser_id=?",
            (str(b_id), str(a_id)),
        ).fetchone()
    a_wins = int(a_wins or 0)
    b_wins = int(b_wins or 0)
    return {"a_wins": a_wins, "b_wins": b_wins, "games": a_wins + b_wins}

# --- Daily craft sales -------------------------------------------------------
# schema
def db_init_daily_sales(state):
    import sqlite3
    with sqlite3.connect(state.db_path) as conn, conn:
        # Ensure daily_sales table exists with the updated schema that supports
        # multiple slots per rarity. Earlier versions used (day_key, rarity) as
        # the primary key which prevents storing multiple rows for the same
        # rarity. When migrating, backfill slot_index=0 for legacy rows.
        info = list(conn.execute("PRAGMA table_info(daily_sales)").fetchall())
        needs_migration = False

        if info and not any(row[1] == "slot_index" for row in info):
            conn.execute("ALTER TABLE daily_sales RENAME TO daily_sales_old")
            info = []
            needs_migration = True

        if not info:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_sales (
                day_key       TEXT NOT NULL,           -- 'YYYYMMDD' in America/New_York
                rarity        TEXT NOT NULL,           -- common/rare/super/ultra/secret
                slot_index    INTEGER NOT NULL,        -- 0-based index per rarity
                card_name     TEXT NOT NULL,
                card_set      TEXT NOT NULL,
                card_code     TEXT,
                card_id       TEXT,
                discount_pct  INTEGER NOT NULL,        -- e.g. 10
                price_shards  INTEGER NOT NULL,        -- discounted per-copy shard price
                created_ts    INTEGER NOT NULL,
                PRIMARY KEY (day_key, rarity, slot_index)
            );
            """)

        if needs_migration:
            conn.execute("""
                INSERT INTO daily_sales (
                    day_key, rarity, slot_index, card_name, card_set, card_code,
                    card_id, discount_pct, price_shards, created_ts
                )
                SELECT
                    day_key, rarity, 0 AS slot_index, card_name, card_set, card_code,
                    card_id, discount_pct, price_shards, created_ts
                  FROM daily_sales_old
            """)
            conn.execute("DROP TABLE daily_sales_old")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS shop_banner (
            guild_id   TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            updated_ts INTEGER NOT NULL
        );
        """)

def db_sales_get_for_day(state, day_key: str) -> dict:
    import sqlite3
    from collections import defaultdict

    rows = defaultdict(list)
    with sqlite3.connect(state.db_path) as conn:
        c = conn.execute("""
            SELECT rarity, card_name, card_set, card_code, card_id,
                   discount_pct, price_shards, slot_index
              FROM daily_sales
             WHERE day_key = ?
            ORDER BY created_ts ASC, slot_index ASC, rowid ASC
        """, (day_key,))
        for r in c.fetchall():
            rarity = (r[0] or "").strip().lower()
            rows[rarity].append({
                "rarity": rarity,
                "card_name": r[1],
                "card_set": r[2],
                "card_code": r[3],
                "card_id":   r[4],
                "discount_pct": int(r[5]),
                "price_shards": int(r[6]),
                "slot_index": int(r[7]),
            })
    return {k: v for k, v in rows.items()}

def db_sales_replace_for_day(state, day_key: str, rows: list[dict]) -> None:
    import sqlite3, time
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("DELETE FROM daily_sales WHERE day_key = ?", (day_key,))
        rarity_counts: Dict[str, int] = {}
        payload = []
        for r in rows:
            rarity = (r.get("rarity") or "").strip().lower()
            next_idx = rarity_counts.get(rarity, 0)
            slot = int(r.get("slot_index", next_idx))
            rarity_counts[rarity] = max(next_idx, slot + 1)
            payload.append(
                (
                    day_key,
                    rarity,
                    slot,
                    r["card_name"],
                    r["card_set"],
                    r.get("card_code"),
                    r.get("card_id"),
                    int(r.get("discount_pct", 10)),
                    int(r["price_shards"]),
                    now,
                )
            )
        conn.executemany("""
            INSERT INTO daily_sales
             (day_key, rarity, slot_index, card_name, card_set, card_code, card_id, discount_pct, price_shards, created_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, payload)

def db_shop_banner_store(state, guild_id: int, channel_id: int, message_id: int):
    import sqlite3, time
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
        INSERT INTO shop_banner (guild_id, channel_id, message_id, updated_ts)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET
            channel_id=excluded.channel_id,
            message_id=excluded.message_id,
            updated_ts=excluded.updated_ts
        """, (str(guild_id), str(channel_id), str(message_id), now))

def db_shop_banner_load(state, guild_id: int) -> dict | None:
    import sqlite3
    with sqlite3.connect(state.db_path) as conn:
        c = conn.execute("""
            SELECT channel_id, message_id
              FROM shop_banner
             WHERE guild_id = ?
        """, (str(guild_id),))
        r = c.fetchone()
        if not r: return None
        return {"channel_id": int(r[0]), "message_id": int(r[1])}

# --- Wheel tokens ------------------------------------------------------------

def db_init_wheel_tokens(state):
    """Create the wheel_tokens table if missing."""
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS wheel_tokens (
            user_id        TEXT PRIMARY KEY,
            tokens         INTEGER NOT NULL DEFAULT 0,
            last_grant_day TEXT,                 -- 'YYYYMMDD' (America/New_York)
            updated_ts     INTEGER NOT NULL
        );
        """)

def db_wheel_tokens_get(state, user_id: int) -> int:
    with sqlite3.connect(state.db_path) as conn:
        c = conn.execute("SELECT tokens FROM wheel_tokens WHERE user_id=?", (str(user_id),))
        r = c.fetchone()
        return int(r[0]) if r else 0

def db_wheel_tokens_add(state, user_id: int, delta: int) -> int:
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
            INSERT INTO wheel_tokens (user_id, tokens, updated_ts)
            VALUES (?, 0, ?)
            ON CONFLICT(user_id) DO NOTHING;
        """, (str(user_id), now))
        conn.execute("""
            UPDATE wheel_tokens
               SET tokens = MAX(0, tokens + ?),
                   updated_ts = ?
             WHERE user_id = ?;
        """, (int(delta), now, str(user_id)))
    return db_wheel_tokens_get(state, user_id)

def db_wheel_tokens_try_spend(state, user_id: int, amount: int = 1) -> int | None:
    """Atomically spend `amount`. Returns new balance or None if insufficient."""
    assert amount > 0
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
            INSERT INTO wheel_tokens (user_id, tokens, updated_ts)
            VALUES (?, 0, ?)
            ON CONFLICT(user_id) DO NOTHING;
        """, (str(user_id), now))
        cur = conn.execute("""
            UPDATE wheel_tokens
               SET tokens = tokens - ?,
                   updated_ts = ?
             WHERE user_id = ?
               AND tokens >= ?;
        """, (int(amount), now, str(user_id), int(amount)))
        if cur.rowcount == 0:
            return None
    return db_wheel_tokens_get(state, user_id)

def db_wheel_tokens_grant_daily(state, user_id: int, day_key: str) -> tuple[int, bool]:
    """
    Idempotent daily grant. If last_grant_day != day_key, add 1 token and set it.
    Returns (new_balance, granted_bool).
    """
    now = int(time.time())
    with sqlite3.connect(state.db_path) as conn, conn:
        conn.execute("""
            INSERT INTO wheel_tokens (user_id, tokens, last_grant_day, updated_ts)
            VALUES (?, 0, NULL, ?)
            ON CONFLICT(user_id) DO NOTHING;
        """, (str(user_id), now))
        cur = conn.execute("""
            UPDATE wheel_tokens
               SET tokens = tokens + 1,
                   last_grant_day = ?,
                   updated_ts = ?
             WHERE user_id = ?
               AND (last_grant_day IS NULL OR last_grant_day <> ?);
        """, (day_key, now, str(user_id), day_key))
        granted = (cur.rowcount > 0)
    return (db_wheel_tokens_get(state, user_id), granted)
