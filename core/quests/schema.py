import asyncio, sqlite3, json
from typing import Iterable, Dict, Any, Tuple
from datetime import datetime, timezone

def _row_to_dict(row: sqlite3.Row) -> dict:
    # sqlite3.Row iterates values; use keys() to build a dict
    return {k: row[k] for k in row.keys()}

def _conn(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def _query_all(conn: sqlite3.Connection, sql: str, params=()):
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description] if cur.description else []
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def _query_one(conn: sqlite3.Connection, sql: str, params=()):
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description] if cur.description else []
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else None

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

# ---------- READ QUEST DEFS ----------
async def db_fetch_active_quests(state) -> list[dict]:
    def _work():
        with _conn(state.db_path) as conn:
            rows = _query_all(conn,
                "SELECT quest_id,title,description,category,target_count,reward_type,reward_payload,active "
                "FROM quests WHERE active=1"
            )
        for d in rows:
            try:
                d["reward_payload"] = json.loads(d.get("reward_payload") or "{}")
            except Exception:
                d["reward_payload"] = {}
            d["active"] = bool(int(d.get("active", 1)))
            d["target_count"] = int(d.get("target_count", 1))
        return rows
    return await asyncio.to_thread(_work)

# ---------- GET USER PROGRESS ----------
async def db_get_user_progress(state, user_id: int, quest_ids, period_key: str) -> dict:
    qids = list(quest_ids or [])
    if not qids:
        return {}
    placeholders = ",".join("?" * len(qids))
    params = [user_id, period_key, *qids]

    def _work():
        with _conn(state.db_path) as conn:
            rows = _query_all(conn,
                f"""SELECT quest_id, progress, completed_at, claimed_at, claimed_steps
                    FROM user_quest_progress
                    WHERE user_id=? AND period_key=? AND quest_id IN ({placeholders})""",
                params
            )
        return {r["quest_id"]: r for r in rows}
    return await asyncio.to_thread(_work)

# ---------- UPSERT PROGRESS ----------
async def db_upsert_progress(state, user_id: int, quest_id: str, period_key: str, delta: int, target: int):
    delta = int(delta or 0)
    target = max(1, int(target or 1))
    now_iso = _now_iso()

    def _work():
        with _conn(state.db_path) as conn:
            conn.execute("BEGIN")
            conn.execute(
                "INSERT OR IGNORE INTO user_quest_progress(user_id,quest_id,period_key,progress) VALUES (?,?,?,0)",
                (user_id, quest_id, period_key)
            )
            conn.execute(
                """UPDATE user_quest_progress
                      SET progress = MIN(progress + ?, ?),
                          completed_at = CASE
                              WHEN (progress + ?) >= ? AND completed_at IS NULL THEN ?
                              ELSE completed_at
                          END
                    WHERE user_id=? AND quest_id=? AND period_key=?""",
                (delta, target, delta, target, now_iso, user_id, quest_id, period_key)
            )
            row = _query_one(conn,
                "SELECT progress, completed_at FROM user_quest_progress WHERE user_id=? AND quest_id=? AND period_key=?",
                (user_id, quest_id, period_key)
            )
            conn.commit()
        prog = int(row.get("progress", 0)) if row else 0
        completed = bool(row.get("completed_at")) if row else False
        return prog, completed

    return await asyncio.to_thread(_work)

# ---------- MARK CLAIMED ----------
async def db_mark_claimed(state, user_id: int, quest_id: str, period_key: str) -> bool:
    now_iso = _now_iso()
    def _work():
        with _conn(state.db_path) as conn:
            cur = conn.execute(
                """UPDATE user_quest_progress
                      SET claimed_at = ?
                    WHERE user_id=? AND quest_id=? AND period_key=? AND claimed_at IS NULL""",
                (now_iso, user_id, quest_id, period_key)
            )
            conn.commit()
            return cur.rowcount > 0
    return await asyncio.to_thread(_work)

# ---------- Initialize Quest Table on startup ----------
async def db_init_quests(state) -> None:
    def _work():
        with _conn(state.db_path) as conn, conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS quests (
              quest_id        TEXT PRIMARY KEY,
              title           TEXT NOT NULL,
              description     TEXT NOT NULL,
              category        TEXT NOT NULL CHECK (category IN ('daily','weekly','permanent')),
              target_count    INTEGER NOT NULL DEFAULT 1,
              reward_type     TEXT NOT NULL,
              reward_payload  TEXT,
              active          INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS user_quest_progress (
              user_id       INTEGER NOT NULL,
              quest_id      TEXT    NOT NULL,
              period_key    TEXT    NOT NULL,
              progress      INTEGER NOT NULL DEFAULT 0,
              completed_at  TEXT,
              claimed_at    TEXT,
              PRIMARY KEY (user_id, quest_id, period_key),
              FOREIGN KEY (quest_id) REFERENCES quests(quest_id)
            );

            CREATE INDEX IF NOT EXISTS idx_uqp_user_period
              ON user_quest_progress(user_id, period_key);
            """)
            # --- migration: add claimed_steps for multi-part quests ---
            cols = [r[1] for r in conn.execute("PRAGMA table_info(user_quest_progress)").fetchall()]
            if "claimed_steps" not in cols:
                conn.execute("ALTER TABLE user_quest_progress ADD COLUMN claimed_steps INTEGER NOT NULL DEFAULT 0")
    await asyncio.to_thread(_work)

async def db_seed_example_quests(state) -> None:
    """
    Keep your original examples if you like, but here’s a NEW daily multi-part quest
    that rewards at 1/5/10/24 packs. You can disable your old 'open_5_packs' to avoid overlap.
    """
    open_packs_multi = {
        "quest_id": "open_packs",
        "title": "Open Packs",
        "description": "Open packs today to earn multiple rewards.",
        "category": "daily",
        "target_count": 24,               
        "reward_type": "mambucks",        # ignored for milestones; kept for compatibility
        "reward_payload": json.dumps({
            "milestones": [
                {"count": 1,  "reward": {"type": "mambucks", "amount": 10}},
                {"count": 5,  "reward": {"type": "mambucks", "amount": 10}},
                {"count": 10, "reward": {"type": "mambucks", "amount": 10}},
                {"count": 24, "reward": {"type": "mambucks", "amount": 10}},
            ]
        }),
        "active": 1,
    }

    def _work():
        with _conn(state.db_path) as conn, conn:
            conn.execute(
                """INSERT OR REPLACE INTO quests
                   (quest_id,title,description,category,target_count,reward_type,reward_payload,active)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    open_packs_multi["quest_id"],
                    open_packs_multi["title"],
                    open_packs_multi["description"],
                    open_packs_multi["category"],
                    open_packs_multi["target_count"],
                    open_packs_multi["reward_type"],
                    open_packs_multi["reward_payload"],
                    open_packs_multi["active"],
                )
            )
            # Optional: deactivate the older single-step seed, if you had it
            try:
                conn.execute("UPDATE quests SET active=0 WHERE quest_id='open_5_packs'")
            except Exception:
                pass
    await asyncio.to_thread(_work)

async def db_mark_claimed_step(state, user_id: int, quest_id: str, period_key: str, expect_steps: int) -> bool:
    """Optimistic bump of claimed_steps to prevent double claims under race."""
    def _work():
        with _conn(state.db_path) as conn, conn:
            cur = conn.execute(
                """UPDATE user_quest_progress
                      SET claimed_steps = claimed_steps + 1
                    WHERE user_id=? AND quest_id=? AND period_key=? AND claimed_steps=?""",
                (user_id, quest_id, period_key, expect_steps)
            )
            return cur.rowcount > 0
    return await asyncio.to_thread(_work)

async def db_reset_all_user_quests(state, user_id: int) -> int:
    """Delete ALL quest progress rows for this user (all periods & quests). Returns rows deleted."""
    def _work():
        with _conn(state.db_path) as conn, conn:
            cur = conn.execute("DELETE FROM user_quest_progress WHERE user_id = ?", (user_id,))
            return cur.rowcount or 0
    return await asyncio.to_thread(_work)
