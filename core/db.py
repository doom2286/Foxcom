import sqlite3
import hashlib
import secrets
import threading
from datetime import datetime, timezone, timedelta

from .utils import utc_now_iso, parse_iso_utc

DB_FILE = "botdatabase"

# Rep voting window / message tracking retention
# (Your reputation cog expects a 4-hour window.)
REP_TTL_SECONDS = 4 * 60 * 60  # 4 hours

PBKDF2_ITERS = 200_000
PBKDF2_SALT_LEN = 16

_DB_WRITE_LOCK = threading.Lock()


def connect() -> sqlite3.Connection:
    # timeout makes sqlite wait for locks instead of failing immediately
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=30000;")  # 30s
    except Exception:
        pass
    return conn


# -----------------------
# Schema helpers
# -----------------------
def _table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def _table_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    if not _table_exists(cur, table):
        return set()
    cur.execute(f"PRAGMA table_info({table})")
    return {row["name"] for row in cur.fetchall()}


def _ensure_column(cur: sqlite3.Cursor, table: str, colname: str, ddl: str):
    cols = _table_columns(cur, table)
    if colname not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _safe_exec(cur: sqlite3.Cursor, sql: str, params: tuple = ()):
    try:
        cur.execute(sql, params)
    except sqlite3.OperationalError:
        pass


def _migrate(cur: sqlite3.Cursor):
    # banlist
    if _table_exists(cur, "banlist"):
        cols = _table_columns(cur, "banlist")
        _ensure_column(cur, "banlist", "blocked", "blocked INTEGER NOT NULL DEFAULT 1")
        _ensure_column(cur, "banlist", "user_name", "user_name TEXT")
        _ensure_column(cur, "banlist", "blocked_by", "blocked_by TEXT")
        _ensure_column(cur, "banlist", "blocked_at", "blocked_at TEXT")
        _ensure_column(cur, "banlist", "reason", "reason TEXT")
        if "username" in cols:
            _safe_exec(
                cur,
                "UPDATE banlist SET user_name = COALESCE(NULLIF(user_name,''), username) "
                "WHERE user_name IS NULL OR user_name=''"
            )

    # rep_users
    if _table_exists(cur, "rep_users"):
        cols = _table_columns(cur, "rep_users")
        _ensure_column(cur, "rep_users", "rep", "rep INTEGER NOT NULL DEFAULT 0")
        _ensure_column(cur, "rep_users", "user_name", "user_name TEXT")
        _ensure_column(cur, "rep_users", "last_updated", "last_updated TEXT")
        if "username" in cols:
            _safe_exec(
                cur,
                "UPDATE rep_users SET user_name = COALESCE(NULLIF(user_name,''), username) "
                "WHERE user_name IS NULL OR user_name=''"
            )
        if "last_seen" in cols:
            _safe_exec(
                cur,
                "UPDATE rep_users SET last_updated = COALESCE(NULLIF(last_updated,''), last_seen) "
                "WHERE last_updated IS NULL OR last_updated=''"
            )
        if "set_at" in cols:
            _safe_exec(
                cur,
                "UPDATE rep_users SET last_updated = COALESCE(NULLIF(last_updated,''), set_at) "
                "WHERE last_updated IS NULL OR last_updated=''"
            )

    # rep_messages
    if _table_exists(cur, "rep_messages"):
        cols = _table_columns(cur, "rep_messages")
        _ensure_column(cur, "rep_messages", "author_id", "author_id INTEGER")
        _ensure_column(cur, "rep_messages", "author_name", "author_name TEXT")
        _ensure_column(cur, "rep_messages", "created_at", "created_at TEXT")
        if "user_id" in cols:
            _safe_exec(cur, "UPDATE rep_messages SET author_id = COALESCE(author_id, user_id) WHERE author_id IS NULL")
        if "username" in cols:
            _safe_exec(
                cur,
                "UPDATE rep_messages SET author_name = COALESCE(NULLIF(author_name,''), username) "
                "WHERE author_name IS NULL OR author_name=''"
            )

    # rep_votes
    if _table_exists(cur, "rep_votes"):
        cols = _table_columns(cur, "rep_votes")
        _ensure_column(cur, "rep_votes", "vote", "vote INTEGER NOT NULL DEFAULT 0")
        if "delta" in cols:
            _safe_exec(cur, "UPDATE rep_votes SET vote = delta WHERE (vote IS NULL OR vote = 0) AND delta IS NOT NULL")

    # maintenance
    if _table_exists(cur, "maintenance"):
        _ensure_column(cur, "maintenance", "last_prune_at", "last_prune_at TEXT")


def init():
    with _DB_WRITE_LOCK:
        conn = connect()
        cur = conn.cursor()

        cur.execute("CREATE TABLE IF NOT EXISTS channels (guild_id INTEGER PRIMARY KEY, channel_id INTEGER NOT NULL)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS approved_servers (
            guild_id INTEGER PRIMARY KEY,
            approved INTEGER NOT NULL DEFAULT 0,
            regiment TEXT,
            server_name TEXT,
            requested_by TEXT,
            approved_by TEXT,
            approved_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_requests (
            guild_id INTEGER PRIMARY KEY,
            server_name TEXT,
            submitted_by TEXT,
            regiment TEXT,
            submitted_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS feedback_config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            channel_id INTEGER
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS banlist (
            user_id INTEGER PRIMARY KEY,
            blocked INTEGER NOT NULL DEFAULT 1,
            user_name TEXT,
            blocked_by TEXT,
            blocked_at TEXT,
            reason TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS rep_users (
            user_id INTEGER PRIMARY KEY,
            rep INTEGER NOT NULL DEFAULT 0,
            user_name TEXT,
            last_updated TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS rep_messages (
            message_id INTEGER PRIMARY KEY,
            author_id INTEGER NOT NULL,
            author_name TEXT,
            created_at TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS rep_votes (
            message_id INTEGER NOT NULL,
            voter_id INTEGER NOT NULL,
            vote INTEGER NOT NULL,
            PRIMARY KEY (message_id, voter_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_actions (
            user_id INTEGER NOT NULL,
            used_at TEXT NOT NULL
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_broadcast_actions_user_time ON broadcast_actions(user_id, used_at)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS maintenance (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_prune_at TEXT
        )
        """)
        cur.execute("INSERT OR IGNORE INTO maintenance (id, last_prune_at) VALUES (1, NULL)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            group_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL UNIQUE COLLATE NOCASE,
            visibility      TEXT NOT NULL CHECK (visibility IN ('public','private')),
            password_salt   BLOB,
            password_hash   BLOB,
            owner_user_id   INTEGER NOT NULL,
            created_at      TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_user_roles (
            group_id    INTEGER NOT NULL,
            user_id     INTEGER NOT NULL,
            role        TEXT NOT NULL CHECK (role IN ('owner','leader','member')),
            created_at  TEXT NOT NULL,
            PRIMARY KEY (group_id, user_id),
            FOREIGN KEY (group_id) REFERENCES groups(group_id) ON DELETE CASCADE
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_servers (
            group_id    INTEGER NOT NULL,
            guild_id    INTEGER NOT NULL,
            guild_name  TEXT NOT NULL,
            joined_at   TEXT NOT NULL,
            PRIMARY KEY (group_id, guild_id),
            FOREIGN KEY (group_id) REFERENCES groups(group_id) ON DELETE CASCADE
        )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_group_servers_guild ON group_servers(guild_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_group_roles_user ON group_user_roles(user_id)")

        _migrate(cur)

        conn.commit()
        conn.close()


# -----------------------
# Utility
# -----------------------
def within_rep_window(created_at_iso: str) -> bool:
    """
    True if created_at is within REP_TTL_SECONDS.
    """
    t = parse_iso_utc(created_at_iso)
    if not t:
        return False
    now = datetime.now(timezone.utc)
    return (now - t).total_seconds() <= REP_TTL_SECONDS


# -----------------------
# Reads (no lock)
# -----------------------
def is_guild_approved(guild_id: int) -> bool:
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT approved FROM approved_servers WHERE guild_id=?", (int(guild_id),))
    r = cur.fetchone()
    conn.close()
    return bool(r and int(r["approved"]) == 1)


def get_regiment(guild_id: int) -> str | None:
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT regiment FROM approved_servers WHERE guild_id=? AND approved=1", (int(guild_id),))
    r = cur.fetchone()
    conn.close()
    return r["regiment"] if r else None


def all_channels():
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT guild_id, channel_id FROM channels")
    rows = cur.fetchall()
    conn.close()
    return [(int(r["guild_id"]), int(r["channel_id"])) for r in rows]


def get_pending(guild_id: int):
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT * FROM pending_requests WHERE guild_id=?", (int(guild_id),))
    r = cur.fetchone()
    conn.close()
    return r


def list_approved():
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT * FROM approved_servers WHERE approved=1 ORDER BY approved_at DESC")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_feedback_channel():
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT channel_id FROM feedback_config WHERE id=1")
    r = cur.fetchone()
    conn.close()
    return int(r["channel_id"]) if r and r["channel_id"] else None


def get_rep(user_id: int) -> int:
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT rep FROM rep_users WHERE user_id=?", (int(user_id),))
    r = cur.fetchone()
    conn.close()
    return int(r["rep"]) if r else 0


def leaderboard(limit: int = 10):
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT user_id, rep, user_name FROM rep_users ORDER BY rep DESC LIMIT ?", (int(limit),))
    rows = cur.fetchall()
    conn.close()
    return rows


def counts():
    conn = connect(); cur = conn.cursor()
    tables = [
        "channels", "approved_servers", "pending_requests", "feedback_config", "banlist",
        "rep_users", "rep_messages", "rep_votes", "broadcast_actions", "maintenance",
        "groups", "group_user_roles", "group_servers",
    ]
    out = {}
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) AS c FROM {t}")
            out[t] = int(cur.fetchone()["c"])
        except Exception:
            out[t] = 0
    conn.close()
    return out


def get_last_prune():
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT last_prune_at FROM maintenance WHERE id=1")
    r = cur.fetchone()
    conn.close()
    return r["last_prune_at"] if r else None


# -----------------------
# Writes (locked)
# -----------------------
def set_channel(guild_id: int, channel_id: int):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO channels (guild_id, channel_id) VALUES (?,?)",
            (int(guild_id), int(channel_id))
        )
        conn.commit(); conn.close()


def set_pending(guild_id: int, server_name: str, submitted_by: str, regiment: str, submitted_at: str):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("""INSERT OR REPLACE INTO pending_requests
                       (guild_id, server_name, submitted_by, regiment, submitted_at)
                       VALUES (?,?,?,?,?)""",
                    (int(guild_id), server_name, submitted_by, regiment, submitted_at))
        conn.commit(); conn.close()


def delete_pending(guild_id: int):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("DELETE FROM pending_requests WHERE guild_id=?", (int(guild_id),))
        conn.commit(); conn.close()


def approve_guild(guild_id: int, regiment: str, server_name: str, requested_by: str, approved_by: str, approved_at: str):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("""INSERT OR REPLACE INTO approved_servers
                       (guild_id, approved, regiment, server_name, requested_by, approved_by, approved_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (int(guild_id), 1, regiment, server_name, requested_by, approved_by, approved_at))
        conn.commit(); conn.close()


def clear_approved():
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("DELETE FROM approved_servers")
        conn.commit(); conn.close()


def set_feedback_channel(channel_id: int):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO feedback_config (id, channel_id) VALUES (1,?)", (int(channel_id),))
        conn.commit(); conn.close()


def is_user_blocked(user_id: int) -> bool:
    conn = connect(); cur = conn.cursor()
    try:
        cur.execute("SELECT blocked FROM banlist WHERE user_id=?", (int(user_id),))
        r = cur.fetchone()
        return bool(r and int(r["blocked"]) == 1)
    except sqlite3.OperationalError:
        cur.execute("SELECT 1 FROM banlist WHERE user_id=?", (int(user_id),))
        r = cur.fetchone()
        return r is not None
    finally:
        conn.close()


def block_user(user_id: int, user_name: str, blocked_by: str, reason: str):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("""INSERT OR REPLACE INTO banlist
                       (user_id, blocked, user_name, blocked_by, blocked_at, reason)
                       VALUES (?,?,?,?,?,?)""",
                    (int(user_id), 1, user_name, blocked_by, utc_now_iso(), (reason or "").strip()))
        conn.commit(); conn.close()


def unblock_user(user_id: int) -> bool:
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("DELETE FROM banlist WHERE user_id = ?", (int(user_id),))
        conn.commit()
        ok = cur.rowcount > 0
        conn.close()
        return ok


def ensure_rep_user(user_id: int, user_name: str):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("SELECT user_id FROM rep_users WHERE user_id=?", (int(user_id),))
        r = cur.fetchone()
        if not r:
            cur.execute(
                "INSERT INTO rep_users (user_id, rep, user_name, last_updated) VALUES (?,?,?,?)",
                (int(user_id), 0, user_name, utc_now_iso())
            )
        else:
            cur.execute(
                "UPDATE rep_users SET user_name=?, last_updated=? WHERE user_id=?",
                (user_name, utc_now_iso(), int(user_id))
            )
        conn.commit(); conn.close()


def adjust_rep(user_id: int, delta: int):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("SELECT rep FROM rep_users WHERE user_id=?", (int(user_id),))
        r = cur.fetchone()
        if r:
            cur.execute(
                "UPDATE rep_users SET rep=?, last_updated=? WHERE user_id=?",
                (int(r["rep"]) + int(delta), utc_now_iso(), int(user_id))
            )
        conn.commit(); conn.close()


def track_rep_message(message_id: int, author_id: int, author_name: str):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("""INSERT OR REPLACE INTO rep_messages (message_id, author_id, author_name, created_at)
                       VALUES (?,?,?,?)""",
                    (int(message_id), int(author_id), author_name, utc_now_iso()))
        conn.commit(); conn.close()


def get_rep_message(message_id: int):
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT * FROM rep_messages WHERE message_id=?", (int(message_id),))
    r = cur.fetchone()
    conn.close()
    return r


def get_vote(message_id: int, voter_id: int):
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT vote FROM rep_votes WHERE message_id=? AND voter_id=?", (int(message_id), int(voter_id)))
    r = cur.fetchone()
    conn.close()
    return int(r["vote"]) if r else None


def set_vote(message_id: int, voter_id: int, vote: int):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO rep_votes (message_id, voter_id, vote) VALUES (?,?,?)",
            (int(message_id), int(voter_id), int(vote))
        )
        conn.commit(); conn.close()


def delete_vote(message_id: int, voter_id: int):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("DELETE FROM rep_votes WHERE message_id=? AND voter_id=?", (int(message_id), int(voter_id)))
        conn.commit(); conn.close()


def delete_rep_message(message_id: int):
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("DELETE FROM rep_votes WHERE message_id=?", (int(message_id),))
        cur.execute("DELETE FROM rep_messages WHERE message_id=?", (int(message_id),))
        conn.commit(); conn.close()


def check_and_consume_broadcast_quota(user_id: int, max_actions: int, window_seconds: int) -> tuple[bool, int]:
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(seconds=int(window_seconds))

    with _DB_WRITE_LOCK:
        conn = connect()
        cur = conn.cursor()

        prune_before = now - timedelta(days=2)
        cur.execute("DELETE FROM broadcast_actions WHERE used_at < ?", (prune_before.isoformat(),))

        cur.execute(
            "SELECT used_at FROM broadcast_actions WHERE user_id=? AND used_at >= ? ORDER BY used_at ASC",
            (int(user_id), window_start.isoformat()),
        )
        rows = cur.fetchall()

        if len(rows) >= int(max_actions):
            oldest_iso = rows[0]["used_at"]
            oldest_dt = parse_iso_utc(oldest_iso)
            if not oldest_dt:
                conn.close()
                return (False, 30)

            unblock_at = oldest_dt + timedelta(seconds=int(window_seconds))
            retry_after = int((unblock_at - now).total_seconds())
            conn.close()
            return (False, max(1, retry_after))

        cur.execute("INSERT INTO broadcast_actions (user_id, used_at) VALUES (?, ?)", (int(user_id), now.isoformat()))
        conn.commit()
        conn.close()
        return (True, 0)


def prune_rep():
    """
    Single write transaction; updates maintenance inside the same connection.
    Avoids 'database is locked' caused by multiple write connections.
    """
    with _DB_WRITE_LOCK:
        conn = connect()
        cur = conn.cursor()

        cur.execute("SELECT message_id, created_at FROM rep_messages")
        rows = cur.fetchall()

        now = datetime.now(timezone.utc)
        expired: list[int] = []
        for r in rows:
            t = parse_iso_utc(r["created_at"])
            if t and (now - t).total_seconds() > REP_TTL_SECONDS:
                expired.append(int(r["message_id"]))

        if expired:
            q = ",".join(["?"] * len(expired))
            cur.execute(f"DELETE FROM rep_votes WHERE message_id IN ({q})", expired)
            cur.execute(f"DELETE FROM rep_messages WHERE message_id IN ({q})", expired)

        cur.execute("UPDATE maintenance SET last_prune_at=? WHERE id=1", (utc_now_iso(),))

        conn.commit()
        conn.close()


def set_user_rep(user_id: int, user_name: str, rep: int, updated_by: str | None = None):
    rep = int(rep)
    with _DB_WRITE_LOCK:
        conn = connect(); cur = conn.cursor()
        cur.execute("SELECT user_id FROM rep_users WHERE user_id=?", (int(user_id),))
        r = cur.fetchone()
        if not r:
            cur.execute(
                "INSERT INTO rep_users (user_id, rep, user_name, last_updated) VALUES (?,?,?,?)",
                (int(user_id), rep, str(user_name), utc_now_iso())
            )
        else:
            cur.execute(
                "UPDATE rep_users SET rep=?, user_name=?, last_updated=? WHERE user_id=?",
                (rep, str(user_name), utc_now_iso(), int(user_id))
            )
        conn.commit(); conn.close()


# ===========================================================
# Groups API
# ===========================================================
def _pbkdf2_hash(password: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERS)


def get_group_by_name(name: str):
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT * FROM groups WHERE name=? COLLATE NOCASE", (name.strip(),))
    r = cur.fetchone()
    conn.close()
    return r


def get_group_id(name: str) -> int | None:
    r = get_group_by_name(name)
    return int(r["group_id"]) if r else None


def create_group(name: str, visibility: str, password: str | None, owner_user_id: int, guild_id: int, guild_name: str) -> int:
    visibility = (visibility or "").strip().lower()
    if visibility not in ("public", "private"):
        raise ValueError("visibility must be 'public' or 'private'")

    salt = None
    pwh = None
    if visibility == "private":
        if not password or len(password.strip()) < 3:
            raise ValueError("Private groups require a password (min 3 chars).")
        salt = secrets.token_bytes(PBKDF2_SALT_LEN)
        pwh = _pbkdf2_hash(password.strip(), salt)

    with _DB_WRITE_LOCK:
        conn = connect()
        cur = conn.cursor()

        cur.execute(
            """INSERT INTO groups (name, visibility, password_salt, password_hash, owner_user_id, created_at)
               VALUES (?,?,?,?,?,?)""",
            (name.strip(), visibility, salt, pwh, int(owner_user_id), utc_now_iso())
        )
        group_id = int(cur.lastrowid)

        cur.execute(
            "INSERT INTO group_user_roles (group_id, user_id, role, created_at) VALUES (?,?, 'owner', ?)",
            (group_id, int(owner_user_id), utc_now_iso())
        )

        cur.execute(
            "INSERT INTO group_servers (group_id, guild_id, guild_name, joined_at) VALUES (?,?,?,?)",
            (group_id, int(guild_id), str(guild_name), utc_now_iso())
        )

        conn.commit()
        conn.close()
        return group_id


def check_group_password(group_id: int, provided_password: str | None) -> bool:
    conn = connect(); cur = conn.cursor()
    cur.execute("SELECT visibility, password_salt, password_hash FROM groups WHERE group_id=?", (int(group_id),))
    r = cur.fetchone()
    conn.close()
    if not r:
        return False
    if r["visibility"] == "public":
        return True
    if not provided_password:
        return False
    salt = r["password_salt"]
    expected = r["password_hash"]
    if salt is None or expected is None:
        return False
    got = _pbkdf2_hash(provided_password.strip(), salt)
    return secrets.compare_digest(got, expected)


def join_group(group_id: int, guild_id: int, guild_name: str) -> None:
    with _DB_WRITE_LOCK:
        conn = connect()
        cur = conn.cursor()
        # insert if missing
        cur.execute(
            """INSERT OR IGNORE INTO group_servers (group_id, guild_id, guild_name, joined_at)
               VALUES (?,?,?,?)""",
            (int(group_id), int(guild_id), str(guild_name), utc_now_iso())
        )
        # always refresh stored name (server rename)
        cur.execute(
            "UPDATE group_servers SET guild_name=? WHERE group_id=? AND guild_id=?",
            (str(guild_name), int(group_id), int(guild_id))
        )
        conn.commit()
        conn.close()


def leave_group(group_id: int, guild_id: int) -> bool:
    with _DB_WRITE_LOCK:
        conn = connect()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM group_servers WHERE group_id=? AND guild_id=?",
            (int(group_id), int(guild_id))
        )
        conn.commit()
        removed = cur.rowcount > 0
        conn.close()
        return removed


def guild_in_group(group_id: int, guild_id: int) -> bool:
    conn = connect(); cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM group_servers WHERE group_id=? AND guild_id=?",
        (int(group_id), int(guild_id))
    )
    r = cur.fetchone()
    conn.close()
    return r is not None


def list_groups_for_guild(guild_id: int):
    conn = connect(); cur = conn.cursor()
    cur.execute(
        """
        SELECT g.group_id, g.name, g.visibility
        FROM group_servers gs
        JOIN groups g ON g.group_id = gs.group_id
        WHERE gs.guild_id=?
        ORDER BY g.name COLLATE NOCASE
        """,
        (int(guild_id),)
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_user_group_role(group_id: int, user_id: int) -> str | None:
    conn = connect(); cur = conn.cursor()
    cur.execute(
        "SELECT role FROM group_user_roles WHERE group_id=? AND user_id=?",
        (int(group_id), int(user_id))
    )
    r = cur.fetchone()
    conn.close()
    return str(r["role"]) if r else None


def set_user_group_role(group_id: int, user_id: int, role: str) -> None:
    role = (role or "").strip().lower()
    if role not in ("owner", "leader", "member"):
        raise ValueError("role must be owner, leader, or member")

    with _DB_WRITE_LOCK:
        conn = connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO group_user_roles (group_id, user_id, role, created_at) VALUES (?,?,?,?)",
            (int(group_id), int(user_id), role, utc_now_iso())
        )
        conn.commit()
        conn.close()


def list_servers_in_group(group_id: int):
    conn = connect(); cur = conn.cursor()
    cur.execute(
        """
        SELECT guild_id, guild_name, joined_at
        FROM group_servers
        WHERE group_id=?
        ORDER BY guild_name COLLATE NOCASE
        """,
        (int(group_id),)
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def remove_server_from_group_by_name(group_id: int, server_name: str) -> bool:
    with _DB_WRITE_LOCK:
        conn = connect()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM group_servers WHERE group_id=? AND guild_name=?",
            (int(group_id), str(server_name).strip())
        )
        conn.commit()
        removed = cur.rowcount > 0
        conn.close()
        return removed


def delete_group(group_id: int) -> None:
    with _DB_WRITE_LOCK:
        conn = connect()
        cur = conn.cursor()
        # ON DELETE CASCADE will clean group_user_roles + group_servers
        cur.execute("DELETE FROM groups WHERE group_id=?", (int(group_id),))
        conn.commit()
        conn.close()
