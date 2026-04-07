import sqlite3
import hashlib
import secrets
from datetime import datetime


DB_NAME = "papers.db"


def get_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def hash_password(password: str, salt: str) -> str:
    value = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        100000
    )
    return value.hex()


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # users
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            plan TEXT DEFAULT 'free',
            trial_ends_at TEXT DEFAULT '',
            plan_started_at TEXT DEFAULT '',
            plan_renews_at TEXT DEFAULT '',
            is_yearly INTEGER DEFAULT 0,
            usage_date TEXT DEFAULT '',
            daily_usage_count INTEGER DEFAULT 0,
            trial_used INTEGER DEFAULT 0,
            ref_code TEXT DEFAULT '',
            ref_by INTEGER,
            trial_extend_days INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # saved papers
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saved_papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            pubmed_id TEXT NOT NULL,
            title TEXT,
            jp_title TEXT DEFAULT '',
            authors TEXT,
            journal TEXT,
            pubdate TEXT,
            abstract TEXT,
            jp TEXT,
            summary_jp TEXT,
            clinical_score TEXT DEFAULT '',
            clinical_reason TEXT DEFAULT '',
            folder_name TEXT DEFAULT '',
            is_favorite INTEGER DEFAULT 0,
            likes INTEGER DEFAULT 0,
            is_public INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    conn.commit()

    # saved_papers migrations

    for sql in [
        "ALTER TABLE saved_papers ADD COLUMN user_id INTEGER",
        "ALTER TABLE saved_papers ADD COLUMN jp_title TEXT DEFAULT ''",
        "ALTER TABLE saved_papers ADD COLUMN folder_name TEXT DEFAULT ''",
        "ALTER TABLE saved_papers ADD COLUMN clinical_score TEXT DEFAULT ''",
        "ALTER TABLE saved_papers ADD COLUMN clinical_reason TEXT DEFAULT ''",
        "ALTER TABLE saved_papers ADD COLUMN is_favorite INTEGER DEFAULT 0",
        "ALTER TABLE saved_papers ADD COLUMN likes INTEGER DEFAULT 0",
        "ALTER TABLE saved_papers ADD COLUMN is_public INTEGER DEFAULT 0",
        "ALTER TABLE saved_papers ADD COLUMN custom_title TEXT DEFAULT ''",
        "ALTER TABLE saved_papers ADD COLUMN user_note TEXT DEFAULT ''",
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    # users migrations

    for sql in [
        "ALTER TABLE users ADD COLUMN plan TEXT DEFAULT 'free'",
        "ALTER TABLE users ADD COLUMN trial_ends_at TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN plan_started_at TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN plan_renews_at TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN is_yearly INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN usage_date TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN daily_usage_count INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN trial_used INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN ref_code TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN ref_by INTEGER",
        "ALTER TABLE users ADD COLUMN trial_extend_days INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN display_name TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN bio TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN avatar TEXT DEFAULT ''",
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    # paper likes (per-user)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_likes (
            pubmed_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (pubmed_id, user_id)
        )
    """)
    conn.commit()

    # board posts
    cur.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            pubmed_id TEXT DEFAULT '',
            paper_title TEXT DEFAULT '',
            paper_jp_title TEXT DEFAULT '',
            tags TEXT DEFAULT '',
            parent_id INTEGER DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (parent_id) REFERENCES posts(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS post_likes (
            post_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (post_id, user_id),
            FOREIGN KEY (post_id) REFERENCES posts(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    conn.commit()

    # user custom tags
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            tag TEXT NOT NULL,
            use_count INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, tag),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    # posts migrations
    for sql in [
        "ALTER TABLE posts ADD COLUMN tags TEXT DEFAULT ''",
        "ALTER TABLE posts ADD COLUMN parent_id INTEGER DEFAULT NULL",
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    # saved_papers highlights migration
    for sql in [
        "ALTER TABLE saved_papers ADD COLUMN highlights TEXT DEFAULT ''",
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    # user interest tags (for recommended papers engine)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_interest_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            tag TEXT NOT NULL,
            score REAL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, tag),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    # index

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_saved_papers_user_pubmed
        ON saved_papers (user_id, pubmed_id)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_ref_code
        ON users (ref_code)
    """)

    conn.commit()
    conn.close()


def save_paper(
    pubmed_id,
    title,
    jp_title,
    authors,
    journal,
    pubdate,
    abstract,
    jp,
    summary_jp,
    folder_name,
    clinical_score,
    clinical_reason,
    user_id=None
):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    if user_id is None:
        cur.execute("""
            SELECT id
            FROM saved_papers
            WHERE user_id IS NULL AND pubmed_id = ?
        """, (pubmed_id,))
    else:
        cur.execute("""
            SELECT id
            FROM saved_papers
            WHERE user_id = ? AND pubmed_id = ?
        """, (user_id, pubmed_id))

    existing = cur.fetchone()

    if existing:
        if user_id is None:
            cur.execute("""
                UPDATE saved_papers
                SET
                    title = ?,
                    jp_title = ?,
                    authors = ?,
                    journal = ?,
                    pubdate = ?,
                    abstract = ?,
                    jp = ?,
                    summary_jp = ?,
                    folder_name = ?,
                    clinical_score = ?,
                    clinical_reason = ?
                WHERE user_id IS NULL AND pubmed_id = ?
            """, (
                title,
                jp_title,
                authors,
                journal,
                pubdate,
                abstract,
                jp,
                summary_jp,
                folder_name,
                clinical_score,
                clinical_reason,
                pubmed_id
            ))
        else:
            cur.execute("""
                UPDATE saved_papers
                SET
                    title = ?,
                    jp_title = ?,
                    authors = ?,
                    journal = ?,
                    pubdate = ?,
                    abstract = ?,
                    jp = ?,
                    summary_jp = ?,
                    folder_name = ?,
                    clinical_score = ?,
                    clinical_reason = ?
                WHERE user_id = ? AND pubmed_id = ?
            """, (
                title,
                jp_title,
                authors,
                journal,
                pubdate,
                abstract,
                jp,
                summary_jp,
                folder_name,
                clinical_score,
                clinical_reason,
                user_id,
                pubmed_id
            ))
    else:
        cur.execute("""
            INSERT INTO saved_papers (
                user_id,
                pubmed_id,
                title,
                jp_title,
                authors,
                journal,
                pubdate,
                abstract,
                jp,
                summary_jp,
                folder_name,
                clinical_score,
                clinical_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            pubmed_id,
            title,
            jp_title,
            authors,
            journal,
            pubdate,
            abstract,
            jp,
            summary_jp,
            folder_name,
            clinical_score,
            clinical_reason
        ))

    conn.commit()
    conn.close()


def get_saved_papers(user_id=None):
    conn = get_connection()
    cur = conn.cursor()

    if user_id is None:
        cur.execute("""
            SELECT *
            FROM saved_papers
            WHERE user_id IS NULL
            ORDER BY folder_name ASC, created_at DESC
        """)
    else:
        cur.execute("""
            SELECT *
            FROM saved_papers
            WHERE user_id = ?
            ORDER BY folder_name ASC, created_at DESC
        """, (user_id,))

    rows = cur.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_public_papers():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM saved_papers
        WHERE is_public = 1
        ORDER BY created_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_saved_paper_by_id(pubmed_id, user_id=None):
    conn = get_connection()
    cur = conn.cursor()

    if user_id is None:
        cur.execute("""
            SELECT *
            FROM saved_papers
            WHERE user_id IS NULL AND pubmed_id = ?
        """, (pubmed_id,))
    else:
        cur.execute("""
            SELECT *
            FROM saved_papers
            WHERE user_id = ? AND pubmed_id = ?
        """, (user_id, pubmed_id))

    row = cur.fetchone()
    conn.close()

    if row:
        return dict(row)

    return None


def get_saved_papers_by_folder(folder_name, user_id=None):
    conn = get_connection()
    cur = conn.cursor()

    if user_id is None:
        cur.execute("""
            SELECT *
            FROM saved_papers
            WHERE user_id IS NULL AND folder_name = ?
            ORDER BY created_at DESC
        """, (folder_name,))
    else:
        cur.execute("""
            SELECT *
            FROM saved_papers
            WHERE user_id = ? AND folder_name = ?
            ORDER BY created_at DESC
        """, (user_id, folder_name))

    rows = cur.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def toggle_favorite(pubmed_id, user_id=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    if user_id is None:
        cur.execute("""
            UPDATE saved_papers
            SET is_favorite = CASE
                WHEN is_favorite = 1 THEN 0
                ELSE 1
            END
            WHERE user_id IS NULL AND pubmed_id = ?
        """, (pubmed_id,))
    else:
        cur.execute("""
            UPDATE saved_papers
            SET is_favorite = CASE
                WHEN is_favorite = 1 THEN 0
                ELSE 1
            END
            WHERE user_id = ? AND pubmed_id = ?
        """, (user_id, pubmed_id))

    conn.commit()
    conn.close()


def toggle_paper_like(pubmed_id: str, user_id: int) -> dict:
    """Toggle like for a paper. Returns {"liked": bool, "likes": int}."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM paper_likes WHERE pubmed_id = ? AND user_id = ?", (pubmed_id, user_id))
    exists = cur.fetchone()
    if exists:
        cur.execute("DELETE FROM paper_likes WHERE pubmed_id = ? AND user_id = ?", (pubmed_id, user_id))
        cur.execute("UPDATE saved_papers SET likes = MAX(0, COALESCE(likes,0)-1) WHERE pubmed_id = ? AND user_id = ?", (pubmed_id, user_id))
        liked = False
    else:
        cur.execute("INSERT OR IGNORE INTO paper_likes (pubmed_id, user_id) VALUES (?,?)", (pubmed_id, user_id))
        cur.execute("UPDATE saved_papers SET likes = COALESCE(likes,0)+1 WHERE pubmed_id = ? AND user_id = ?", (pubmed_id, user_id))
        liked = True
    conn.commit()
    cur.execute("SELECT likes FROM saved_papers WHERE pubmed_id = ? AND user_id = ?", (pubmed_id, user_id))
    row = cur.fetchone()
    likes = int(row[0] or 0) if row else 0
    conn.close()
    return {"liked": liked, "likes": likes}


def get_paper_liked(pubmed_id: str, user_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM paper_likes WHERE pubmed_id = ? AND user_id = ?", (pubmed_id, user_id))
    row = cur.fetchone()
    conn.close()
    return row is not None


def add_like(pubmed_id, user_id=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    if user_id is None:
        cur.execute("""
            UPDATE saved_papers
            SET likes = COALESCE(likes, 0) + 1
            WHERE user_id IS NULL AND pubmed_id = ?
        """, (pubmed_id,))
    else:
        cur.execute("""
            UPDATE saved_papers
            SET likes = COALESCE(likes, 0) + 1
            WHERE user_id = ? AND pubmed_id = ?
        """, (user_id, pubmed_id))

    conn.commit()
    conn.close()


def toggle_public(pubmed_id, user_id=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    if user_id is None:
        cur.execute("""
            UPDATE saved_papers
            SET is_public = CASE
                WHEN is_public = 1 THEN 0
                ELSE 1
            END
            WHERE user_id IS NULL AND pubmed_id = ?
        """, (pubmed_id,))
    else:
        cur.execute("""
            UPDATE saved_papers
            SET is_public = CASE
                WHEN is_public = 1 THEN 0
                ELSE 1
            END
            WHERE user_id = ? AND pubmed_id = ?
        """, (user_id, pubmed_id))

    conn.commit()
    conn.close()


def get_folder_name_suggestions(user_id=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    if user_id is None:
        cur.execute("""
            SELECT folder_name
            FROM saved_papers
            WHERE user_id IS NULL
              AND folder_name IS NOT NULL
              AND TRIM(folder_name) != ''
              AND folder_name != '自動保存'
            GROUP BY folder_name
            ORDER BY MAX(created_at) DESC
        """)
    else:
        cur.execute("""
            SELECT folder_name
            FROM saved_papers
            WHERE user_id = ?
              AND folder_name IS NOT NULL
              AND TRIM(folder_name) != ''
              AND folder_name != '自動保存'
            GROUP BY folder_name
            ORDER BY MAX(created_at) DESC
        """, (user_id,))

    rows = cur.fetchall()
    conn.close()

    return [row[0] for row in rows if row[0]]


def get_user_by_email(email: str):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM users
        WHERE email = ?
    """, (email,))

    row = cur.fetchone()
    conn.close()

    if row:
        return dict(row)

    return None


def get_user_by_id(user_id: int):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM users
        WHERE id = ?
    """, (user_id,))

    row = cur.fetchone()
    conn.close()

    if row:
        return dict(row)

    return None

def generate_ref_code():
    return secrets.token_hex(4).upper()


def create_user(email: str, password: str):
    existing = get_user_by_email(email)
    if existing:
        return None

    salt = secrets.token_hex(16)
    password_hash = hash_password(password, salt)

    ref_code = generate_ref_code()

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO users (
            email,
            password_hash,
            salt,
            plan,
            trial_ends_at,
            plan_started_at,
            plan_renews_at,
            is_yearly,
            usage_date,
            daily_usage_count,
            trial_used,
            ref_code,
            ref_by,
            trial_extend_days
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        email,
        password_hash,
        salt,
        "free",
        "",
        "",
        "",
        0,
        "",
        0,
        0,
        ref_code,
        None,
        0
    ))

    conn.commit()
    user_id = cur.lastrowid
    conn.close()

    return get_user_by_id(user_id)


def verify_user(email: str, password: str):
    user = get_user_by_email(email)
    if not user:
        return None

    password_hash = hash_password(password, user["salt"])

    if password_hash == user["password_hash"]:
        return user

    return None


# --------------------------
# plan / trial / usage helpers
# --------------------------

def update_user_plan(
    user_id,
    plan,
    trial_ends_at=None,
    plan_started_at=None,
    plan_renews_at=None,
    is_yearly=0,
    trial_used=None,
):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    if trial_used is None:
        cur.execute("""
            UPDATE users
            SET
                plan = ?,
                trial_ends_at = ?,
                plan_started_at = ?,
                plan_renews_at = ?,
                is_yearly = ?
            WHERE id = ?
        """, (
            plan,
            trial_ends_at,
            plan_started_at,
            plan_renews_at,
            is_yearly,
            user_id,
        ))
    else:
        cur.execute("""
            UPDATE users
            SET
                plan = ?,
                trial_ends_at = ?,
                plan_started_at = ?,
                plan_renews_at = ?,
                is_yearly = ?,
                trial_used = ?
            WHERE id = ?
        """, (
            plan,
            trial_ends_at,
            plan_started_at,
            plan_renews_at,
            is_yearly,
            trial_used,
            user_id,
        ))

    conn.commit()
    conn.close()


def reset_daily_usage_if_needed(user_id):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT usage_date, daily_usage_count
        FROM users
        WHERE id = ?
    """, (user_id,))
    row = cur.fetchone()

    if not row:
        conn.close()
        return

    today = datetime.now().strftime("%Y-%m-%d")
    usage_date = row["usage_date"] or ""

    if usage_date != today:
        cur.execute("""
            UPDATE users
            SET usage_date = ?, daily_usage_count = 0
            WHERE id = ?
        """, (today, user_id))
        conn.commit()

    conn.close()


def get_user_daily_usage(user_id):
    reset_daily_usage_if_needed(user_id)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT daily_usage_count
        FROM users
        WHERE id = ?
    """, (user_id,))

    row = cur.fetchone()
    conn.close()

    if not row:
        return 0

    return int(row["daily_usage_count"] or 0)


def increment_daily_usage(user_id, amount=1):
    reset_daily_usage_if_needed(user_id)

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        UPDATE users
        SET daily_usage_count = COALESCE(daily_usage_count, 0) + ?
        WHERE id = ?
    """, (amount, user_id))

    conn.commit()
    conn.close()


def count_user_saved_papers(user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM saved_papers
        WHERE user_id = ?
          AND folder_name IS NOT NULL
          AND TRIM(folder_name) != ''
    """, (user_id,))

    count = cur.fetchone()[0]
    conn.close()

    return int(count or 0)

def rename_folder(user_id: int, old_name: str, new_name: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE saved_papers
        SET folder_name = ?
        WHERE user_id = ? AND folder_name = ?
    """, (new_name, user_id, old_name))
    conn.commit()
    conn.close()


def update_user_profile(user_id: int, display_name: str, bio: str, avatar: str = ""):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET display_name = ?, bio = ?, avatar = ?
        WHERE id = ?
    """, (display_name, bio, avatar, user_id))
    conn.commit()
    conn.close()


def get_user_by_ref_code(ref_code: str):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM users
        WHERE ref_code = ?
    """, (ref_code,))

    row = cur.fetchone()
    conn.close()

    if row:
        return dict(row)

    return None


def apply_referral_bonus(referrer_id, referred_user_id):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM users WHERE id = ?", (referrer_id,))
    referrer = cur.fetchone()

    cur.execute("SELECT * FROM users WHERE id = ?", (referred_user_id,))
    referred = cur.fetchone()

    if not referrer or not referred:
        conn.close()
        return False, "user_not_found"

    if referred["ref_by"]:
        conn.close()
        return False, "already_used"

    if referrer["id"] == referred["id"]:
        conn.close()
        return False, "self_referral"

    current_extend = int(referrer["trial_extend_days"] or 0)
    new_extend = min(current_extend + 7, 60)

    cur.execute("""
        UPDATE users
        SET ref_by = ?
        WHERE id = ?
    """, (referrer["id"], referred_user_id))

    cur.execute("""
        UPDATE users
        SET trial_extend_days = ?
        WHERE id = ?
    """, (new_extend, referrer_id))

    conn.commit()
    conn.close()

    return True, "ok"

def init_memos_tables():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS memos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT DEFAULT '',
            body TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_memos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            pubmed_id TEXT NOT NULL,
            paper_title TEXT DEFAULT '',
            body TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # memos migrations
    for sql in [
        "ALTER TABLE memos ADD COLUMN tags TEXT DEFAULT ''",
        "ALTER TABLE paper_memos ADD COLUMN tags TEXT DEFAULT ''",
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    conn.commit()
    conn.close()


def count_user_all_memos(user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM memos WHERE user_id = ?", (user_id,))
    quick_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM paper_memos WHERE user_id = ?", (user_id,))
    paper_count = cur.fetchone()[0]
    conn.close()
    return int(quick_count or 0) + int(paper_count or 0)


def get_user_memos(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM memos WHERE user_id = ?
        ORDER BY updated_at DESC
    """, (user_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_memo_by_id(memo_id, user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM memos WHERE id = ? AND user_id = ?", (memo_id, user_id))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def create_memo(user_id, title, body):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO memos (user_id, title, body, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, title, body, now, now))
    conn.commit()
    memo_id = cur.lastrowid
    conn.close()
    return memo_id


def update_memo(memo_id, user_id, title, body):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        UPDATE memos SET title = ?, body = ?, updated_at = ?
        WHERE id = ? AND user_id = ?
    """, (title, body, now, memo_id, user_id))
    conn.commit()
    conn.close()


def delete_memo(memo_id, user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM memos WHERE id = ? AND user_id = ?", (memo_id, user_id))
    conn.commit()
    conn.close()


def get_user_paper_memos(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM paper_memos WHERE user_id = ?
        ORDER BY updated_at DESC
    """, (user_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_paper_memo_by_id(memo_id, user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM paper_memos WHERE id = ? AND user_id = ?", (memo_id, user_id))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def create_paper_memo(user_id, pubmed_id, paper_title, body):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO paper_memos (user_id, pubmed_id, paper_title, body, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (user_id, pubmed_id, paper_title, body, now, now))
    conn.commit()
    memo_id = cur.lastrowid
    conn.close()
    return memo_id


def update_paper_memo(memo_id, user_id, body):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        UPDATE paper_memos SET body = ?, updated_at = ?
        WHERE id = ? AND user_id = ?
    """, (body, now, memo_id, user_id))
    conn.commit()
    conn.close()


def delete_paper_memo(memo_id, user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM paper_memos WHERE id = ? AND user_id = ?", (memo_id, user_id))
    conn.commit()
    conn.close()


def set_trial_extend_days(user_id, days):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        UPDATE users
        SET trial_extend_days = ?
        WHERE id = ?
    """, (days, user_id))

    conn.commit()
    conn.close()

def update_saved_paper_folder(pubmed_id, folder_name, user_id=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    clean_folder_name = (folder_name or "").strip()

    if user_id is None:
        cur.execute("""
            UPDATE saved_papers
            SET folder_name = ?
            WHERE user_id IS NULL AND pubmed_id = ?
        """, (clean_folder_name, pubmed_id))
    else:
        cur.execute("""
            UPDATE saved_papers
            SET folder_name = ?
            WHERE user_id = ? AND pubmed_id = ?
        """, (clean_folder_name, user_id, pubmed_id))

    conn.commit()
    conn.close()


def update_saved_paper_custom_title(pubmed_id, custom_title, user_id=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    clean_custom_title = (custom_title or "").strip()

    if user_id is None:
        cur.execute("""
            UPDATE saved_papers
            SET custom_title = ?
            WHERE user_id IS NULL AND pubmed_id = ?
        """, (clean_custom_title, pubmed_id))
    else:
        cur.execute("""
            UPDATE saved_papers
            SET custom_title = ?
            WHERE user_id = ? AND pubmed_id = ?
        """, (clean_custom_title, user_id, pubmed_id))

    conn.commit()
    conn.close()


def update_saved_paper_user_note(pubmed_id, user_note, user_id=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    clean_user_note = (user_note or "").strip()

    if user_id is None:
        cur.execute("""
            UPDATE saved_papers
            SET user_note = ?
            WHERE user_id IS NULL AND pubmed_id = ?
        """, (clean_user_note, pubmed_id))
    else:
        cur.execute("""
            UPDATE saved_papers
            SET user_note = ?
            WHERE user_id = ? AND pubmed_id = ?
        """, (clean_user_note, user_id, pubmed_id))

    conn.commit()
    conn.close()


# ── Board / SNS ──────────────────────────────────────────────────────────────

def create_post(user_id: int, content: str, pubmed_id: str = "", paper_title: str = "",
                paper_jp_title: str = "", tags: str = "", parent_id: int = None) -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO posts (user_id, content, pubmed_id, paper_title, paper_jp_title, tags, parent_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, content.strip(), pubmed_id, paper_title, paper_jp_title, tags, parent_id))
    post_id = cur.lastrowid
    conn.commit()
    conn.close()
    return post_id


def get_posts(limit: int = 30, offset: int = 0, viewer_user_id: int = None, tag_filter: str = ""):
    conn = get_connection()
    cur = conn.cursor()
    where = "WHERE p.parent_id IS NULL"
    params: list = []
    if tag_filter:
        where += " AND (',' || p.tags || ',' LIKE ?)"
        params.append(f"%,{tag_filter},%")
    cur.execute(f"""
        SELECT p.id, p.content, p.pubmed_id, p.paper_title, p.paper_jp_title,
               p.created_at, p.user_id, p.tags,
               u.display_name, u.avatar,
               COUNT(DISTINCT pl.user_id) AS like_count,
               COUNT(DISTINCT r.id) AS reply_count
        FROM posts p
        JOIN users u ON u.id = p.user_id
        LEFT JOIN post_likes pl ON pl.post_id = p.id
        LEFT JOIN posts r ON r.parent_id = p.id
        {where}
        GROUP BY p.id
        ORDER BY p.created_at DESC
        LIMIT ? OFFSET ?
    """, (*params, limit, offset))
    rows = cur.fetchall()

    liked_ids = set()
    if viewer_user_id:
        cur.execute("SELECT post_id FROM post_likes WHERE user_id = ?", (viewer_user_id,))
        liked_ids = {r[0] for r in cur.fetchall()}

    conn.close()

    result = []
    for row in rows:
        d = dict(row)
        d["liked"] = d["id"] in liked_ids
        d["tags_list"] = [t.strip() for t in d["tags"].split(",") if t.strip()] if d.get("tags") else []
        result.append(d)
    return result


def get_replies(post_id: int, viewer_user_id: int = None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.id, p.content, p.pubmed_id, p.paper_title, p.paper_jp_title,
               p.created_at, p.user_id, p.tags, p.parent_id,
               u.display_name, u.avatar,
               COUNT(pl.user_id) AS like_count
        FROM posts p
        JOIN users u ON u.id = p.user_id
        LEFT JOIN post_likes pl ON pl.post_id = p.id
        WHERE p.parent_id = ?
        GROUP BY p.id
        ORDER BY p.created_at ASC
    """, (post_id,))
    rows = cur.fetchall()

    liked_ids = set()
    if viewer_user_id:
        cur.execute("SELECT post_id FROM post_likes WHERE user_id = ?", (viewer_user_id,))
        liked_ids = {r[0] for r in cur.fetchall()}

    conn.close()
    result = []
    for row in rows:
        d = dict(row)
        d["liked"] = d["id"] in liked_ids
        result.append(d)
    return result


def toggle_post_like(post_id: int, user_id: int) -> bool:
    """Returns True if liked, False if unliked."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM post_likes WHERE post_id = ? AND user_id = ?", (post_id, user_id))
    exists = cur.fetchone()
    if exists:
        cur.execute("DELETE FROM post_likes WHERE post_id = ? AND user_id = ?", (post_id, user_id))
        liked = False
    else:
        cur.execute("INSERT INTO post_likes (post_id, user_id) VALUES (?, ?)", (post_id, user_id))
        liked = True
    conn.commit()
    conn.close()
    return liked


def delete_post(post_id: int, user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    # delete likes for replies too
    cur.execute("DELETE FROM post_likes WHERE post_id IN (SELECT id FROM posts WHERE parent_id = ?)", (post_id,))
    cur.execute("DELETE FROM posts WHERE parent_id = ?", (post_id,))
    cur.execute("DELETE FROM post_likes WHERE post_id = ?", (post_id,))
    cur.execute("DELETE FROM posts WHERE id = ? AND user_id = ?", (post_id, user_id))
    conn.commit()
    conn.close()


def update_saved_paper_highlights(pubmed_id: str, highlights: str, user_id=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    if user_id is None:
        cur.execute("UPDATE saved_papers SET highlights = ? WHERE user_id IS NULL AND pubmed_id = ?",
                    (highlights, pubmed_id))
    else:
        cur.execute("UPDATE saved_papers SET highlights = ? WHERE user_id = ? AND pubmed_id = ?",
                    (highlights, user_id, pubmed_id))
    conn.commit()
    conn.close()


def update_memo_tags(memo_id: int, user_id: int, tags: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE memos SET tags = ? WHERE id = ? AND user_id = ?", (tags, memo_id, user_id))
    conn.commit()
    conn.close()


def update_paper_memo_tags(memo_id: int, user_id: int, tags: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE paper_memos SET tags = ? WHERE id = ? AND user_id = ?", (tags, memo_id, user_id))
    conn.commit()
    conn.close()


def get_user_tags(user_id: int) -> list:
    """Return user's tags sorted by use_count desc, then created_at."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT tag, use_count FROM user_tags
        WHERE user_id = ?
        ORDER BY use_count DESC, created_at DESC
        LIMIT 30
    """, (user_id,))
    rows = cur.fetchall()
    conn.close()
    return [{"tag": r[0], "use_count": r[1]} for r in rows]


def upsert_user_tag(user_id: int, tag: str):
    """Insert tag or increment use_count if already exists."""
    tag = tag.strip()
    if not tag:
        return
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO user_tags (user_id, tag, use_count)
        VALUES (?, ?, 1)
        ON CONFLICT(user_id, tag) DO UPDATE SET use_count = use_count + 1
    """, (user_id, tag))
    conn.commit()
    conn.close()


def delete_user_tag(user_id: int, tag: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM user_tags WHERE user_id = ? AND tag = ?", (user_id, tag))
    conn.commit()
    conn.close()


def record_interest(user_id: int, tags: list, weight: float):
    """ユーザーの興味スコアを加算する。weight: 閲覧=1, 要約=2, 保存=3, お気に入り=4"""
    if not tags or not user_id:
        return
    conn = get_connection()
    cur = conn.cursor()
    for tag in tags:
        tag = tag.strip()
        if not tag:
            continue
        cur.execute("""
            INSERT INTO user_interest_tags (user_id, tag, score, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id, tag) DO UPDATE SET
                score = score + ?,
                updated_at = CURRENT_TIMESTAMP
        """, (user_id, tag, weight, weight))
    conn.commit()
    conn.close()


def get_interest_tags(user_id: int, limit: int = 10) -> list:
    """スコア上位の興味タグを返す"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT tag, score FROM user_interest_tags
        WHERE user_id = ?
        ORDER BY score DESC
        LIMIT ?
    """, (user_id, limit))
    rows = cur.fetchall()
    conn.close()
    return [{"tag": r[0], "score": r[1]} for r in rows]


def get_recommended_papers(user_id: int, limit: int = 10) -> list:
    """興味タグに近い保存論文をおすすめとして返す（タグベース）"""
    interest = get_interest_tags(user_id, limit=5)
    if not interest:
        # 興味タグなし → 全体人気順を返す
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT pubmed_id, title, jp_title, authors, journal, pubdate,
                   summary_jp, clinical_score, likes, folder_name
            FROM saved_papers
            WHERE summary_jp IS NOT NULL AND summary_jp != ''
            ORDER BY likes DESC, created_at DESC
            LIMIT ?
        """, (limit,))
        rows = cur.fetchall()
        conn.close()
        return [{"paper": dict(r), "reason": "人気の論文です", "matched_tags": []} for r in rows]

    conn = get_connection()
    cur = conn.cursor()
    results = []
    seen = set()

    for item in interest:
        tag = item["tag"]
        # jp_title または summary_jp にタグが含まれる論文を検索
        cur.execute("""
            SELECT pubmed_id, title, jp_title, authors, journal, pubdate,
                   summary_jp, clinical_score, likes, folder_name
            FROM saved_papers
            WHERE (jp_title LIKE ? OR summary_jp LIKE ? OR title LIKE ?)
              AND summary_jp IS NOT NULL AND summary_jp != ''
            ORDER BY likes DESC, created_at DESC
            LIMIT 5
        """, (f"%{tag}%", f"%{tag}%", f"%{tag}%"))
        for row in cur.fetchall():
            pid = row["pubmed_id"]
            if pid not in seen:
                seen.add(pid)
                results.append({
                    "paper": dict(row),
                    "reason": f"よく見る「{tag}」に近い論文です",
                    "matched_tags": [tag]
                })
        if len(results) >= limit:
            break

    # 足りなければ人気順で補完
    if len(results) < limit:
        cur.execute("""
            SELECT pubmed_id, title, jp_title, authors, journal, pubdate,
                   summary_jp, clinical_score, likes, folder_name
            FROM saved_papers
            WHERE summary_jp IS NOT NULL AND summary_jp != ''
            ORDER BY likes DESC, created_at DESC
            LIMIT ?
        """, (limit * 2,))
        for row in cur.fetchall():
            pid = row["pubmed_id"]
            if pid not in seen and len(results) < limit:
                seen.add(pid)
                results.append({
                    "paper": dict(row),
                    "reason": "人気の論文です",
                    "matched_tags": []
                })

    conn.close()
    return results[:limit]


def get_paper_jp_title_global(pubmed_id: str) -> str:
    """Return the first non-empty jp_title for this pubmed_id, across all users."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT jp_title FROM saved_papers WHERE pubmed_id = ? AND jp_title != '' LIMIT 1",
        (pubmed_id,)
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else ""