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
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

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