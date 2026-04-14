import os
import sqlite3
import hashlib
import secrets
import base64
import json
from datetime import datetime
from typing import Any

try:
    from cryptography.fernet import Fernet, InvalidToken
except Exception:  # pragma: no cover - optional dependency at deploy time
    Fernet = None

    class InvalidToken(Exception):
        pass


DB_NAME = os.getenv("DB_NAME", "papers.db")


def _get_wordpress_secret_material() -> str:
    return (
        (os.getenv("WORDPRESS_SECRET_KEY", "") or "").strip()
        or (os.getenv("SESSION_SECRET", "") or "").strip()
    )


def is_wordpress_encryption_available() -> bool:
    return bool(Fernet and _get_wordpress_secret_material())


def _get_wordpress_fernet():
    secret_material = _get_wordpress_secret_material()
    if not Fernet or not secret_material:
        return None
    derived_key = base64.urlsafe_b64encode(
        hashlib.sha256(secret_material.encode("utf-8")).digest()
    )
    return Fernet(derived_key)


def _is_encrypted_wordpress_secret(value: str) -> bool:
    return (value or "").startswith("enc::")


def encrypt_wordpress_secret(value: str) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return ""
    if _is_encrypted_wordpress_secret(normalized):
        return normalized
    fernet = _get_wordpress_fernet()
    if not fernet:
        return normalized
    return "enc::" + fernet.encrypt(normalized.encode("utf-8")).decode("utf-8")


def decrypt_wordpress_secret(value: str) -> tuple[str, bool]:
    raw_value = (value or "").strip()
    if not raw_value:
        return "", False
    if not _is_encrypted_wordpress_secret(raw_value):
        return raw_value, False

    fernet = _get_wordpress_fernet()
    if not fernet:
        return "", True
    try:
        decrypted = fernet.decrypt(raw_value.removeprefix("enc::").encode("utf-8")).decode("utf-8")
        return decrypted, True
    except InvalidToken:
        return "", True


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
            save_source TEXT DEFAULT 'auto',
            is_favorite INTEGER DEFAULT 0,
            likes INTEGER DEFAULT 0,
            is_public INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            pubmed_id TEXT NOT NULL,
            title TEXT DEFAULT '',
            jp_title TEXT DEFAULT '',
            authors TEXT DEFAULT '',
            journal TEXT DEFAULT '',
            pubdate TEXT DEFAULT '',
            abstract TEXT DEFAULT '',
            summary_jp TEXT DEFAULT '',
            clinical_score TEXT DEFAULT '',
            clinical_reason TEXT DEFAULT '',
            viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, pubmed_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            pubmed_id TEXT NOT NULL,
            paper_title TEXT DEFAULT '',
            paper_jp_title TEXT DEFAULT '',
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            category TEXT DEFAULT 'general',
            message TEXT NOT NULL,
            page_context TEXT DEFAULT 'mypage',
            status TEXT DEFAULT 'new',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_fulltext_cache (
            pubmed_id TEXT PRIMARY KEY,
            pmcid TEXT DEFAULT '',
            license_name TEXT DEFAULT '',
            license_url TEXT DEFAULT '',
            source_url TEXT DEFAULT '',
            is_translatable INTEGER DEFAULT 0,
            sections_json TEXT DEFAULT '',
            sections_jp_json TEXT DEFAULT '',
            checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            translated_at TEXT DEFAULT ''
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
        "ALTER TABLE saved_papers ADD COLUMN save_source TEXT DEFAULT 'auto'",
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
        # promo code columns
        "ALTER TABLE users ADD COLUMN promo_plan TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN promo_ends_at TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN promo_code_used TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN promo_code_used_at TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN promo_is_lifetime INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN acquisition_channel TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN acquisition_article_draft_id INTEGER",
        "ALTER TABLE users ADD COLUMN acquisition_article_variant TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN acquisition_at TEXT DEFAULT ''",
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_saved_papers_user_pubmed ON saved_papers(user_id, pubmed_id)",
        "CREATE INDEX IF NOT EXISTS idx_saved_papers_user_folder ON saved_papers(user_id, folder_name)",
        "CREATE INDEX IF NOT EXISTS idx_saved_papers_pubmed ON saved_papers(pubmed_id)",
        "CREATE INDEX IF NOT EXISTS idx_saved_papers_user_created ON saved_papers(user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_saved_papers_user_favorite ON saved_papers(user_id, is_favorite)",
        "CREATE INDEX IF NOT EXISTS idx_saved_papers_user_source ON saved_papers(user_id, save_source, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_paper_history_user_viewed ON paper_history(user_id, viewed_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_paper_history_user_pubmed ON paper_history(user_id, pubmed_id)",
        "CREATE INDEX IF NOT EXISTS idx_paper_comments_pubmed_created ON paper_comments(pubmed_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_paper_comments_user_created ON paper_comments(user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_user_feedback_user_created ON user_feedback(user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_paper_fulltext_pmcid ON paper_fulltext_cache(pmcid)",
    ]:
        cur.execute(sql)
    conn.commit()

    # saved_papers source normalization
    cur.execute(
        """
        UPDATE saved_papers
        SET save_source = 'cache'
        WHERE user_id IS NULL
          AND (save_source IS NULL OR TRIM(save_source) = '' OR save_source = 'auto')
        """
    )
    cur.execute(
        """
        UPDATE saved_papers
        SET save_source = 'manual_save'
        WHERE user_id IS NOT NULL
          AND (save_source IS NULL OR TRIM(save_source) = '' OR save_source = 'auto')
          AND TRIM(COALESCE(folder_name, '')) != ''
          AND folder_name NOT IN ('未分類', 'あとで見る')
        """
    )
    cur.execute(
        """
        UPDATE saved_papers
        SET save_source = 'manual_summary'
        WHERE user_id IS NOT NULL
          AND (save_source IS NULL OR TRIM(save_source) = '' OR save_source = 'auto')
          AND (
            TRIM(COALESCE(folder_name, '')) = ''
            OR folder_name IN ('未分類', 'あとで見る')
          )
          AND (
            COALESCE(summary_jp, '') != ''
            OR COALESCE(jp, '') != ''
            OR COALESCE(clinical_reason, '') != ''
            OR COALESCE(clinical_score, '') != ''
          )
          AND COALESCE(likes, 0) = 0
        """
    )
    cur.execute(
        """
        UPDATE saved_papers
        SET save_source = 'auto'
        WHERE user_id IS NOT NULL
          AND (save_source IS NULL OR TRIM(save_source) = '')
        """
    )
    conn.commit()

    # friend promo codes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS friend_promo_codes (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            code          TEXT UNIQUE NOT NULL,
            plan_to_grant TEXT DEFAULT 'pro',
            free_days     INTEGER DEFAULT 90,
            grant_lifetime INTEGER DEFAULT 0,
            max_uses      INTEGER DEFAULT 1,
            used_count    INTEGER DEFAULT 0,
            expires_at    TEXT DEFAULT '',
            target_email  TEXT DEFAULT '',
            is_active     INTEGER DEFAULT 1,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    for sql in [
        "ALTER TABLE friend_promo_codes ADD COLUMN grant_lifetime INTEGER DEFAULT 0",
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS supporter_campaign_claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            campaign_slug TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'active',
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_article_drafts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pubmed_id TEXT NOT NULL,
            source_title TEXT DEFAULT '',
            source_jp_title TEXT DEFAULT '',
            source_summary_jp TEXT DEFAULT '',
            source_abstract TEXT DEFAULT '',
            source_clinical_score TEXT DEFAULT '',
            source_clinical_reason TEXT DEFAULT '',
            article_title TEXT NOT NULL,
            article_excerpt TEXT DEFAULT '',
            article_slug TEXT DEFAULT '',
            article_html TEXT DEFAULT '',
            geo_score INTEGER DEFAULT 0,
            geo_feedback TEXT DEFAULT '',
            geo_last_reviewed_at TEXT DEFAULT '',
            wordpress_post_id TEXT DEFAULT '',
            wordpress_status TEXT DEFAULT '',
            created_by_user_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (created_by_user_id) REFERENCES users(id)
        )
    """)
    conn.commit()
    for sql in [
        "ALTER TABLE master_article_drafts ADD COLUMN geo_score INTEGER DEFAULT 0",
        "ALTER TABLE master_article_drafts ADD COLUMN geo_feedback TEXT DEFAULT ''",
        "ALTER TABLE master_article_drafts ADD COLUMN geo_last_reviewed_at TEXT DEFAULT ''",
        "ALTER TABLE master_article_drafts ADD COLUMN marketing_variant TEXT DEFAULT ''",
        "ALTER TABLE master_article_drafts ADD COLUMN wordpress_published_at TEXT DEFAULT ''",
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_wordpress_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            site_url TEXT DEFAULT '',
            username TEXT DEFAULT '',
            app_password TEXT DEFAULT '',
            app_base_url TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()
    for sql in [
        "ALTER TABLE master_wordpress_settings ADD COLUMN app_base_url TEXT DEFAULT ''",
    ]:
        try:
            cur.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_wordpress_autopost_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            is_enabled INTEGER DEFAULT 0,
            daily_time TEXT DEFAULT '09:00',
            last_attempted_date TEXT DEFAULT '',
            last_success_date TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_wordpress_autopost_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            draft_id INTEGER,
            status TEXT DEFAULT '',
            message TEXT DEFAULT '',
            wordpress_post_id TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (draft_id) REFERENCES master_article_drafts(id)
        )
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_article_marketing_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            variant TEXT DEFAULT '',
            source TEXT DEFAULT '',
            user_id INTEGER,
            ip_hash TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (draft_id) REFERENCES master_article_drafts(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS memo_map_layouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            layout_json TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS memo_mind_maps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            map_json TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS memo_mind_map_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT DEFAULT '',
            map_json TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

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

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_supporter_campaign_claims_user
        ON supporter_campaign_claims (user_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_article_drafts_created
        ON master_article_drafts (created_at DESC)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_master_wordpress_settings_user
        ON master_wordpress_settings (user_id)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_memo_map_layouts_user
        ON memo_map_layouts (user_id)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_memo_mind_maps_user
        ON memo_mind_maps (user_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_memo_mind_map_files_user_updated
        ON memo_mind_map_files (user_id, updated_at DESC)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_master_wordpress_autopost_settings_user
        ON master_wordpress_autopost_settings (user_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_wordpress_autopost_logs_user_created
        ON master_wordpress_autopost_logs (user_id, created_at DESC)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_article_marketing_events_draft_type_created
        ON master_article_marketing_events (draft_id, event_type, created_at DESC)
    """)

    conn.commit()
    conn.close()


MANUAL_SAVED_SOURCES = ("manual_save", "manual_summary")
MANUAL_FOLDER_SOURCES = ("manual_save",)


def _normalize_saved_sources(sources):
    normalized = []
    seen = set()
    for source in sources or []:
        clean = str(source or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        normalized.append(clean)
    return normalized


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
    user_id=None,
    save_source=None,
):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    if user_id is None:
        cur.execute("""
            SELECT id, save_source
            FROM saved_papers
            WHERE user_id IS NULL AND pubmed_id = ?
        """, (pubmed_id,))
    else:
        cur.execute("""
            SELECT id, save_source
            FROM saved_papers
            WHERE user_id = ? AND pubmed_id = ?
        """, (user_id, pubmed_id))

    existing = cur.fetchone()
    existing_source = ""
    if existing:
        existing_source = str(existing[1] or "").strip()

    resolved_source = str(save_source or "").strip()
    if not resolved_source:
        resolved_source = existing_source or ("cache" if user_id is None else "auto")

    if existing_source == "manual_save" and resolved_source in {"manual_summary", "auto"}:
        resolved_source = "manual_save"
    elif existing_source == "manual_summary" and resolved_source == "auto":
        resolved_source = "manual_summary"

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
                    save_source = ?,
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
                resolved_source,
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
                    save_source = ?,
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
                resolved_source,
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
                save_source,
                clinical_score,
                clinical_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            resolved_source,
            clinical_score,
            clinical_reason
        ))

    conn.commit()
    conn.close()


def get_saved_papers(user_id=None, sources=None):
    conn = get_connection()
    cur = conn.cursor()
    normalized_sources = _normalize_saved_sources(sources)

    if user_id is None:
        sql = """
            SELECT *
            FROM saved_papers
            WHERE user_id IS NULL
        """
        params = []
    else:
        sql = """
            SELECT *
            FROM saved_papers
            WHERE user_id = ?
        """
        params = [user_id]

    if normalized_sources:
        placeholders = ",".join(["?"] * len(normalized_sources))
        sql += f" AND save_source IN ({placeholders})"
        params.extend(normalized_sources)

    sql += " ORDER BY folder_name ASC, created_at DESC"
    cur.execute(sql, tuple(params))

    rows = cur.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_saved_papers_by_pubmed_ids(pubmed_ids, user_id=None, sources=None):
    ids = [str(pid).strip() for pid in (pubmed_ids or []) if str(pid).strip()]
    if not ids:
        return []

    conn = get_connection()
    cur = conn.cursor()
    placeholders = ",".join(["?"] * len(ids))
    normalized_sources = _normalize_saved_sources(sources)

    if user_id is None:
        sql = f"""
            SELECT *
            FROM saved_papers
            WHERE user_id IS NULL
              AND pubmed_id IN ({placeholders})
        """
        params = ids
    else:
        sql = f"""
            SELECT *
            FROM saved_papers
            WHERE user_id = ?
              AND pubmed_id IN ({placeholders})
        """
        params = [user_id, *ids]

    if normalized_sources:
        source_placeholders = ",".join(["?"] * len(normalized_sources))
        sql += f" AND save_source IN ({source_placeholders})"
        params.extend(normalized_sources)

    cur.execute(sql, params)

    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_all_saved_papers():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM saved_papers
        ORDER BY created_at DESC
    """)
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


def get_best_cached_paper(pubmed_id: str) -> dict | None:
    """Return the strongest cached paper row for this PMID across all users."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *,
               (
                   CASE WHEN COALESCE(summary_jp, '') != '' THEN 8 ELSE 0 END +
                   CASE WHEN COALESCE(clinical_reason, '') != '' THEN 4 ELSE 0 END +
                   CASE WHEN COALESCE(clinical_score, '') != '' THEN 2 ELSE 0 END +
                   CASE WHEN COALESCE(jp, '') != '' THEN 2 ELSE 0 END +
                   CASE WHEN COALESCE(jp_title, '') != '' THEN 1 ELSE 0 END
               ) AS cache_strength
        FROM saved_papers
        WHERE pubmed_id = ?
          AND (
              COALESCE(jp_title, '') != ''
              OR COALESCE(jp, '') != ''
              OR COALESCE(summary_jp, '') != ''
              OR COALESCE(clinical_reason, '') != ''
              OR COALESCE(clinical_score, '') != ''
          )
        ORDER BY
            cache_strength DESC,
            CASE WHEN user_id IS NULL THEN 1 ELSE 0 END DESC,
            created_at DESC
        LIMIT 1
        """,
        (pubmed_id,),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_paper_fulltext_cache(pubmed_id: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM paper_fulltext_cache
        WHERE pubmed_id = ?
        LIMIT 1
        """,
        (pubmed_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None

    item = dict(row)
    for key in ("sections_json", "sections_jp_json"):
        raw_value = item.get(key) or ""
        if not raw_value:
            item[key] = []
            continue
        try:
            item[key] = json.loads(raw_value)
        except Exception:
            item[key] = []
    item["is_translatable"] = bool(int(item.get("is_translatable") or 0))
    return item


def get_fulltext_available_pubmed_ids(pubmed_ids: list[str] | tuple[str, ...]) -> set[str]:
    normalized_ids = [
        str(pubmed_id).strip()
        for pubmed_id in (pubmed_ids or [])
        if str(pubmed_id).strip()
    ]
    if not normalized_ids:
        return set()

    conn = get_connection()
    cur = conn.cursor()
    placeholders = ",".join(["?"] * len(normalized_ids))
    cur.execute(
        f"""
        SELECT pubmed_id
        FROM paper_fulltext_cache
        WHERE is_translatable = 1
          AND pubmed_id IN ({placeholders})
        """,
        normalized_ids,
    )
    rows = cur.fetchall()
    conn.close()
    return {str(row["pubmed_id"]).strip() for row in rows if str(row["pubmed_id"]).strip()}


def upsert_paper_fulltext_cache(
    pubmed_id: str,
    pmcid: str = "",
    license_name: str = "",
    license_url: str = "",
    source_url: str = "",
    is_translatable: bool = False,
    sections: list[dict[str, Any]] | None = None,
    sections_jp: list[dict[str, Any]] | None = None,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO paper_fulltext_cache (
            pubmed_id,
            pmcid,
            license_name,
            license_url,
            source_url,
            is_translatable,
            sections_json,
            sections_jp_json,
            checked_at,
            translated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(pubmed_id) DO UPDATE SET
            pmcid = excluded.pmcid,
            license_name = excluded.license_name,
            license_url = excluded.license_url,
            source_url = excluded.source_url,
            is_translatable = excluded.is_translatable,
            sections_json = CASE
                WHEN excluded.sections_json != '' THEN excluded.sections_json
                ELSE paper_fulltext_cache.sections_json
            END,
            sections_jp_json = CASE
                WHEN excluded.sections_jp_json != '' THEN excluded.sections_jp_json
                ELSE paper_fulltext_cache.sections_jp_json
            END,
            checked_at = CURRENT_TIMESTAMP,
            translated_at = CASE
                WHEN excluded.sections_jp_json != '' THEN excluded.translated_at
                ELSE paper_fulltext_cache.translated_at
            END
        """,
        (
            pubmed_id,
            (pmcid or "").strip(),
            (license_name or "").strip(),
            (license_url or "").strip(),
            (source_url or "").strip(),
            1 if is_translatable else 0,
            json.dumps(sections or [], ensure_ascii=False) if sections else "",
            json.dumps(sections_jp or [], ensure_ascii=False) if sections_jp else "",
            datetime.utcnow().isoformat() if sections_jp else "",
        ),
    )
    conn.commit()
    conn.close()


def get_saved_papers_by_folder(folder_name, user_id=None, sources=None):
    conn = get_connection()
    cur = conn.cursor()
    normalized_folder_name = (folder_name or "").strip()
    is_default_folder = normalized_folder_name in {"", "未分類", "あとで見る"}
    normalized_sources = _normalize_saved_sources(sources)
    source_clause = ""
    source_params = []
    if normalized_sources:
        placeholders = ",".join(["?"] * len(normalized_sources))
        source_clause = f" AND save_source IN ({placeholders})"
        source_params.extend(normalized_sources)

    if user_id is None:
        if is_default_folder:
            cur.execute(f"""
                SELECT *
                FROM saved_papers
                WHERE user_id IS NULL
                  AND (
                    TRIM(COALESCE(folder_name, '')) = ''
                    OR folder_name = '未分類'
                    OR folder_name = 'あとで見る'
                  )
                  {source_clause}
                ORDER BY created_at DESC
            """, tuple(source_params))
        else:
            cur.execute(f"""
                SELECT *
                FROM saved_papers
                WHERE user_id IS NULL AND folder_name = ?
                {source_clause}
                ORDER BY created_at DESC
            """, (normalized_folder_name, *source_params))
    else:
        if is_default_folder:
            cur.execute(f"""
                SELECT *
                FROM saved_papers
                WHERE user_id = ?
                  AND (
                    TRIM(COALESCE(folder_name, '')) = ''
                    OR folder_name = '未分類'
                    OR folder_name = 'あとで見る'
                  )
                  {source_clause}
                ORDER BY created_at DESC
            """, (user_id, *source_params))
        else:
            cur.execute(f"""
                SELECT *
                FROM saved_papers
                WHERE user_id = ? AND folder_name = ?
                {source_clause}
                ORDER BY created_at DESC
            """, (user_id, normalized_folder_name, *source_params))

    rows = cur.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def upsert_paper_history(
    user_id: int,
    pubmed_id: str,
    title: str = "",
    jp_title: str = "",
    authors: str = "",
    journal: str = "",
    pubdate: str = "",
    abstract: str = "",
    summary_jp: str = "",
    clinical_score: str = "",
    clinical_reason: str = "",
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO paper_history (
            user_id, pubmed_id, title, jp_title, authors, journal, pubdate, abstract,
            summary_jp, clinical_score, clinical_reason, viewed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id, pubmed_id) DO UPDATE SET
            title = excluded.title,
            jp_title = excluded.jp_title,
            authors = excluded.authors,
            journal = excluded.journal,
            pubdate = excluded.pubdate,
            abstract = excluded.abstract,
            summary_jp = excluded.summary_jp,
            clinical_score = excluded.clinical_score,
            clinical_reason = excluded.clinical_reason,
            viewed_at = CURRENT_TIMESTAMP
        """,
        (
            user_id,
            pubmed_id,
            title,
            jp_title,
            authors,
            journal,
            pubdate,
            abstract,
            summary_jp,
            clinical_score,
            clinical_reason,
        ),
    )
    conn.commit()
    conn.close()


def get_paper_history(user_id: int, limit: int | None = None):
    conn = get_connection()
    cur = conn.cursor()
    sql = """
        SELECT *
        FROM paper_history
        WHERE user_id = ?
        ORDER BY viewed_at DESC
    """
    params: list = [user_id]
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def create_paper_comment(
    user_id: int,
    pubmed_id: str,
    content: str,
    paper_title: str = "",
    paper_jp_title: str = "",
):
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        """
        INSERT INTO paper_comments (
            user_id, pubmed_id, paper_title, paper_jp_title, content, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            str(pubmed_id).strip(),
            (paper_title or "").strip(),
            (paper_jp_title or "").strip(),
            (content or "").strip(),
            now,
            now,
        ),
    )
    comment_id = cur.lastrowid
    conn.commit()
    conn.close()
    return comment_id


def get_paper_comments(pubmed_id: str, limit: int = 50):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            paper_comments.*,
            users.email AS user_email,
            users.display_name AS user_display_name,
            users.avatar AS user_avatar
        FROM paper_comments
        JOIN users ON users.id = paper_comments.user_id
        WHERE paper_comments.pubmed_id = ?
        ORDER BY paper_comments.created_at DESC, paper_comments.id DESC
        LIMIT ?
        """,
        (str(pubmed_id).strip(), int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_paper_comment_counts(pubmed_ids):
    ids = [str(pid).strip() for pid in (pubmed_ids or []) if str(pid).strip()]
    if not ids:
        return {}

    conn = get_connection()
    cur = conn.cursor()
    placeholders = ",".join(["?"] * len(ids))
    cur.execute(
        f"""
        SELECT pubmed_id, COUNT(*) AS comment_count
        FROM paper_comments
        WHERE pubmed_id IN ({placeholders})
        GROUP BY pubmed_id
        """,
        ids,
    )
    rows = cur.fetchall()
    conn.close()
    return {str(row["pubmed_id"]): int(row["comment_count"] or 0) for row in rows}


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
              AND save_source IN ('manual_save')
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
              AND save_source IN ('manual_save')
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
          AND save_source IN ('manual_save')
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


def get_user_memo_map_layout(user_id: int) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT layout_json FROM memo_map_layouts WHERE user_id = ?",
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return {}
    raw_value = row["layout_json"] if isinstance(row, sqlite3.Row) else row[0]
    try:
        parsed = json.loads(raw_value or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def upsert_user_memo_map_layout(user_id: int, layout: dict):
    normalized_layout = layout if isinstance(layout, dict) else {}
    payload = json.dumps(normalized_layout, ensure_ascii=False)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO memo_map_layouts (user_id, layout_json, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            layout_json = excluded.layout_json,
            updated_at = excluded.updated_at
        """,
        (user_id, payload, now, now),
    )
    conn.commit()
    conn.close()


def get_user_memo_mind_map(user_id: int) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT map_json FROM memo_mind_maps WHERE user_id = ?",
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return {}
    raw_value = row["map_json"] if isinstance(row, sqlite3.Row) else row[0]
    try:
        parsed = json.loads(raw_value or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def upsert_user_memo_mind_map(user_id: int, mind_map: dict):
    normalized_map = mind_map if isinstance(mind_map, dict) else {}
    payload = json.dumps(normalized_map, ensure_ascii=False)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO memo_mind_maps (user_id, map_json, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            map_json = excluded.map_json,
            updated_at = excluded.updated_at
        """,
        (user_id, payload, now, now),
    )
    conn.commit()
    conn.close()


def list_user_memo_mind_map_files(user_id: int) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, user_id, title, map_json, created_at, updated_at
        FROM memo_mind_map_files
        WHERE user_id = ?
        ORDER BY updated_at DESC, id DESC
        """,
        (user_id,),
    )
    rows = cur.fetchall()
    conn.close()
    files = []
    for row in rows:
        item = dict(row)
        try:
            parsed = json.loads(item.get("map_json") or "{}")
        except Exception:
            parsed = {}
        item["map_json"] = parsed if isinstance(parsed, dict) else {}
        files.append(item)
    return files


def get_user_memo_mind_map_file(user_id: int, file_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, user_id, title, map_json, created_at, updated_at
        FROM memo_mind_map_files
        WHERE user_id = ? AND id = ?
        LIMIT 1
        """,
        (user_id, file_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    item = dict(row)
    try:
        parsed = json.loads(item.get("map_json") or "{}")
    except Exception:
        parsed = {}
    item["map_json"] = parsed if isinstance(parsed, dict) else {}
    return item


def create_user_memo_mind_map_file(user_id: int, title: str, mind_map: dict | None = None) -> int:
    normalized_map = mind_map if isinstance(mind_map, dict) else {}
    payload = json.dumps(normalized_map, ensure_ascii=False)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO memo_mind_map_files (user_id, title, map_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (user_id, (title or "").strip() or "新しいマップ", payload, now, now),
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(new_id)


def update_user_memo_mind_map_file(user_id: int, file_id: int, *, title: str | None = None, mind_map: dict | None = None) -> bool:
    updates = []
    params: list[Any] = []
    if title is not None:
        updates.append("title = ?")
        params.append((title or "").strip() or "新しいマップ")
    if mind_map is not None:
        updates.append("map_json = ?")
        params.append(json.dumps(mind_map if isinstance(mind_map, dict) else {}, ensure_ascii=False))
    if not updates:
        return False
    updates.append("updated_at = ?")
    params.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    params.extend([user_id, file_id])
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        f"""
        UPDATE memo_mind_map_files
        SET {", ".join(updates)}
        WHERE user_id = ? AND id = ?
        """,
        tuple(params),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def delete_user_memo_mind_map_file(user_id: int, file_id: int) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM memo_mind_map_files WHERE user_id = ? AND id = ?",
        (user_id, file_id),
    )
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


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
    if clean_folder_name in {"あとで見る", "未分類"}:
        clean_folder_name = ""

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


# ─── 友人用プロモコード ────────────────────────────────────────────────

def get_friend_promo_code(code: str) -> dict | None:
    """コード文字列で friend_promo_codes を検索して返す。見つからなければ None。"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM friend_promo_codes WHERE code = ?",
        (code.strip().upper(),)
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def use_friend_promo_code(code_id: int) -> None:
    """used_count を 1 増やす。"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        "UPDATE friend_promo_codes SET used_count = used_count + 1 WHERE id = ?",
        (code_id,)
    )
    conn.commit()
    conn.close()


def set_friend_promo_target_email(code_id: int, email: str) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE friend_promo_codes
        SET target_email = ?
        WHERE id = ?
          AND (target_email = '' OR lower(target_email) = ?)
        """,
        ((email or "").strip().lower(), code_id, (email or "").strip().lower())
    )
    updated = cur.rowcount > 0
    conn.commit()
    conn.close()
    return updated


def apply_promo_to_user(user_id: int, plan: str, ends_at: str, code: str) -> None:
    """ユーザーの promo フィールドを更新する。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE users
        SET promo_plan        = ?,
            promo_ends_at     = ?,
            promo_code_used   = ?,
            promo_code_used_at = ?,
            promo_is_lifetime = 0
        WHERE id = ?
        """,
        (plan, ends_at, code.strip().upper(), now, user_id)
    )
    conn.commit()
    conn.close()


def apply_lifetime_promo_to_user(user_id: int, plan: str, code: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE users
        SET promo_plan        = ?,
            promo_ends_at     = '',
            promo_code_used   = ?,
            promo_code_used_at = ?,
            promo_is_lifetime = 1
        WHERE id = ?
        """,
        (plan, code.strip().upper(), now, user_id)
    )
    conn.commit()
    conn.close()


def get_supporter_campaign_claim_counts():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT campaign_slug, COUNT(*) AS claim_count
        FROM supporter_campaign_claims
        WHERE status = 'active'
        GROUP BY campaign_slug
    """)
    rows = cur.fetchall()
    conn.close()
    return {row["campaign_slug"]: int(row["claim_count"] or 0) for row in rows}


def get_user_supporter_campaign_claim(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM supporter_campaign_claims
        WHERE user_id = ?
          AND status = 'active'
        LIMIT 1
    """, (user_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def claim_supporter_campaign(user_id: int, campaign_slug: str, campaign_limit: int):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    try:
        cur.execute("BEGIN IMMEDIATE")

        cur.execute("""
            SELECT *
            FROM supporter_campaign_claims
            WHERE user_id = ?
              AND status = 'active'
            LIMIT 1
        """, (user_id,))
        existing = cur.fetchone()
        if existing:
            conn.rollback()
            return False, "already_claimed", dict(existing)

        cur.execute("""
            SELECT COUNT(*) AS claim_count
            FROM supporter_campaign_claims
            WHERE campaign_slug = ?
              AND status = 'active'
        """, (campaign_slug,))
        current_count = int(cur.fetchone()["claim_count"] or 0)
        if current_count >= int(campaign_limit or 0):
            conn.rollback()
            return False, "sold_out", None

        cur.execute("""
            INSERT INTO supporter_campaign_claims (user_id, campaign_slug, status)
            VALUES (?, ?, 'active')
        """, (user_id, campaign_slug))
        conn.commit()
        return True, "ok", {
            "user_id": user_id,
            "campaign_slug": campaign_slug,
            "status": "active",
        }
    except sqlite3.IntegrityError:
        conn.rollback()
        return False, "already_claimed", None
    finally:
        conn.close()


def create_master_article_draft(
    *,
    pubmed_id: str,
    source_title: str,
    source_jp_title: str,
    source_summary_jp: str,
    source_abstract: str,
    source_clinical_score: str,
    source_clinical_reason: str,
    article_title: str,
    article_excerpt: str,
    article_slug: str,
    article_html: str,
    created_by_user_id: int | None = None,
):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO master_article_drafts (
            pubmed_id,
            source_title,
            source_jp_title,
            source_summary_jp,
            source_abstract,
            source_clinical_score,
            source_clinical_reason,
            article_title,
            article_excerpt,
            article_slug,
            article_html,
            created_by_user_id,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pubmed_id,
            source_title,
            source_jp_title,
            source_summary_jp,
            source_abstract,
            source_clinical_score,
            source_clinical_reason,
            article_title,
            article_excerpt,
            article_slug,
            article_html,
            created_by_user_id,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
    )
    draft_id = cur.lastrowid
    variant_cycle = ["A", "B", "C"]
    marketing_variant = variant_cycle[(max(int(draft_id or 1), 1) - 1) % len(variant_cycle)]
    cur.execute(
        """
        UPDATE master_article_drafts
        SET marketing_variant = ?
        WHERE id = ?
        """,
        (marketing_variant, draft_id)
    )
    conn.commit()
    conn.close()
    return draft_id


def get_master_article_drafts(limit: int = 30):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM master_article_drafts
        ORDER BY updated_at DESC, created_at DESC
        LIMIT ?
        """,
        (limit,)
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_master_article_draft(draft_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM master_article_drafts
        WHERE id = ?
        LIMIT 1
        """,
        (draft_id,)
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def update_master_article_draft_content(draft_id: int, article_title: str, article_excerpt: str, article_slug: str, article_html: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE master_article_drafts
        SET article_title = ?,
            article_excerpt = ?,
            article_slug = ?,
            article_html = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            article_title,
            article_excerpt,
            article_slug,
            article_html,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            draft_id,
        )
    )
    conn.commit()
    conn.close()


def update_master_article_draft_geo_review(
    draft_id: int,
    geo_score: int,
    geo_feedback: str,
    article_title: str,
    article_excerpt: str,
    article_slug: str,
    article_html: str,
):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE master_article_drafts
        SET geo_score = ?,
            geo_feedback = ?,
            geo_last_reviewed_at = ?,
            article_title = ?,
            article_excerpt = ?,
            article_slug = ?,
            article_html = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            int(geo_score or 0),
            geo_feedback,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            article_title,
            article_excerpt,
            article_slug,
            article_html,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            draft_id,
        )
    )
    conn.commit()
    conn.close()


def mark_master_article_wordpress_posted(draft_id: int, wordpress_post_id: str, wordpress_status: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE master_article_drafts
        SET wordpress_post_id = ?,
            wordpress_status = ?,
            wordpress_published_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            wordpress_post_id,
            wordpress_status,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            draft_id,
        )
    )
    conn.commit()
    conn.close()


def _get_master_wordpress_settings_row(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM master_wordpress_settings
        WHERE user_id = ?
        LIMIT 1
        """,
        (user_id,)
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_master_wordpress_settings(user_id: int):
    row = _get_master_wordpress_settings_row(user_id)
    if not row:
        return None

    decrypted_password, is_encrypted = decrypt_wordpress_secret(row.get("app_password") or "")
    row["app_password_raw"] = row.get("app_password") or ""
    row["app_password_is_encrypted"] = is_encrypted
    row["app_password_unavailable"] = bool(is_encrypted and not decrypted_password)
    row["app_password"] = decrypted_password
    return row


def upsert_master_wordpress_settings(
    user_id: int,
    site_url: str,
    username: str,
    app_password: str | None = None,
    app_base_url: str | None = None,
):
    existing = get_master_wordpress_settings(user_id)
    existing_raw = _get_master_wordpress_settings_row(user_id)
    normalized_password = (app_password or "").strip()
    if existing_raw and not normalized_password:
        existing_password_raw = (existing_raw.get("app_password") or "").strip()
        if existing_password_raw and not _is_encrypted_wordpress_secret(existing_password_raw):
            normalized_password = encrypt_wordpress_secret(existing_password_raw)
        else:
            normalized_password = existing_password_raw
    elif normalized_password:
        normalized_password = encrypt_wordpress_secret(normalized_password)
    normalized_app_base_url = (app_base_url or "").strip()
    if existing and not normalized_app_base_url:
        normalized_app_base_url = existing.get("app_base_url") or ""

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if existing:
        cur.execute(
            """
            UPDATE master_wordpress_settings
            SET site_url = ?,
                username = ?,
                app_password = ?,
                app_base_url = ?,
                updated_at = ?
            WHERE user_id = ?
            """,
            (
                site_url,
                username,
                normalized_password,
                normalized_app_base_url,
                now,
                user_id,
            )
        )
    else:
        cur.execute(
            """
            INSERT INTO master_wordpress_settings (
                user_id,
                site_url,
                username,
                app_password,
                app_base_url,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                site_url,
                username,
                normalized_password,
                normalized_app_base_url,
                now,
                now,
            )
        )
    conn.commit()
    conn.close()


def record_master_article_marketing_event(
    draft_id: int,
    event_type: str,
    variant: str = "",
    source: str = "",
    user_id: int | None = None,
    ip_hash: str = "",
    user_agent: str = "",
):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO master_article_marketing_events (
            draft_id,
            event_type,
            variant,
            source,
            user_id,
            ip_hash,
            user_agent
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            draft_id,
            event_type,
            variant,
            source,
            user_id,
            ip_hash,
            user_agent,
        )
    )
    conn.commit()
    conn.close()


def set_user_article_attribution(user_id: int, channel: str, draft_id: int, variant: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE users
        SET acquisition_channel = ?,
            acquisition_article_draft_id = ?,
            acquisition_article_variant = ?,
            acquisition_at = ?
        WHERE id = ?
        """,
        (
            channel,
            draft_id,
            variant,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            user_id,
        )
    )
    conn.commit()
    conn.close()


def get_master_article_marketing_summary(created_by_user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            d.id AS draft_id,
            COALESCE(e.impressions, 0) AS impressions,
            COALESCE(e.clicks, 0) AS clicks,
            COALESCE(u.registrations, 0) AS registrations,
            COALESCE(u.paid_users, 0) AS paid_users
        FROM master_article_drafts d
        LEFT JOIN (
            SELECT
                draft_id,
                SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) AS impressions,
                SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks
            FROM master_article_marketing_events
            GROUP BY draft_id
        ) e
          ON e.draft_id = d.id
        LEFT JOIN (
            SELECT
                acquisition_article_draft_id AS draft_id,
                COUNT(*) AS registrations,
                SUM(
                    CASE
                        WHEN LOWER(COALESCE(plan, '')) IN ('pro', 'expert')
                          OR LOWER(COALESCE(promo_plan, '')) IN ('pro', 'expert')
                        THEN 1 ELSE 0
                    END
                ) AS paid_users
            FROM users
            WHERE acquisition_article_draft_id IS NOT NULL
            GROUP BY acquisition_article_draft_id
        ) u
          ON u.draft_id = d.id
        WHERE d.created_by_user_id = ?
        ORDER BY d.updated_at DESC, d.created_at DESC
        """,
        (created_by_user_id,)
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_master_wordpress_autopost_settings(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM master_wordpress_autopost_settings
        WHERE user_id = ?
        LIMIT 1
        """,
        (user_id,)
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_master_wordpress_autopost_settings(user_id: int, is_enabled: int, daily_time: str):
    existing = get_master_wordpress_autopost_settings(user_id)
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if existing:
        cur.execute(
            """
            UPDATE master_wordpress_autopost_settings
            SET is_enabled = ?,
                daily_time = ?,
                updated_at = ?
            WHERE user_id = ?
            """,
            (
                int(is_enabled or 0),
                daily_time,
                now,
                user_id,
            )
        )
    else:
        cur.execute(
            """
            INSERT INTO master_wordpress_autopost_settings (
                user_id,
                is_enabled,
                daily_time,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                user_id,
                int(is_enabled or 0),
                daily_time,
                now,
                now,
            )
        )
    conn.commit()
    conn.close()


def update_master_wordpress_autopost_run_state(user_id: int, attempted_date: str, success_date: str | None = None):
    existing = get_master_wordpress_autopost_settings(user_id)
    if not existing:
        upsert_master_wordpress_autopost_settings(user_id, 0, "09:00")
        existing = get_master_wordpress_autopost_settings(user_id) or {}

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE master_wordpress_autopost_settings
        SET last_attempted_date = ?,
            last_success_date = ?,
            updated_at = ?
        WHERE user_id = ?
        """,
        (
            attempted_date or "",
            success_date if success_date is not None else (existing.get("last_success_date") or ""),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            user_id,
        )
    )
    conn.commit()
    conn.close()


def create_master_wordpress_autopost_log(
    user_id: int,
    status: str,
    message: str,
    draft_id: int | None = None,
    wordpress_post_id: str = "",
):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO master_wordpress_autopost_logs (
            user_id,
            draft_id,
            status,
            message,
            wordpress_post_id
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            user_id,
            draft_id,
            status,
            message,
            wordpress_post_id,
        )
    )
    conn.commit()
    conn.close()


def get_master_wordpress_autopost_logs(user_id: int, limit: int = 20):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM master_wordpress_autopost_logs
        WHERE user_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (user_id, limit)
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_master_wordpress_autopost_enabled_settings():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM master_wordpress_autopost_settings
        WHERE is_enabled = 1
        ORDER BY user_id ASC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_next_master_article_draft_for_autopost(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM master_article_drafts
        WHERE created_by_user_id = ?
          AND COALESCE(wordpress_post_id, '') = ''
        ORDER BY geo_score DESC, updated_at DESC, created_at DESC
        LIMIT 1
        """,
        (user_id,)
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def create_user_feedback(user_id: int, category: str, message: str, page_context: str = "mypage"):
    normalized_category = (category or "general").strip() or "general"
    normalized_message = (message or "").strip()
    normalized_context = (page_context or "mypage").strip() or "mypage"

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO user_feedback (
            user_id,
            category,
            message,
            page_context
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            user_id,
            normalized_category,
            normalized_message,
            normalized_context,
        )
    )
    conn.commit()
    conn.close()


def seed_initial_promo_codes() -> None:
    """初期プロモコードを投入する（既存コードはスキップ）。
    アプリ起動時に呼び出すことで本番DBにも自動反映される。
    """
    initial_codes = [
        # (code, plan_to_grant, free_days, grant_lifetime, max_uses, target_email)
        ("MASTER-ET08",      "pro", 0,  1, 1, "e.toshihide08@gmail.com"),
        ("LIFETIME1",        "pro", 0,  1, 1, ""),
        ("LIFETIME-FDBC47",  "pro", 0,  1, 1, ""),
        ("FREE90X5",         "pro", 90, 0, 5, ""),
        ("FREE90-A9FDD0",    "pro", 90, 0, 1, ""),
        ("FREE90-E2C645",    "pro", 90, 0, 1, ""),
        ("FREE90-24A2DA",    "pro", 90, 0, 1, ""),
        ("FREE90-351BA9",    "pro", 90, 0, 1, ""),
        ("FREE90-8764C8",    "pro", 90, 0, 1, ""),
    ]
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for code, plan, days, lifetime, max_uses, email in initial_codes:
        cur.execute(
            """
            INSERT OR IGNORE INTO friend_promo_codes
                (code, plan_to_grant, free_days, grant_lifetime, max_uses, target_email, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1)
            """,
            (code, plan, days, lifetime, max_uses, email),
        )
    conn.commit()
    conn.close()


def get_all_friend_promo_codes() -> list[dict]:
    """全プロモコードを返す（管理用）。"""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM friend_promo_codes ORDER BY id DESC")
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_friend_promo_code(
    code: str,
    plan_to_grant: str,
    free_days: int,
    grant_lifetime: int,
    max_uses: int,
    target_email: str = "",
) -> bool:
    """新規プロモコードを作成する。重複コードは False を返す。"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO friend_promo_codes
                (code, plan_to_grant, free_days, grant_lifetime, max_uses, target_email, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1)
            """,
            (
                code.strip().upper(),
                plan_to_grant,
                int(free_days),
                int(grant_lifetime),
                int(max_uses),
                (target_email or "").strip().lower(),
            ),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False


def toggle_friend_promo_code_active(code_id: int) -> bool:
    """is_active を反転させて、反転後の値を返す。"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        "UPDATE friend_promo_codes SET is_active = 1 - is_active WHERE id = ?",
        (code_id,),
    )
    conn.commit()
    cur.execute("SELECT is_active FROM friend_promo_codes WHERE id = ?", (code_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row[0]) if row else False
