from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import Config


def _to_sync_url(url: str) -> str:
    if not url:
        return url
    return (
        url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
        .replace("postgres+asyncpg://", "postgresql+psycopg2://")
        .replace("postgres://", "postgresql://")
    )


_ENGINE: Engine | None = None


def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    if not Config.DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required for postgres backend")
    _ENGINE = create_engine(
        _to_sync_url(Config.DATABASE_URL),
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )
    return _ENGINE


def ensure_schema() -> None:
    eng = get_engine()
    with eng.begin() as conn:

        # ── users ─────────────────────────────────────────────────────────
        # PK is (username_norm, title_only):
        #   title_only=0  →  full scrape (separate dataset)
        #   title_only=1  →  title-only scrape (separate dataset)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
              username_norm       TEXT        NOT NULL,
              username_display    TEXT        NOT NULL,
              cached_newer_than   TEXT        NOT NULL,
              cached_older_than   TEXT        NOT NULL,
              last_scraped_at     TIMESTAMPTZ NOT NULL,
              title_only          INT         NOT NULL DEFAULT 0,
              PRIMARY KEY (username_norm, title_only)
            );
        """))

        # ── media ─────────────────────────────────────────────────────────
        # UNIQUE is (username_norm, title_only, media_url)
        # full-scrape and title-only data are stored completely separately
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS media (
              id               BIGSERIAL   PRIMARY KEY,
              username_norm    TEXT        NOT NULL,
              username_display TEXT        NOT NULL,
              post_date        TEXT        NOT NULL,
              media_url        TEXT        NOT NULL,
              media_type       TEXT        NOT NULL,
              created_at       TIMESTAMPTZ NOT NULL,
              title_only       INT         NOT NULL DEFAULT 0,
              UNIQUE (username_norm, title_only, media_url)
            );
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_media_user_date_to
                ON media (username_norm, title_only, post_date);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_media_user_type_to
                ON media (username_norm, title_only, media_type);
        """))

        # ── jobs ──────────────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS jobs (
              job_id           TEXT        PRIMARY KEY,
              status           TEXT        NOT NULL,
              username_norm    TEXT,
              username_display TEXT,
              created_at       TIMESTAMPTZ NOT NULL,
              started_at       TIMESTAMPTZ,
              finished_at      TIMESTAMPTZ,
              inserted         BIGINT      DEFAULT 0,
              matched_posts    BIGINT      DEFAULT 0,
              page             INT,
              total_pages      INT,
              batch            INT         DEFAULT 0,
              range_newer_than TEXT,
              range_older_than TEXT,
              title_only       INT         DEFAULT 0,
              error            TEXT
            );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs (created_at DESC);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_jobs_status  ON jobs (status);"))

        # ── safe column migrations for existing deployments ────────────────
        # (run migrate_title_only_separation.py first to fix PK/UNIQUE keys)
        conn.execute(text("ALTER TABLE jobs  ADD COLUMN IF NOT EXISTS title_only INT DEFAULT 0;"))
        conn.execute(text("ALTER TABLE media ADD COLUMN IF NOT EXISTS title_only INT NOT NULL DEFAULT 0;"))
        conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS title_only INT DEFAULT 0;"))
