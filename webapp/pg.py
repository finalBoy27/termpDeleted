from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import Config


def _to_sync_url(url: str) -> str:
    """
    Accept Railway/async URLs like:
      postgresql+asyncpg://...
    and convert to a sync driver for Flask usage:
      postgresql+psycopg2://...
    """
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
    _ENGINE = create_engine(_to_sync_url(Config.DATABASE_URL), pool_pre_ping=True)
    return _ENGINE


def ensure_schema() -> None:
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS users (
                  username_norm TEXT PRIMARY KEY,
                  username_display TEXT NOT NULL,
                  cached_newer_than TEXT NOT NULL,
                  cached_older_than TEXT NOT NULL,
                  last_scraped_at TIMESTAMPTZ NOT NULL
                );

                CREATE TABLE IF NOT EXISTS media (
                  id BIGSERIAL PRIMARY KEY,
                  username_norm TEXT NOT NULL,
                  username_display TEXT NOT NULL,
                  post_date TEXT NOT NULL,
                  media_url TEXT NOT NULL,
                  media_type TEXT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL,
                  UNIQUE (username_norm, media_url)
                );

                CREATE INDEX IF NOT EXISTS idx_media_user_date ON media (username_norm, post_date);
                CREATE INDEX IF NOT EXISTS idx_media_user_type ON media (username_norm, media_type);

                CREATE TABLE IF NOT EXISTS jobs (
                  job_id TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  username_norm TEXT,
                  username_display TEXT,
                  created_at TIMESTAMPTZ NOT NULL,
                  started_at TIMESTAMPTZ,
                  finished_at TIMESTAMPTZ,
                  inserted BIGINT DEFAULT 0,
                  matched_posts BIGINT DEFAULT 0,
                  page INT,
                  total_pages INT,
                  batch INT DEFAULT 0,
                  range_newer_than TEXT,
                  range_older_than TEXT,
                  error TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs (created_at DESC);
                """
            )
        )

