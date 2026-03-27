from __future__ import annotations

from dataclasses import dataclass

from pymongo import MongoClient, ASCENDING

from .config import Config


@dataclass(frozen=True)
class Collections:
    media: str = "media"
    users: str = "users"
    jobs: str = "jobs"


_CLIENT: MongoClient | None = None


def get_client() -> MongoClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = MongoClient(Config.MONGODB_URI)
    return _CLIENT


def get_db():
    return get_client()[Config.MONGODB_DB]


def ensure_indexes() -> None:
    db = get_db()
    c = Collections()

    # ── light migration for older schema ──────────────────────────────────────
    # Old docs may have: users.username (case-sensitive) and media.username.
    # We backfill username_norm / title_only to avoid unique-index errors.
    try:
        # users: backfill username_norm + username_display from username
        db[c.users].update_many(
            {"username_norm": {"$exists": False}, "username": {"$type": "string"}},
            [{"$set": {"username_norm": {"$toLower": "$username"}, "username_display": "$username"}}],
        )
        # users: backfill title_only = 0 where missing
        db[c.users].update_many(
            {"title_only": {"$exists": False}},
            {"$set": {"title_only": 0}},
        )

        # media: backfill username_norm + username_display from username
        db[c.media].update_many(
            {"username_norm": {"$exists": False}, "username": {"$type": "string"}},
            [{"$set": {"username_norm": {"$toLower": "$username"}, "username_display": "$username"}}],
        )
        # media: backfill title_only = 0 where missing
        db[c.media].update_many(
            {"title_only": {"$exists": False}},
            {"$set": {"title_only": 0}},
        )

        # delete unusable docs that would block unique indexes
        db[c.users].delete_many({"username_norm": None})
        db[c.media].delete_many({"username_norm": None})
    except Exception:
        # Don't block app startup on migration
        pass

    # ── drop ALL legacy indexes that conflict with the new composite keys ─────
    # (old single/two-field indexes must be removed before new ones are created)
    for coll, idx_name in [
        # old media indexes
        (c.media, "username_norm_1_media_url_1"),
        (c.media, "username_1_media_url_1"),
        (c.media, "username_norm_1_post_date_1"),
        (c.media, "username_norm_1_media_type_1"),
        # old users indexes
        (c.users, "username_norm_1"),
        (c.users, "username_1"),
        # new-format indexes from previous broken deployments (drop to recreate cleanly)
        (c.media, "username_norm_1_title_only_1_media_url_1"),
        (c.media, "username_norm_1_title_only_1_post_date_1"),
        (c.media, "username_norm_1_title_only_1_media_type_1"),
        (c.users, "username_norm_1_title_only_1"),
    ]:
        try:
            db[coll].drop_index(idx_name)
        except Exception:
            pass

    # ── media indexes ─────────────────────────────────────────────────────────
    # BUG FIX: unique key is now (username_norm, title_only, media_url)
    # so full-scrape (title_only=0) and title-only-scrape (title_only=1)
    # are stored as completely separate datasets and never collide.
    db[c.media].create_index(
        [("username_norm", ASCENDING), ("title_only", ASCENDING), ("media_url", ASCENDING)],
        unique=True,
        partialFilterExpression={"username_norm": {"$type": "string"}},
        name="username_norm_1_title_only_1_media_url_1",
    )
    db[c.media].create_index(
        [("username_norm", ASCENDING), ("title_only", ASCENDING), ("post_date", ASCENDING)],
        partialFilterExpression={"username_norm": {"$type": "string"}},
        name="username_norm_1_title_only_1_post_date_1",
    )
    db[c.media].create_index(
        [("username_norm", ASCENDING), ("title_only", ASCENDING), ("media_type", ASCENDING)],
        partialFilterExpression={"username_norm": {"$type": "string"}},
        name="username_norm_1_title_only_1_media_type_1",
    )

    # ── users index ───────────────────────────────────────────────────────────
    # BUG FIX: unique key is now (username_norm, title_only)
    # so the same username can have two separate cache entries:
    #   title_only=0 → full scrape
    #   title_only=1 → title-only scrape
    db[c.users].create_index(
        [("username_norm", ASCENDING), ("title_only", ASCENDING)],
        unique=True,
        partialFilterExpression={"username_norm": {"$type": "string"}},
        name="username_norm_1_title_only_1",
    )

    # ── jobs index ────────────────────────────────────────────────────────────
    db[c.jobs].create_index([("created_at", ASCENDING)])
