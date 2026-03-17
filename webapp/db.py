from __future__ import annotations

from dataclasses import dataclass

from pymongo import MongoClient, ASCENDING

from .config import Config


@dataclass(frozen=True)
class Collections:
    media: str = "media"
    users: str = "users"
    jobs: str = "jobs"


def get_client() -> MongoClient:
    return MongoClient(Config.MONGODB_URI)


def get_db():
    client = get_client()
    return client[Config.MONGODB_DB]


def ensure_indexes() -> None:
    db = get_db()
    c = Collections()

    # --- light migration for older schema ---
    # Old docs may have: users.username (case-sensitive) and media.username.
    # We backfill username_norm to avoid unique-index errors on null.
    try:
        # users: set username_norm + username_display from username
        db[c.users].update_many(
            {"username_norm": {"$exists": False}, "username": {"$type": "string"}},
            [{"$set": {"username_norm": {"$toLower": "$username"}, "username_display": "$username"}}],
        )
        # media: set username_norm + username_display from username
        db[c.media].update_many(
            {"username_norm": {"$exists": False}, "username": {"$type": "string"}},
            [{"$set": {"username_norm": {"$toLower": "$username"}, "username_display": "$username"}}],
        )
        # delete unusable docs that would block unique indexes
        db[c.users].delete_many({"username_norm": None})
        db[c.media].delete_many({"username_norm": None})
    except Exception:
        # Don't block app startup on migration; indexes below use partial filters.
        pass

    # Drop conflicting legacy indexes (same name, different options)
    for coll, idx_name in [
        (c.media, "username_norm_1_media_url_1"),
        (c.media, "username_1_media_url_1"),
        (c.users, "username_norm_1"),
        (c.users, "username_1"),
        (c.media, "username_norm_1_post_date_1"),
        (c.media, "username_norm_1_media_type_1"),
    ]:
        try:
            db[coll].drop_index(idx_name)
        except Exception:
            pass

    # Media uniqueness: per-username (normalized) unique URL
    db[c.media].create_index(
        [("username_norm", ASCENDING), ("media_url", ASCENDING)],
        unique=True,
        partialFilterExpression={"username_norm": {"$type": "string"}},
    )
    db[c.media].create_index([("username_norm", ASCENDING), ("post_date", ASCENDING)], partialFilterExpression={"username_norm": {"$type": "string"}})
    db[c.media].create_index([("username_norm", ASCENDING), ("media_type", ASCENDING)], partialFilterExpression={"username_norm": {"$type": "string"}})

    # Users
    db[c.users].create_index([("username_norm", ASCENDING)], unique=True, partialFilterExpression={"username_norm": {"$type": "string"}})

    # Jobs
    db[c.jobs].create_index([("created_at", ASCENDING)])
