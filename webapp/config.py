import os

from dotenv import load_dotenv


load_dotenv()


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None or v == "":
        if default is None:
            raise RuntimeError(f"Missing required env var: {name}")
        return default
    return v


class Config:
    SECRET_KEY = env("SECRET_KEY", "dev-secret-change-me")

    # Auth (defaults requested by user)
    ADMIN_USER = env("ADMIN_USER", "admin")
    ADMIN_PASSWORD = env("ADMIN_PASSWORD", "devpsw")
    CLIENT_USER = env("CLIENT_USER", "client")
    CLIENT_PASSWORD = env("CLIENT_PASSWORD", "dev007")

    # Scrape years
    NEWER_THAN = env("NEWER_THAN", "2019")
    OLDER_THAN = env("OLDER_THAN", "2026")

    # Storage backend: "postgres" (default) or "mongo"
    DB_BACKEND = env("DB_BACKEND", "postgres").lower()

    # Postgres (Railway)
    DATABASE_URL = env("DATABASE_URL", "")

    # Mongo
    MONGODB_URI = env("MONGODB_URI", "mongodb://localhost:27017")
    MONGODB_DB = env("MONGODB_DB", "g2media")
