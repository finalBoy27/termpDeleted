from __future__ import annotations

import re


_WS_RE = re.compile(r"\s+")


def normalize_username(name: str) -> str:
    """
    Case-insensitive + whitespace-normalized key.
    Example: "  Tanu   Jain " -> "tanu jain"
    """
    name = (name or "").strip().lower()
    name = _WS_RE.sub(" ", name)
    return name


def split_usernames(raw: str) -> list[str]:
    """
    Accept comma-separated usernames.
    Keeps original token text (trimmed) for display, but removes empties.
    """
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]

