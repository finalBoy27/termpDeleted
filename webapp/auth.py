from __future__ import annotations

from functools import wraps
from typing import Callable, TypeVar, Any

from flask import session, redirect, url_for, request

from .config import Config

T = TypeVar("T", bound=Callable[..., Any])


def login_user(role: str) -> None:
    session["role"] = role


def logout_user() -> None:
    session.pop("role", None)


def current_role() -> str | None:
    r = session.get("role")
    return r if isinstance(r, str) else None


def require_role(role: str):
    def deco(fn: T) -> T:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if current_role() != role:
                next_url = request.full_path if request.query_string else request.path
                if role == "admin":
                    return redirect(url_for("admin_login", next=next_url))
                return redirect(url_for("client_login", next=next_url))
            return fn(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return deco


def check_credentials(role: str, username: str, password: str) -> bool:
    if role == "admin":
        return username == Config.ADMIN_USER and password == Config.ADMIN_PASSWORD
    if role == "client":
        return username == Config.CLIENT_USER and password == Config.CLIENT_PASSWORD
    return False
