"""
============================

Overview
--------
Efficient authentication service for the XTS Market-Data API with support for
login, logout, and session management. Provides both programmatic and CLI interfaces.

The module:
- Loads credentials and endpoints from environment variables (via `python-dotenv`).
- Resolves a `uniqueKey` from an internal host-lookup microservice.
- Performs login/logout HTTP calls using `httpx` with context manager support.
- Persists session identifiers (`UNIQUE_KEY`, `SECRET_UNIQUE_KEY`, `USER_ID`) to `.env` for reuse.
- Clears stale session keys on initialization for clean state management.

Environment variables expected:
- `API_KEY_MARKET_DATA`: Public application key.
- `API_SECRET_MARKET_DATA`: Secret key for the Market-Data API.
- `SOURCE`: Source identifier (defaults to `"WEB"`).
- `ROOT_URL`: Base URL for the XTS gateway (e.g., `https://hostname:port`).


Dataclasses
-----------

`AuthEnv`
    Frozen dataclass encapsulating authentication configuration.
    
    Attributes:
    - `app_key` (str): Market Data API public key.
    - `secret_key` (str): Market Data API secret key.
    - `root_url` (str): Base URL for XTS gateway (normalized without trailing slash).
    - `source` (str): Source identifier, defaults to "WEB".
    - `env_file_path` (Path): Path to `.env` file for credential storage.
    
    Properties:
    - `login_url`: Derived URL for login endpoint.
    - `logout_url`: Derived URL for logout endpoint.
    
    Classmethod:
    - `from_environment(env_file_path)`: Loads configuration from environment variables.
      Raises `ValueError` if required variables are missing.

`EnvStore`
    Manages persistent `.env` file operations.
    
    Methods:
    - `ensure_file()`: Creates `.env` if it doesn't exist.
    - `get(key, default=None)`: Retrieves environment variable value.
    - `set(key, value)`: Writes and updates environment variable.
    - `remove_keys(*keys)`: Removes specified keys from `.env` file and environment.

`AuthResult`
    Result dataclass for authentication operations.
    
    Attributes:
    - `ok` (bool): Success indicator.
    - `message` (str): Human-readable result message.
    - `payload` (dict | None): Raw API response payload.


Class: MarketDataAuth
---------------------
Main authentication service inheriting from `Config` and `LoggerBase`.

Initialization:
- Accepts `reset_session_on_init` (bool) to clear session keys before starting.
- Accepts `timeout` (float) for HTTP client timeout configuration.
- Loads configuration via `Config` and sets up logging via `LoggerBase`.
- Creates persistent `httpx.Client` for connection reuse.

Public methods:

`login() -> AuthResult`
    Authenticate against the XTS Market-Data API.
    
    Steps:
    - Resolves `UNIQUE_KEY` via host-lookup microservice if not cached.
    - Sends POST to login endpoint with credentials and unique key header.
    - Extracts and persists `SECRET_UNIQUE_KEY` and `USER_ID` to `.env`.
    - Returns `AuthResult` with success status and API response payload.
    - Handles HTTP errors, malformed responses, and exceptions gracefully.

`logout() -> AuthResult`
    Invalidate the active Market-Data session.
    
    Steps:
    - Retrieves `SECRET_UNIQUE_KEY` from environment.
    - Sends DELETE to logout endpoint with token header.
    - Returns `AuthResult` with success status and API response.
    - Logs errors if token is missing or request fails.

`reset_session() -> None`
    Clears `UNIQUE_KEY` and `SECRET_UNIQUE_KEY` from `.env` and environment.

`close() -> None`
    Closes the underlying `httpx.Client` connection.

Context manager:
    Supports `with MarketDataAuth() as auth:` for automatic resource cleanup.

Private methods:

`_host_lookup_url() -> str`
    Constructs host-lookup microservice URL from configuration.

`_resolve_unique_key(*, force_refresh=False) -> str`
    Retrieves or generates `UNIQUE_KEY` from host-lookup service.
    - Returns cached key if available and not forcing refresh.
    - Caches result in `.env` for subsequent runs.


Top-level functions
-------------------

`login() -> str`
    Convenience wrapper for programmatic login.
    
    Returns:
    - `"Logged In Successfully"` on success.
    - `"Login Failed"` on any exception (logged with context).

`logout() -> str`
    Convenience wrapper for programmatic logout.
    
    Returns:
    - `"Logged Out Successfully"` on success.
    - `"Logout Failed"` on any exception.


CLI usage
---------
When executed as a script, exposes a minimal CLI using `argparse`:

Usage:
    `python -m auth login [--reset-session]`
    `python -m auth logout [--reset-session]`

Arguments:
- `action`: Required choice of `login` or `logout`.
- `--reset-session`: Optional flag to clear session keys before running.

Returns exit code 0 on success, 1 on failure. Useful for shell scripts and
integration testing without direct module imports.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

import httpx
from dotenv import load_dotenv, set_key

from config import Config
from logger import LoggerBase


_LOG_ORIG: Final[str] = "auth.py"
DEFAULT_SOURCE: Final[str] = "WEB"
ENV_KEYS_TO_CLEAR: Final[tuple[str, ...]] = ("UNIQUE_KEY", "SECRET_UNIQUE_KEY")


@dataclass(frozen=True, slots=True)
class AuthEnv:
    app_key: str
    secret_key: str
    root_url: str
    source: str = DEFAULT_SOURCE
    env_file_path: Path = Path(".env")

    @property
    def login_url(self) -> str:
        return f"{self.root_url}/apimarketdata/auth/login"

    @property
    def logout_url(self) -> str:
        return f"{self.root_url}/apimarketdata/auth/logout"

    @classmethod
    def from_environment(cls, env_file_path: Path | str = ".env") -> "AuthEnv":
        load_dotenv()

        app_key = os.getenv("API_KEY_MARKET_DATA")
        secret_key = os.getenv("API_SECRET_MARKET_DATA")
        root_url = os.getenv("ROOT_URL")
        source = os.getenv("SOURCE") or DEFAULT_SOURCE

        missing = [
            name
            for name, value in (
                ("API_KEY_MARKET_DATA", app_key),
                ("API_SECRET_MARKET_DATA", secret_key),
                ("ROOT_URL", root_url),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        return cls(
            app_key=app_key,
            secret_key=secret_key,
            root_url=root_url.rstrip("/"),
            source=source,
            env_file_path=Path(env_file_path),
        )


@dataclass(slots=True)
class EnvStore:
    env_file_path: Path

    def ensure_file(self) -> None:
        if not self.env_file_path.exists():
            self.env_file_path.touch()

    def get(self, key: str, default: str | None = None) -> str | None:
        return os.getenv(key, default)

    def set(self, key: str, value: str) -> None:
        self.ensure_file()
        set_key(str(self.env_file_path), key, value)
        os.environ[key] = value

    def remove_keys(self, *keys: str) -> None:
        if not self.env_file_path.exists():
            return

        keyset = set(keys)
        lines = self.env_file_path.read_text(encoding="utf-8").splitlines()
        filtered = [
            line
            for line in lines
            if not any(line.strip().startswith(f"{key}=") for key in keyset)
        ]
        content = "\n".join(filtered).rstrip()
        self.env_file_path.write_text(content + ("\n" if content else ""), encoding="utf-8")

        for key in keys:
            os.environ.pop(key, None)


@dataclass(slots=True)
class AuthResult:
    ok: bool
    message: str
    payload: dict | None = None


class MarketDataAuth(Config, LoggerBase):
    """
    Efficient, reusable authentication service for the XTS Market-Data API.
    """

    def __init__(
        self,
        *,
        reset_session_on_init: bool = False,
        timeout: float = 10.0,
    ) -> None:
        Config.__init__(self)
        LoggerBase.__init__(self)

        env_path = Path(self.env_file_path) if hasattr(self, "env_file_path") else Path(".env")
        self.settings = AuthEnv.from_environment(env_path)
        self.env_store = EnvStore(self.settings.env_file_path)

        if reset_session_on_init:
            self.reset_session()

        self._client = httpx.Client(
            timeout=timeout,
            headers={"Content-Type": "application/json"},
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "MarketDataAuth":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def reset_session(self) -> None:
        self.env_store.remove_keys(*ENV_KEYS_TO_CLEAR)

    def _host_lookup_url(self) -> str:
        lookup = self.auth["host_lookup_variables"]
        return f"{self.settings.root_url}:{lookup['port']}/{lookup['url_extension']}"

    def _resolve_unique_key(self, *, force_refresh: bool = False) -> str:
        if not force_refresh:
            cached = self.env_store.get("UNIQUE_KEY")
            if cached:
                return cached

        lookup = self.auth["host_lookup_variables"]
        payload = {
            "accesspassword": lookup["access_password"],
            "version": lookup["version"],
        }

        try:
            response = self._client.post(self._host_lookup_url(), json=payload)
            response.raise_for_status()
            data = response.json()
            unique_key = data["result"]["uniqueKey"]
            self.env_store.set("UNIQUE_KEY", unique_key)
            return unique_key

        except httpx.HTTPError as exc:
            self.error(f"Market Data API: host lookup failed: {exc}")
            raise
        except KeyError as exc:
            self.error(f"Market Data API: malformed host lookup response: missing {exc}")
            raise

    def login(self) -> AuthResult:
        try:
            unique_key = self._resolve_unique_key()

            payload = {
                "secretKey": self.settings.secret_key,
                "appKey": self.settings.app_key,
                "source": self.settings.source,
            }
            headers = {"authorization": unique_key}

            response = self._client.post(
                self.settings.login_url,
                headers=headers,
                json=payload,
            )
            response.raise_for_status()

            data = response.json()
            result = data["result"]
            secret_unique_key = result["token"]
            user_id = str(result["userID"])

            self.env_store.set("SECRET_UNIQUE_KEY", secret_unique_key)
            self.env_store.set("USER_ID", user_id)

            self.info(f"Market Data API: Login successful: {data}")
            return AuthResult(True, "Logged In Successfully", data)

        except httpx.HTTPError as exc:
            self.error(f"Market Data API: Login failed: {exc}")
            return AuthResult(False, "Login Failed")
        except (KeyError, ValueError) as exc:
            self.error(f"Market Data API: Login response invalid: {exc}")
            return AuthResult(False, "Login Failed")
        except Exception as exc:
            self.error(f"Market Data API: Unexpected login error: {exc}")
            return AuthResult(False, "Login Failed")

    def logout(self) -> AuthResult:
        token = self.env_store.get("SECRET_UNIQUE_KEY")
        if not token:
            self.error("Market Data API: No SECRET_UNIQUE_KEY present for logout")
            return AuthResult(False, "Logout Failed")

        try:
            response = self._client.delete(
                self.settings.logout_url,
                headers={"authorization": token},
            )
            response.raise_for_status()
            data = response.json()

            self.info(f"Market Data API: Successfully logged out: {data}")
            return AuthResult(True, "Logged Out Successfully", data)

        except httpx.HTTPError as exc:
            self.error(f"Market Data API: Logout failed: {exc}")
            return AuthResult(False, "Logout Failed")
        except Exception as exc:
            self.error(f"Market Data API: Unexpected logout error: {exc}")
            return AuthResult(False, "Logout Failed")


def login() -> str:
    try:
        with MarketDataAuth() as auth:
            return auth.login().message
    except Exception as exc:
        LoggerBase().error(f"[{_LOG_ORIG}.login] Did not login successfully: {exc}")
        return "Login Failed"


def logout() -> str:
    try:
        with MarketDataAuth() as auth:
            return auth.logout().message
    except Exception as exc:
        LoggerBase().error(f"[{_LOG_ORIG}.logout] Did not logout successfully: {exc}")
        return "Logout Failed"


def main() -> int:
    parser = argparse.ArgumentParser(description="XTS Market-Data API Authentication")
    parser.add_argument(
        "action",
        choices=("login", "logout"),
        help="Action to perform",
    )
    parser.add_argument(
        "--reset-session",
        action="store_true",
        help="Clear UNIQUE_KEY and SECRET_UNIQUE_KEY before running",
    )

    args = parser.parse_args()

    try:
        with MarketDataAuth(reset_session_on_init=args.reset_session) as auth:
            result = auth.login() if args.action == "login" else auth.logout()
            print(result.message)
            return 0 if result.ok else 1
    except Exception as exc:
        LoggerBase().error(f"[{_LOG_ORIG}.main] Fatal error: {exc}")
        print("Login Failed" if args.action == "login" else "Logout Failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())