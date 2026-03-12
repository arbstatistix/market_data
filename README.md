# Market Data Module

## Overview

The `market_data` module provides core utilities and authentication functionality for interacting with the **XTS Market-Data API**. It consolidates configuration management, logging infrastructure, and authentication workflows into a unified, easy-to-use package.

## Features

- **Authentication**: CLI entrypoint for `login` and `logout` operations against the XTS Market-Data API
- **Configuration Management**: Centralized, in-code source-of-truth for all runtime settings
- **Logging Infrastructure**: Structured logging with rotating file handlers and UTF-8 support
- **Environment Integration**: Seamless loading of credentials from `.env` files using `python-dotenv`
- **Session Persistence**: Automatic persistence of session identifiers for reuse between runs

## Module Structure

### `auth.py`
Efficient authentication service for the XTS Market-Data API.

**Key Dataclasses:**
- `AuthEnv`: Configuration holder with derived `login_url` and `logout_url` properties. Loads from environment via `from_environment()` classmethod.
- `EnvStore`: Manages `.env` file persistence with `get()`, `set()`, and `remove_keys()` methods.
- `AuthResult`: Result wrapper with `ok` (bool), `message` (str), and optional `payload` (dict) attributes.

**Main Class:** `MarketDataAuth`
- Inherits from `Config` and `LoggerBase`
- Supports context manager: `with MarketDataAuth() as auth:`
- Automatically closes HTTP client on exit

**Public Methods:**
- `login() -> AuthResult` – Authenticates against XTS Market-Data API, persists `SECRET_UNIQUE_KEY` and `USER_ID`
- `logout() -> AuthResult` – Invalidates active session
- `reset_session()` – Clears `UNIQUE_KEY` and `SECRET_UNIQUE_KEY`
- `close()` – Closes underlying `httpx.Client`

**Private Methods:**
- `_host_lookup_url()` – Constructs host-lookup microservice URL
- `_resolve_unique_key(*, force_refresh=False)` – Retrieves or caches `UNIQUE_KEY` from host-lookup service

**Top-level Functions:**
- `login() -> str` – Convenience wrapper, returns success/failure message
- `logout() -> str` – Convenience wrapper, returns success/failure message

**CLI Support:**
```bash
python -m auth login [--reset-session]
python -m auth logout [--reset-session]
```
Returns exit code 0 on success, 1 on failure.

### `config.py`
Single source-of-truth for all runtime configuration.

**Covers:**
- Environment paths and `.env` file locations
- REST route fragments for both Interactive and Market-Data APIs
- Constant dictionaries: order types, products, exchange segments, etc.
- Connection defaults for Redis and MongoDB
- Canned examples and reference data (XTS message codes, month-index maps)

**Usage:**
```python
from config import Config

cfg = Config()
base_url = cfg.market_data_api["url"]
```

### `logger.py`
Shared logging utilities across the codebase.

**Provides:**
- UTF-8 encoding enforcement for stdout/stderr
- `LogErrorType` enum for consistent error categorization
- `LoggerConfig` dataclass for uniform logger setup
- `LoggerBase` wrapper with rotating file and stderr handlers
- `ScopedLogger` context manager for prefixed logging blocks

**Error Types:**
- `FILE` – file I/O failures
- `MONGO` – MongoDB-related errors
- `REDIS` – Redis-related errors
- `MISSING_DATA` – required data not found
- `EMPTY_DATA` – data present but empty
- `KEY_INDEX` – key/index lookup failures
- `VALUE` – invalid value provided
- `TYPE` – type mismatch or invalid type
- `ATTR` – missing or invalid attribute access
- `CONNECTION` – network/connection-level problems

## Environment Variables

The module expects the following environment variables (typically loaded from `.env`):

| Variable | Description |
|----------|-------------|
| `API_KEY_MARKET_DATA` | Public application key for Market-Data API |
| `API_SECRET_MARKET_DATA` | Secret key for Market-Data API |
| `SOURCE` | Source identifier (defaults to `"WEB"`) |
| `ROOT_URL` | Base URL for the XTS gateway (e.g., `https://hostname:port`) |

**Derived URLs:**
- `LOGIN_URL_MARKET_API`: `{ROOT_URL}/apimarketdata/auth/login`
- `LOGOUT_URL_MARKET_API`: `{ROOT_URL}/apimarketdata/auth/logout`

## Session Management

On import, any stale `UNIQUE_KEY` and `SECRET_UNIQUE_KEY` entries are automatically removed from `.env`, ensuring each session starts in a clean state.

Session identifiers are persisted after successful login for reuse between application runs.

## License

MIT License – see [LICENSE](./LICENSE) file for details.

Copyright (c) 2026 arbstatistix
