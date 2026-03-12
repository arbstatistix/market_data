"""
============================

Overview
--------
CLI entrypoint for authenticating against the XTS Market-Data API using
simple `login` and `logout` commands. [web:11]

The module:
- Loads credentials and endpoints from environment variables (via `python-dotenv`). [web:12][web:15]
- Resolves a `uniqueKey` from an internal host-lookup microservice.
- Performs login/logout HTTP calls using `httpx`. [web:11][web:8]
- Persists session identifiers (`UNIQUE_KEY`, `SECRET_UNIQUE_KEY`, `USER_ID`) back into the `.env` file for reuse between runs. [web:9][web:6]

On import, any stale `UNIQUE_KEY` / `SECRET_UNIQUE_KEY` entries are removed
from `.env` so that each session starts in a clean state. [web:6]


Environment & constants
-----------------------
Environment variables expected:
- `API_KEY_MARKET_DATA`: Public application key.
- `API_SECRET_MARKET_DATA`: Secret key for the Market-Data API.
- `SOURCE`: Source identifier (currently unused, payload defaults to `"WEB"`).
- `ROOT_URL`: Base URL for the XTS gateway (e.g. `https://hostname:port`). [web:11]

Derived URLs:
- `LOGIN_URL_MARKET_API`: `{ROOT_URL}/apimarketdata/auth/login`
- `LOGOUT_URL_MARKET_API`: `{ROOT_URL}/apimarketdata/auth/logout`


Class: MarketDataAuth
---------------------
`MarketDataAuth` combines configuration (`Config`) and logging (`LoggerBase`)
to handle the full login lifecycle.

Initialisation:
- Calls `Config.__init__` to load configuration (including `.env` path).
- Calls `LoggerBase.__init__` to attach logging helpers.

Private methods:

`__host__lookup__(self)`
    Resolve or obtain the `UNIQUE_KEY` required for authentication.

    Steps:
    - Read `access_password`, `version`, `port`, `url_extension` from
      `self.auth['host_lookup_variables']`.
    - Build the host-lookup URL as `f"{ROOT_URL}:{port}/{extended_url}"`.
    - POST `{"accesspassword": ..., "version": ...}` using `httpx.post`. [web:11]
    - If `UNIQUE_KEY` is already in the environment, reuse it; otherwise,
      extract `result.uniqueKey` from the response JSON and write it to
      the `.env` file via `set_key`. [web:9][web:6]
    - Log HTTP errors or unexpected exceptions via `GLOBAL_LOGGER`. [web:13]

`__login__(self)`
    Perform Market-Data login and persist the session token.

    Behaviour:
    - Ensures `UNIQUE_KEY` is available, calling `__host__lookup__` if needed.
    - Sends a POST to `LOGIN_URL_MARKET_API` with JSON payload:
      `{"secretKey": SECRET_KEY, "appKey": APP_KEY, "source": "WEB"}`.
    - Adds headers:
      `{"Content-Type": "application/json", "authorization": UNIQUE_KEY}`.
    - On success, extracts `result.token` (stored as `SECRET_UNIQUE_KEY`)
      and `result.userID`, then writes both to the `.env` file. [web:9][web:6]
    - Logs a success message including the raw response payload.
    - Logs HTTP or generic exceptions without raising them further. [web:7][web:13]

`__logout__(self)`
    Invalidate the active Market-Data session.

    Behaviour:
    - Reads `SECRET_UNIQUE_KEY` from the environment.
    - Sends an HTTP DELETE to `LOGOUT_URL_MARKET_API` with header:
      `{"Content-Type": "application/json", "authorization": SECRET_UNIQUE_KEY}`.
    - Logs success with the returned JSON, or errors on failure. [web:11][web:7]


Top-level helpers
-----------------
`login() -> str`
    - Instantiates `MarketDataAuth`.
    - Calls `__login__`.
    - Returns `"Logged In Successfully"` on success, `"Login Failed"`
      on any exception (also logged with `_LOG_ORIG` context).

`logout() -> str`
    - Instantiates `MarketDataAuth`.
    - Calls `__logout__`.
    - Returns `"Logged Out Successfully"` on success, `"Logout Failed"`
      on any exception.


CLI usage
---------
When executed as a script, this module exposes a minimal CLI around the
login/logout helpers using `argparse`:

Usage:
    `python -m auth login`
    `python -m auth logout` [web:3][web:1]

The `action` argument accepts:
- `login`  – attempts authentication and prints the result message.
- `logout` – attempts logout and prints the result message.

This makes it easy to test connectivity or integrate into shell scripts
without importing the package directly.
"""



import sys
import argparse
from pathlib import Path
import httpx
import os
from dotenv import load_dotenv, set_key
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))
from config import Config
from logger import LoggerBase
GLOBAL_LOGGER = LoggerBase()

# Load environment variables
load_dotenv()
APP_KEY = os.getenv("API_KEY_MARKET_DATA")
SECRET_KEY = os.getenv("API_SECRET_MARKET_DATA")
SOURCE = os.getenv("SOURCE")
ROOT_URL = os.getenv("ROOT_URL")
LOGIN_URL_MARKET_API = f"{ROOT_URL}/apimarketdata/auth/login"
LOGOUT_URL_MARKET_API = f"{ROOT_URL}/apimarketdata/auth/logout"

UNIQUE_KEY = None
_LOG_ORIG = "auth.py"

# Clean specific keys from .env file to reset session
env_path = Path(".env")
if env_path.exists():
    lines = env_path.read_text().splitlines()
    cleaned_lines = [line for line in lines if not line.strip().startswith(("UNIQUE_KEY", "SECRET_UNIQUE_KEY"))]
    env_path.write_text("\n".join(cleaned_lines) + "\n")

class MarketDataAuth(Config, LoggerBase):
    def __init__(self):
        Config.__init__(self)
        LoggerBase.__init__(self)

    def __host__lookup__(self):
        global UNIQUE_KEY
        access_password = self.auth['host_lookup_variables']['access_password']
        version = self.auth['host_lookup_variables']['version']
        port = self.auth['host_lookup_variables']['port']
        extended_url = self.auth['host_lookup_variables']['url_extension']
        payload = {
            "accesspassword": access_password,
            "version": version,
        }
        try:
            HOST_LOOKUP_URL = f"{ROOT_URL}:{port}/{extended_url}"
            response = httpx.post(url=HOST_LOOKUP_URL, json=payload)
            response.raise_for_status()
            if os.getenv("UNIQUE_KEY"):
                UNIQUE_KEY = os.getenv("UNIQUE_KEY")
            else:
                response_json = response.json()
                UNIQUE_KEY = response_json['result']['uniqueKey']
                if not Path(self.env_file_path).exists():
                    Path(self.env_file_path).touch()
                set_key(self.env_file_path, "UNIQUE_KEY", UNIQUE_KEY)
        except httpx.HTTPStatusError as http_err:
            GLOBAL_LOGGER.error(f"Market Data API: HTTP error in getting the `uniqueKey`: {http_err}")
        except Exception as e:
            GLOBAL_LOGGER.error(f"Market Data API: Error in `__host__lookup__`: {e}")

    def __login__(self):
        if not UNIQUE_KEY:
            self.__host__lookup__()
        payload = {
            "secretKey": SECRET_KEY,
            "appKey": APP_KEY,
            "source": "WEB"
        }
        headers = {
            "Content-Type": "application/json",
            "authorization": UNIQUE_KEY
        }
        try:
            response = httpx.post(url=LOGIN_URL_MARKET_API, 
                                  headers=headers, 
                                  json=payload)
            response.raise_for_status()
            response_json = response.json()
            global SECRET_UNIQUE_KEY, USER_ID
            SECRET_UNIQUE_KEY = response_json['result']['token']
            USER_ID = response_json['result']['userID']
            env_path = Path(self.env_file_path)
            if not env_path.exists():
                env_path.touch()
            set_key(self.env_file_path, 
                    "SECRET_UNIQUE_KEY", 
                    SECRET_UNIQUE_KEY)
            set_key(self.env_file_path, 
                    "USER_ID", 
                    USER_ID)
            GLOBAL_LOGGER.info(f"Market Data API: Login Successful: {response_json}")
        except httpx.HTTPStatusError as http_err:
            GLOBAL_LOGGER.error(f"Market Data API: Login failed: {http_err}")
        except Exception as e:
            GLOBAL_LOGGER.error(f"Market Data API: Error in `__login__`: {e}")

    def __logout__(self):
        headers = {
            "Content-Type": "application/json",
            "authorization": os.getenv("SECRET_UNIQUE_KEY")
        }
        try:
            response = httpx.delete(LOGOUT_URL_MARKET_API, 
                                    headers=headers)
            response.raise_for_status()
            GLOBAL_LOGGER.info(f"Market Data API: Successfully Logged out: {response.json()}")
        except httpx.HTTPStatusError as http_err:
            GLOBAL_LOGGER.error(f"Market Data API: Unsuccessfully Logged Out: {http_err}")
        except Exception as e:
            GLOBAL_LOGGER.error(f"Market Data API: Error in `__logout__`: {e}")

def login():
    try:
        inst = MarketDataAuth()
        inst.__login__()
        return "Logged In Successfully"
    except Exception as e:
        GLOBAL_LOGGER.error(f"[{_LOG_ORIG}.login] Did Not Login Successfully {e}")
        return "Login Failed"

def logout():
    try:
        inst = MarketDataAuth()
        inst.__logout__()
        return "Logged Out Successfully"
    except Exception as e:
        GLOBAL_LOGGER.error(f"[{_LOG_ORIG}.logout] Did Not Logout Successfully {e}")
        return "Logout Failed"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="XTS Market-Data API Authentication")
    parser.add_argument("action", choices=["login", "logout"], help="Action to perform: login or logout")
    args = parser.parse_args()

    if args.action == "login":
        print(login())
    elif args.action == "logout":
        print(logout())