#!/usr/bin/env python3
"""
XTS Market Data API Client – cleaned & modernized version
"""

import os
import sys
import json
from typing import Dict, Any, List, Optional, Union
from pathlib import Path

import requests
import redis
from pymongo import MongoClient
from pymongo.database import Database
from dotenv import load_dotenv

# Project imports (adjust paths as needed)
CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
sys.path.append(str(PARENT_DIR))

from config import Config                          # type: ignore
from logger import LoggerBase                      # type: ignore


# ────────────────────────────────────────────────
# Environment setup
# ────────────────────────────────────────────────

ENV_PATH = PARENT_DIR / "auth" / ".env"
if not load_dotenv(ENV_PATH):
    print(f"Warning: Could not load .env from {ENV_PATH}")

ROOT_URL            = os.getenv("ROOT_URL", "")
SECRET_UNIQUE_KEY   = os.getenv("SECRET_UNIQUE_KEY", "")

if not ROOT_URL or not SECRET_UNIQUE_KEY:
    raise ValueError("ROOT_URL and SECRET_UNIQUE_KEY must be set in environment variables")

# ────────────────────────────────────────────────
# Constants / defaults
# ────────────────────────────────────────────────

DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "authorization": SECRET_UNIQUE_KEY
}

EXCHANGE_SEGMENTS = {
    1: "NSECM",
    2: "NSEFO",
    # add more if needed
}


class md_api_func(Config, LoggerBase):
    """Client for XTS Market Data HTTP API + master data management"""

    def __init__(self):
        super().__init__()

        self.headers = DEFAULT_HEADERS.copy()

        # Redis
        self.redis = redis.Redis(
            host=self.REDIS["host"],
            port=self.REDIS["port"],
            db=0,
            decode_responses=True
        )

        # MongoDB
        mongo_uri = f"mongodb://{self.mongodb_config['host']}:{self.mongodb_config['port']}/"
        self.mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db: Database = self.mongo_client[self.mongodb_config["XTS"]["MASTER_DETAIL"]["DB_NAME"]]

        # Collection shortcuts
        coll_names = self.mongodb_config["XTS"]["MASTER_DETAIL"]["COLL_NAME"]
        self.col_fut_idx = self.db[coll_names["FUT_IDX_NAME"]]
        self.col_opt_idx = self.db[coll_names["OPT_IDX_NAME"]]
        self.col_fut_stk = self.db[coll_names["FUT_STK_NAME"]]
        self.col_opt_stk = self.db[coll_names["OPT_STK_NAME"]]
        self.col_cm     = self.db[coll_names["CM_NAME"]]

        # Cache frequently used config in instance (optional)
        self._config_cache: Dict[str, Any] = {}

    # ────────────────────────────────────────────────
    # Low-level HTTP helpers
    # ────────────────────────────────────────────────

    def _get(self, path_key: str, params: Optional[Dict] = None) -> Dict:
        url = f"{ROOT_URL}{self.routes['market_data_api'][path_key]}"
        try:
            r = requests.get(url, headers=self.headers, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            self.error(f"GET {path_key} failed → {e}")
            return {}

    def _post(self, path_key: str, json_data: Dict) -> Dict:
        url = f"{ROOT_URL}{self.routes['market_data_api'][path_key]}"
        try:
            r = requests.post(url, headers=self.headers, json=json_data, timeout=10)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            self.error(f"POST {path_key} failed → {e}")
            return {}

    # ────────────────────────────────────────────────
    # Master data pipeline
    # ────────────────────────────────────────────────

    def update_master_data(self) -> bool:
        """Fetch → clean → store full master data (CM + F&O)"""
        raw_lines = self._fetch_master_raw()
        if not raw_lines:
            return False

        docs = self._parse_master_lines(raw_lines)
        self._clear_and_insert_master_docs(docs)
        self.info("Master data update completed")
        return True

    def _fetch_master_raw(self) -> List[str]:
        payload = {"exchangeSegmentList": ["NSECM", "NSEFO"]}
        resp = self._post("market.instruments.master", payload)
        result = resp.get("result", "")
        return [line.strip() for line in result.split("\n") if line.strip()]

    def _parse_master_lines(self, lines: List[str]) -> Dict[str, List[Dict]]:
        """Group parsed documents by collection type"""
        docs: Dict[str, List[Dict]] = {
            "CM_DETAILS": [],
            "FUT_IDX_DETAILS": [],
            "FUT_STK_DETAILS": [],
            "OPT_IDX_DETAILS": [],
            "OPT_STK_DETAILS": [],
        }

        for line in lines:
            parts = line.split("|")
            if len(parts) < 5:
                continue

            tag, instr_type = parts[0], parts[2]

            if tag == "NSECM":
                docs["CM_DETAILS"].append(self._parse_cm(parts))
            elif tag == "NSEFO":
                series = parts[5]
                if series == "FUTIDX":
                    docs["FUT_IDX_DETAILS"].append(self._parse_futures(parts, long_short=True))
                elif series == "FUTSTK":
                    docs["FUT_STK_DETAILS"].append(self._parse_futures(parts, long_short=True))
                elif series in ("OPTIDX", "OPTSTK"):
                    docs[f"{series}_DETAILS"].append(self._parse_option(parts))

        return docs

    @staticmethod
    def _parse_cm(meta: List[str]) -> Dict:
        return {
            "ExchangeInstrumentID": meta[1],
            "InstrumentType": meta[2],
            "Name": meta[3],
            "Description": meta[4],
            "Series": meta[5],
            "ISIN": meta[15],
            # ... add other fields you actually use
        }

    @staticmethod
    def _parse_futures(meta: List[str], long_short: bool = False) -> Dict:
        doc = {
            "ExchangeInstrumentID": meta[1],
            "InstrumentType": meta[2],
            "Name": meta[3],
            "Description": meta[4],
            "Series": meta[5],
            "PriceBand": {"High": float(meta[8]), "Low": float(meta[9])},
            "FreezeQty": float(meta[10]),
            "TickSize": float(meta[11]),
            "LotSize": float(meta[12]),
            "ContractExpiration": meta[16],
            "UnderlyingIndexName": meta[15],
        }
        if long_short:
            doc["MultiplierLong"] = float(meta[13])
            doc["MultiplierShort"] = float(meta[14])
        else:
            doc["Multiplier"] = float(meta[13])
        return doc

    @staticmethod
    def _parse_option(meta: List[str]) -> Dict:
        doc = md_api_func._parse_futures(meta)  # common fields
        doc.update({
            "StrikePrice": float(meta[17]),
            "OptionType": meta[18],
            "Multiplier": float(meta[13]),
            "UnderlyingInstrumentId": meta[14],
        })
        return doc

    def _clear_and_insert_master_docs(self, grouped_docs: Dict[str, List[Dict]]):
        for coll_name, documents in grouped_docs.items():
            if not documents:
                continue
            coll = self.db[coll_name]
            coll.delete_many({})
            try:
                coll.insert_many(documents, ordered=False)
                self.info(f"Inserted {len(documents):,} documents into {coll_name}")
            except Exception as e:
                self.error(f"Failed to insert into {coll_name}: {e}")

    # ────────────────────────────────────────────────
    # Config / reference data
    # ────────────────────────────────────────────────

    def fetch_and_cache_client_config(self) -> bool:
        """Fetch client config and store in Redis"""
        data = self._get("market.config")
        result = data.get("result", {})

        if not result:
            return False

        keys = [
            "exchangeSegments",
            "xtsMessageCode",
            "publishFormat",
            "broadCastMode",
            "instrumentType",
        ]

        for k in keys:
            if val := result.get(k):
                self.redis.set(k, json.dumps(val))

        self.info("Client config cached in Redis")
        return True

    def get_cached_config(self, key: str) -> Optional[Any]:
        """Retrieve parsed config value from Redis"""
        raw = self.redis.get(key)
        return json.loads(raw) if raw else None

    # ────────────────────────────────────────────────
    # Subscription / Quotes
    # ────────────────────────────────────────────────

    def subscribe(self,
                  exchange_segment: int,
                  instrument_id: int,
                  xts_message_code: int = 1501) -> bool:
        payload = {
            "instruments": [{
                "exchangeSegment": int(exchange_segment),
                "exchangeInstrumentID": int(instrument_id)
            }],
            "xtsMessageCode": int(xts_message_code)
        }
        resp = self._post("market.instruments.subscription", payload)
        success = bool(resp.get("result"))
        self.info(f"Subscribe {instrument_id} → {'OK' if success else 'FAILED'}")
        return success

    def unsubscribe(self,
                    exchange_segment: int,
                    instrument_id: int,
                    xts_message_code: int = 1501) -> bool:
        payload = {
            "instruments": [{
                "exchangeSegment": int(exchange_segment),
                "exchangeInstrumentID": int(instrument_id)
            }],
            "xtsMessageCode": int(xts_message_code)
        }
        # Note: most APIs use DELETE or POST – check your provider docs
        # Here assuming POST (common mistake in original was PUT)
        resp = self._post("market.instruments.unsubscription", payload)
        success = bool(resp.get("result"))
        self.info(f"Unsubscribe {instrument_id} → {'OK' if success else 'FAILED'}")
        return success

    def get_quote(self,
                  exchange_segment: int,
                  instrument_id: int,
                  xts_message_code: int = 1501,
                  publish_format: str = "JSON") -> Dict:
        payload = {
            "instruments": [{
                "exchangeSegment": int(exchange_segment),
                "exchangeInstrumentID": int(instrument_id)
            }],
            "xtsMessageCode": int(xts_message_code),
            "publishFormat": publish_format
        }
        return self._post("market.instruments.quotes", payload)


# ────────────────────────────────────────────────
# CLI interface (for manual testing / cron)
# ────────────────────────────────────────────────

def main():
    api = md_api_func()

    if len(sys.argv) < 2:
        print("Usage: python market_data_api.py <command>")
        print("  commands: master-update, config-fetch, subscribe <seg> <id>")
        return

    cmd = sys.argv[1]

    if cmd == "master-update":
        api.update_master_data()
    elif cmd == "config-fetch":
        api.fetch_and_cache_client_config()
    elif cmd == "subscribe" and len(sys.argv) == 4:
        seg, iid = int(sys.argv[2]), int(sys.argv[3])
        api.subscribe(seg, iid)
    else:
        print(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()