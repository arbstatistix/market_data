"""
================================================================================
Market Data API Client
================================================================================

This module implements a high-performance client for interacting with the
XTS Market Data API.

Primary responsibilities of this module:

1. Market configuration retrieval
2. Instrument master data ingestion
3. Instrument lookup utilities
4. Market quote retrieval
5. Subscription / unsubscription to market feeds
6. Redis-based configuration caching
7. MongoDB-based master data persistence

Architecture Overview
---------------------

External Systems
----------------
Market Data API (HTTP REST)
        │
        ▼
MarketDataAPI Client
        │
        ├── Redis
        │     Stores lightweight configuration metadata for fast access
        │
        └── MongoDB
              Stores instrument master details for futures, options,
              equities, and indices.

Data Flow
---------
Market API → Parsing Layer → Structured Documents → MongoDB Collections

Performance Design
------------------
This module is optimized for low latency and operational robustness:

• HTTP connection reuse via persistent session
• Batch MongoDB writes for master data
• Redis caching for frequently accessed configuration
• Defensive parsing to handle malformed API responses

Used in
-------
• Market data ingestion pipelines
• Trading strategy infrastructure
• Instrument discovery systems
• Options chain builders

================================================================================
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from requests import Session, Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import redis
import msgpack
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config import Config
from logger import LoggerBase


# ---------------------------------------------------------------------
# Environment bootstrap
# ---------------------------------------------------------------------
dotenv_path = os.path.join(os.path.dirname(__file__), "..", "auth", ".env")
load_dotenv(dotenv_path=dotenv_path)

API_KEY_MARKET_DATA = os.getenv("API_KEY_MARKET_DATA", "")
API_SECRET_MARKET_DATA = os.getenv("API_SECRET_MARKET_DATA", "")
SOURCE = os.getenv("SOURCE", "")
ROOT_MARKET_DATA = os.getenv("ROOT_MARKET_DATA", "")
ROOT_URL = os.getenv("ROOT_URL", "https://ttblaze.iifl.com")
UNIQUE_KEY = os.getenv("UNIQUE_KEY", "")
SECRET_UNIQUE_KEY = os.getenv("SECRET_UNIQUE_KEY", "")

LOGIN_URL_MARKET_API = f"{ROOT_URL}/apimarketdata/auth/login"
LOGOUT_URL_MARKET_API = f"{ROOT_URL}/apimarketdata/auth/logout"

GLOBAL_LOGGER = LoggerBase()


class MarketDataAPI(Config, LoggerBase):
    """
    Optimized market data client.

    Main improvements:
    - persistent HTTP session
    - retry + timeout support
    - compact Redis serialization via msgpack
    - centralized request path
    - stream-like master parsing
    - faster Mongo full-refresh strategy
    """

    REQUEST_TIMEOUT: Tuple[float, float] = (3.0, 20.0)  # connect, read
    REDIS_CFG_KEYS = (
        "exchange_segments",
        "xts_message_code",
        "publish_format",
        "broadcast_mode",
        "instrument_type",
        "index_list",
    )

    def __init__(self) -> None:
        Config.__init__(self)
        LoggerBase.__init__(self)

        self.exchangeSegment: Optional[int] = None
        self.xtsMessageCode: Optional[int] = None
        self.publishFormat: Optional[str] = None
        self.broadCastMode: Optional[str] = None
        self.instrumentType: Optional[str] = None

        # Redis client
        self.redis_client = redis.Redis(
            host=self.REDIS["host"],
            port=self.REDIS["port"],
            db=0,
            decode_responses=False,  # keep bytes, cheaper and explicit
        )

        # Mongo client
        mongo_host = self.mongodb_config["host"]
        mongo_port = self.mongodb_config["port"]
        self.client = MongoClient(
            f"mongodb://{mongo_host}:{mongo_port}/",
            maxPoolSize=50,
            minPoolSize=5,
            connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000,
        )

        db_name = self.mongodb_config["XTS"]["MASTER_DETAIL"]["DB_NAME"]
        coll_names = self.mongodb_config["XTS"]["MASTER_DETAIL"]["COLL_NAME"]

        self.db = self.client[db_name]

        self.collection_fut_idx = self.db[coll_names["FUT_IDX_NAME"]]
        self.collection_opt_idx = self.db[coll_names["OPT_IDX_NAME"]]
        self.collection_fut_stk = self.db[coll_names["FUT_STK_NAME"]]
        self.collection_opt_stk = self.db[coll_names["OPT_STK_NAME"]]
        self.collection_cm = self.db[coll_names["CM_NAME"]]

        self.header = {
            "Content-Type": "application/json",
            "authorization": SECRET_UNIQUE_KEY,
        }

        self.session = self._build_session()

    # -----------------------------------------------------------------
    # Session / request helpers
    # -----------------------------------------------------------------
    def _build_session(self) -> Session:
        session = Session()

        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.3,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PUT"),
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(self.header)
        return session

    def _request(
        self,
        method: str,
        route: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_payload: Optional[Dict[str, Any]] = None,
        expected_status: int = 200,
    ) -> Dict[str, Any]:
        url = f"{ROOT_URL}{route}"

        try:
            response: Response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json_payload,
                timeout=self.REQUEST_TIMEOUT,
            )
        except Exception as e:
            self.critical(f"HTTP request failed [{method} {url}]: {e}")
            return {}

        if response.status_code != expected_status:
            self.error(
                f"HTTP {response.status_code} for [{method} {url}] "
                f"params={params} json={json_payload} body={response.text[:500]}"
            )
            return {}

        try:
            return response.json()
        except Exception as e:
            self.error(f"JSON decode failed for [{method} {url}]: {e}")
            return {}

    # -----------------------------------------------------------------
    # Redis helpers
    # -----------------------------------------------------------------
    @staticmethod
    def _pack(obj: Any) -> bytes:
        return msgpack.packb(obj, use_bin_type=True)

    @staticmethod
    def _unpack(blob: Optional[bytes]) -> Any:
        if blob is None:
            return None
        return msgpack.unpackb(blob, raw=False)

    def _redis_set_many(self, mapping: Dict[str, Any]) -> None:
        pipe = self.redis_client.pipeline(transaction=False)
        for k, v in mapping.items():
            pipe.set(k, self._pack(v))
        pipe.execute()

    def _redis_get_many(self, keys: Iterable[str]) -> Tuple[Any, ...]:
        values = self.redis_client.mget(list(keys))
        return tuple(self._unpack(v) for v in values)

    # -----------------------------------------------------------------
    # Parsing helpers
    # -----------------------------------------------------------------
    @staticmethod
    def _to_float(x: str) -> Optional[float]:
        if x == "" or x is None:
            return None
        try:
            return float(x)
        except ValueError:
            return None

    @staticmethod
    def _parse_cm(meta: List[str]) -> Dict[str, Any]:
        return {
            "ExchangeInstrumentID": meta[1],
            "InstrumentType": meta[2],
            "Name": meta[3],
            "Description": meta[4],
            "Series": meta[5],
            "NameWithSeries": meta[6],
            "InstrumentID": meta[7],
            "PriceBandHigh": meta[8],
            "PriceBandLow": meta[9],
            "FreezeQty": meta[10],
            "TickSize": meta[11],
            "LotSize": meta[12],
            "Multiplier": meta[13],
            "DisplayName": meta[14],
            "ISIN": meta[15],
            "PriceNumerator": meta[16],
            "PriceDenominator": meta[17],
            "DetailedDescription": meta[18],
            "ExtendedSurvIndicator": meta[19],
            "CautionIndicator": meta[20],
            "GSMIndicator": meta[21],
        }

    @classmethod
    def _parse_fut(cls, meta: List[str]) -> Dict[str, Any]:
        return {
            "ExchangeInstrumentID": meta[1],
            "InstrumentType": meta[2],
            "Name": meta[3],
            "Description": meta[4],
            "Series": meta[5],
            "NameWithSeries": meta[6],
            "InstrumentID": meta[7],
            "PriceBandHigh": cls._to_float(meta[8]),
            "PriceBandLow": cls._to_float(meta[9]),
            "FreezeQty": cls._to_float(meta[10]),
            "TickSize": cls._to_float(meta[11]),
            "LotSize": cls._to_float(meta[12]),
            "MultiplierLong": cls._to_float(meta[13]),
            "MultiplierShort": cls._to_float(meta[14]),
            "UnderlyingIndexName": meta[15],
            "ContractExpiration": meta[16],
            "DisplayName": meta[17],
            "PriceNumerator": meta[18],
            "PriceDenominator": meta[19],
            "DetailedDescription": meta[20],
        }

    @classmethod
    def _parse_opt(cls, meta: List[str]) -> Dict[str, Any]:
        return {
            "ExchangeInstrumentID": meta[1],
            "InstrumentType": meta[2],
            "Name": meta[3],
            "Description": meta[4],
            "Series": meta[5],
            "NameWithSeries": meta[6],
            "InstrumentID": meta[7],
            "PriceBandHigh": cls._to_float(meta[8]),
            "PriceBandLow": cls._to_float(meta[9]),
            "FreezeQty": cls._to_float(meta[10]),
            "TickSize": cls._to_float(meta[11]),
            "LotSize": cls._to_float(meta[12]),
            "Multiplier": cls._to_float(meta[13]),
            "UnderlyingInstrumentId": meta[14],
            "UnderlyingIndexName": meta[15],
            "ContractExpiration": meta[16],
            "StrikePrice": meta[17],
            "OptionType": meta[18],
            "DisplayName": meta[19],
            "PriceNumerator": meta[20],
            "PriceDenominator": meta[21],
            "DetailedDescription": meta[22],
        }

    def _iter_master_docs(
        self, lines: Iterable[str]
    ) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """
        Yield (bucket, document) instead of building many huge intermediate lists.
        """
        for line in lines:
            if not line:
                continue

            parts = line.split("|")
            if len(parts) < 6:
                continue

            tag = parts[0]
            if tag == "NSECM":
                yield "cm", self._parse_cm(parts)
                continue

            if tag != "NSEFO":
                continue

            series = parts[5]
            if series == "FUTIDX":
                yield "fut_idx", self._parse_fut(parts)
            elif series == "FUTSTK":
                yield "fut_stk", self._parse_fut(parts)
            elif series == "OPTIDX":
                yield "opt_idx", self._parse_opt(parts)
            elif series == "OPTSTK":
                yield "opt_stk", self._parse_opt(parts)

    @staticmethod
    def _chunked(iterable: Iterable[Dict[str, Any]], size: int) -> Iterator[List[Dict[str, Any]]]:
        batch: List[Dict[str, Any]] = []
        for item in iterable:
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _replace_collection(self, collection: Collection, docs: Iterable[Dict[str, Any]], batch_size: int = 5000) -> int:
        """
        Full refresh semantics:
        drop existing collection contents, then insert in batches.
        """
        collection.drop()
        inserted = 0
        for batch in self._chunked(docs, batch_size):
            collection.insert_many(batch, ordered=False)
            inserted += len(batch)
        return inserted

    # -----------------------------------------------------------------
    # Public methods
    # -----------------------------------------------------------------
    def __master__detail__update__(self) -> List[str]:
        """
        Fetch raw master lines.
        """
        api_route = "/apimarketdata/instruments/master"
        payload = {"exchangeSegmentList": ["NSECM", "NSEFO"]}

        data = self._request("POST", api_route, json_payload=payload)
        result = data.get("result")
        if not isinstance(result, str):
            self.error("Master detail response missing string 'result'")
            return []

        return result.splitlines()

    def __master__detail__data__cleaning__(self) -> None:
        """
        Parse and fully refresh master collections.
        More memory-efficient than building all documents at once.
        """
        try:
            lines = self.__master__detail__update__()
            if not lines:
                self.error("No data returned from `__master__detail__data__cleaning__`")
                return

            buckets: Dict[str, List[Dict[str, Any]]] = {
                "cm": [],
                "fut_idx": [],
                "fut_stk": [],
                "opt_idx": [],
                "opt_stk": [],
            }

            # Still buffered, but much simpler. If dataset becomes huge,
            # route directly to temp files or per-collection chunk inserts.
            for bucket, doc in self._iter_master_docs(lines):
                buckets[bucket].append(doc)

            inserted_fut_idx = self._replace_collection(self.collection_fut_idx, buckets["fut_idx"])
            inserted_fut_stk = self._replace_collection(self.collection_fut_stk, buckets["fut_stk"])
            inserted_opt_idx = self._replace_collection(self.collection_opt_idx, buckets["opt_idx"])
            inserted_opt_stk = self._replace_collection(self.collection_opt_stk, buckets["opt_stk"])
            inserted_cm = self._replace_collection(self.collection_cm, buckets["cm"])

            self.info(f"Inserted FUT IDX docs: {inserted_fut_idx}")
            self.info(f"Inserted FUT STK docs: {inserted_fut_stk}")
            self.info(f"Inserted OPT IDX docs: {inserted_opt_idx}")
            self.info(f"Inserted OPT STK docs: {inserted_opt_stk}")
            self.info(f"Inserted CM docs: {inserted_cm}")

        except Exception as e:
            self.critical(f"Error in `__master__detail__data__cleaning__`: {e}")

    def __client__config__response__(self) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.config"]
        data = self._request("GET", route)
        result = data.get("result")
        if not isinstance(result, dict):
            self.error("Invalid config response shape")
            return {}

        cache_payload = {
            "exchange_segments": result.get("exchangeSegments"),
            "xts_message_code": result.get("xtsMessageCode"),
            "publish_format": result.get("publishFormat"),
            "broadcast_mode": result.get("broadCastMode"),
            "instrument_type": result.get("instrumentType"),
        }
        self._redis_set_many(cache_payload)
        return result

    def __retrieve__config__(self) -> Tuple[Any, ...]:
        try:
            return self._redis_get_many(self.REDIS_CFG_KEYS)
        except Exception as e:
            self.critical(f"Error in `__retrieve__config__`: {e}")
            return (None,) * len(self.REDIS_CFG_KEYS)

    def __index__list__(self, exchange_segment: int = 1) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.instruments.indexlist"]
        return self._request(
            "GET",
            route,
            params={"exchangeSegment": int(exchange_segment)},
        )

    def __get__series__(self, exchange_segment: int) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.instruments.instrument.series"]
        return self._request(
            "GET",
            route,
            params={"exchangeSegment": int(exchange_segment)},
        )

    def __quotes__(
        self,
        exchange_segment: int,
        exchange_instrument_id: int,
        xts_message_code: int,
        publish_format: str,
    ) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.instruments.quotes"]
        payload = {
            "instruments": [
                {
                    "exchangeSegment": int(exchange_segment),
                    "exchangeInstrumentID": int(exchange_instrument_id),
                }
            ],
            "xtsMessageCode": int(xts_message_code),
            "publishFormat": str(publish_format),
        }

        data = self._request("POST", route, json_payload=payload)
        return data.get("result", {})

    def __subscription__(
        self,
        exchange_segment: int,
        exchange_instrument_id: int,
        xts_message_code: int,
    ) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.instruments.subscription"]
        payload = {
            "instruments": [
                {
                    "exchangeSegment": int(exchange_segment),
                    "exchangeInstrumentID": int(exchange_instrument_id),
                }
            ],
            "xtsMessageCode": int(xts_message_code),
        }

        data = self._request("POST", route, json_payload=payload)
        if data:
            self.info(f"Subscribed successfully: {payload['instruments']}")
        else:
            self.error(f"Subscription failed: {payload['instruments']}")
        return data

    def __unsubscription__(
        self,
        exchange_segment: int,
        exchange_instrument_id: int,
        xts_message_code: int,
    ) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.instruments.unsubscription"]
        payload = {
            "instruments": [
                {
                    "exchangeSegment": int(exchange_segment),
                    "exchangeInstrumentID": int(exchange_instrument_id),
                }
            ],
            "xtsMessageCode": int(xts_message_code),
        }

        data = self._request("PUT", route, json_payload=payload)
        if data:
            self.info(f"Unsubscribed successfully: {payload['instruments']}")
        else:
            self.error(f"Unsubscription failed: {payload['instruments']}")
        return data

    def __get__equity__symbol__(
        self,
        exchange_segment: int,
        series: str,
        symbol: str,
    ) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.instruments.instrument.equitysymbol"]
        return self._request(
            "GET",
            route,
            params={
                "exchangeSegment": int(exchange_segment),
                "series": str(series),
                "symbol": str(symbol),
            },
        )

    def __get__expiry__date__(
        self,
        exchange_segment: int,
        series: int,
        symbol: int,
    ) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.instruments.instrument.expirydate"]
        return self._request(
            "GET",
            route,
            params={
                "exchangeSegment": int(exchange_segment),
                "series": int(series),
                "symbol": int(symbol),
            },
        )

    def __get__future__symbol__(
        self,
        exchange_segment: int,
        series: str,
        symbol: str,
        expiry_date: str,
    ) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.instruments.instrument.futuresymbol"]
        return self._request(
            "GET",
            route,
            params={
                "exchangeSegment": int(exchange_segment),
                "series": str(series),
                "symbol": str(symbol),
                "expiryDate": str(expiry_date),
            },
        )

    def __get__option__symbol__(
        self,
        exchange_segment: int,
        series: str,
        symbol: str,
        expiry_date: str,
        option_type: str,
        strike_price: str,
    ) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.instruments.instrument.optionsymbol"]
        return self._request(
            "GET",
            route,
            params={
                "exchangeSegment": int(exchange_segment),
                "series": str(series),
                "symbol": str(symbol),
                "expiryDate": str(expiry_date),
                "optionType": str(option_type),
                "strikePrice": str(strike_price),
            },
        )

    def __get__option__type__(
        self,
        exchange_segment: int,
        series: str,
        symbol: str,
        expiry_date: str,
    ) -> Dict[str, Any]:
        route = self.routes["market_data_api"]["market.instruments.instrument.optiontype"]
        return self._request(
            "GET",
            route,
            params={
                "exchangeSegment": int(exchange_segment),
                "series": str(series),
                "symbol": str(symbol),
                "expiryDate": str(expiry_date),
            },
        )

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        try:
            self.client.close()
        except Exception:
            pass


if __name__ == "__main__":
    api = MarketDataAPI()
    try:
        if len(sys.argv) <= 1:
            GLOBAL_LOGGER.warning(
                "[market_data.xts.market_data.main] Please provide a function name."
            )
            raise SystemExit(1)

        func_name = sys.argv[1]

        if func_name == "__master__detail__data__cleaning__":
            api.__master__detail__data__cleaning__()
        elif func_name == "__client__config__response__":
            api.__client__config__response__()
        elif func_name == "__retrieve__config__":
            print(api.__retrieve__config__())
        elif func_name == "__index__list__ --derv":
            print(api.__index__list__(exchange_segment=2))
        elif func_name == "__index__list__ --spot":
            print(api.__index__list__(exchange_segment=1))
        elif func_name == "__get__series__ --derv":
            print(api.__get__series__(exchange_segment=2))
        elif func_name == "__get__series__ --spot":
            print(api.__get__series__(exchange_segment=1))
        else:
            GLOBAL_LOGGER.warning(
                "[get_mappings] Unknown function name."
            )
    finally:
        api.close()