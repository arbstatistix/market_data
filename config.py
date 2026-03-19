"""
=======

Overview
--------
``Config`` is the single, in-code source-of-truth for all runtime configuration.
It consolidates what would otherwise be scattered across YAML/JSON files into one
importable object, keeping every module's dependency surface minimal:

    from config.main import Config

    cfg      = Config()
    base_url = cfg.market_data_api["url"]

Config covers:
- Environment paths and ``.env`` file locations
- REST route fragments for both the Interactive and Market-Data APIs
- Constant dictionaries: order types, products, exchange segments, etc.
- Connection defaults for Redis and MongoDB
- Canned examples: subscribe payload, XTS message codes, month-index map


Sections
--------
auth.host_lookup_variables
    Connection details for the in-house service-discovery microservice
    (version, port, etc.).

market_data_api
    Base URL and path to the ``.env`` file holding API keys.

products
    Enums mirrored from XTS Interactive API documentation
    (order types, transaction types, etc.).

routes
    Relative paths for every REST call used in the codebase, grouped by
    logical bundle: ``orders``, ``portfolio_handling``, ``market_data_api``, etc.

xts_master_detail_collection_info
    Database and collection names for the master-data MongoDB instance
    shipped with XTS.

xts_message_codes/XTS_RESPONSE_CODE_MAP
    Socket/event codes of interest.

subscribe_payload
    Example batch payload used in quick integration tests.

redis_config / mongodb_config
    Local connectivity defaults.

month_idx
    Convenience map: ``1 → 'Jan'``, ``2 → 'Feb'``, …

queue_config
    Single tuning knob for internal in-memory queues.


Helper: __find__minimum__strike__difference__
---------------------------------------------
Resolves the minimum strike interval for a given option symbol — useful for
delta-neutral strategy generators.

Args:
    name (str):              Index or stock option name (e.g. ``"NIFTY"``).
    exchange_segment (int):  Exchange segment identifier.
    asset_type (int):        Asset type identifier.

Returns:
    int | float:
        - Minimum non-zero gap between successive strike prices on success.
        - ``0``  if fewer than 2 strikes are found (spacing not calculable).
        - ``-1`` on any database error.

Algorithm:
    1. Connects to MongoDB (``XTS_DB.OPT_*_DETAILS``).
    2. Fetches all unique strike prices for the symbol.
    3. Returns the smallest non-zero difference between consecutive strikes.

Example::

    cfg  = Config()
    step = cfg.__find__minimum__strike__difference__(
               name="NIFTY", exchange_segment=2, asset_type=1)
    print(step)


Design Notes
------------
- Most attributes are intentionally public to keep them JSON-serialisable,
  which makes them easy to inspect in debug UIs or notebooks.
- If configuration is ever externalised (YAML/ENV), this class can be
  refactored into a loader rather than a container.
"""


from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


@dataclass(frozen=True, slots=True)
class MongoSettings:
    host: str="localhost"
    port: int=27017

    @property
    def uri(self)->str:
        return f"mongodb://{self.host}:{self.port}"
    
@dataclass(frozen=True, slots=True)
class RedisSettings:
    host: str="localhost"
    port: int=6379

@dataclass(slots=True)
class Config:
    """
    High-performance configuration container.

    Design goals:
    - Constants live at class level, so they are allocated once.
    - Instance state is minimal.
    - MongoDB client is lazily initialized and reused.
    - Strike-step computation is streaming and O(1) auxiliary memory.
    """

    base_dir: Path=field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    env_file_path: Path=field(init=False)
    mongo: MongoSettings=field(default_factory=MongoSettings)
    redis: RedisSettings=field(default_factory=RedisSettings)

    _mongo_client: MongoClient | None=field(default=None,
                                            init=False,
                                            repr=False)
    AUTH_DETAILS: ClassVar[dict[str, Any]] = {
        "host_lookup_variables": {
            "access_password": "2021HostLookUpAccess",
            "version": "interactive_1.0.1",
            "port": "4000",
            "url_extension": "HostLookUp",
        }
    }

    REDIS: ClassVar[dict[str,Any]] = {
        "host": "localhost",
        "port": "6379",
    }

    MARKET_DATA_API: ClassVar[dict[str, Any]] = {
        "url": "https://eapi.emkayglobal.com",
        "env_file_path": ".env",
    }

    PRODUCT_CONSTANTS: ClassVar[dict[str, dict[str, str]]] = {
        "products": {
            "PRODUCT_MIS": "MIS",
            "PRODUCT_NRML": "NRML",
        },
        "order_types": {
            "ORDER_TYPE_MARKET": "MARKET",
            "ORDER_TYPE_LIMIT": "LIMIT",
            "ORDER_TYPE_STOPMARKET": "STOPMARKET",
            "ORDER_TYPE_STOPLIMIT": "STOPLIMIT",
        },
        "transaction_types": {
            "TRANSACTION_TYPE_BUY": "BUY",
            "TRANSACTION_TYPE_SELL": "SELL",
        },
        "squareoff_modes": {
            "SQUAREOFF_DAYWISE": "DayWise",
            "SQUAREOFF_NETWISE": "Netwise",
        },
        "squareoff_quantity_types": {
            "SQUAREOFFQUANTITY_EXACTQUANTITY": "ExactQty",
            "SQUAREOFFQUANTITY_PERCENTAGE": "Percentage",
        },
        "validity_types": {
            "VALIDITY_DAY": "DAY",
        },
        "exchange_segments": {
            "EXCHANGE_NSECM": "NSECM",
            "EXCHANGE_NSEFO": "NSEFO",
            "EXCHANGE_NSECD": "NSECD",
            "EXCHANGE_MCXFO": "MCXFO",
            "EXCHANGE_BSECM": "BSECM",
        },
}

    ROUTES: ClassVar[dict[str, dict[str, str]]] = {
        "interactive_api": {
            "interactive.prefix": "interactive",
            "user.login": "/interactive/user/session",
            "user.logout": "/interactive/user/session",
            "user.profile": "/interactive/user/profile",
            "user.balance": "/interactive/user/balance",
        },
        "orders": {
            "orders": "/interactive/orders",
            "trades": "/interactive/orders/trades",
            "order.status": "/interactive/orders",
            "order.place": "/interactive/orders",
        },
        "order_handling": {
            "bracketorder.place": "/interactive/orders/bracket",
            "bracketorder.modify": "/interactive/orders/bracket",
            "bracketorder.cancel": "/interactive/orders/bracket",
            "order.place.cover": "/interactive/orders/cover",
            "order.exit.cover": "/interactive/orders/cover",
            "order.modify": "/interactive/orders",
            "order.cancel": "/interactive/orders",
            "order.cancelall": "/interactive/orders/cancelall",
            "order.history": "/interactive/orders",
        },
        "portfolio_handling": {
            "portfolio.positions": "/interactive/portfolio/positions",
            "portfolio.holdings": "/interactive/portfolio/holdings",
            "portfolio.positions.convert": "/interactive/portfolio/positions/convert",
            "portfolio.squareoff": "/interactive/portfolio/squareoff",
            "portfolio.dealerpositions": "/interactive/portfolio/dealerpositions",
            "order.dealer.status": "/interactive/orders/dealerorderbook",
            "dealer.trades": "/interactive/orders/dealertradebook",
        },
        "market_data_api": {
            "marketdata.prefix": "apimarketdata",
            "market.login": "/apimarketdata/auth/login",
            "market.logout": "/apimarketdata/auth/logout",
            "market.config": "/apimarketdata/config/clientConfig",
            "market.instruments.master": "/apimarketdata/instruments/master",
            "market.instruments.subscription": "/apimarketdata/instruments/subscription",
            "market.instruments.unsubscription": "/apimarketdata/instruments/subscription",
            "market.instruments.ohlc": "/apimarketdata/instruments/ohlc",
            "market.instruments.indexlist": "/apimarketdata/instruments/indexlist",
            "market.instruments.quotes": "/apimarketdata/instruments/quotes",
            "market.search.instrumentsbyid": "/apimarketdata/search/instrumentsbyid",
            "market.search.instrumentsbystring": "/apimarketdata/search/instruments",
            "market.instruments.instrument.series": "/apimarketdata/instruments/instrument/series",
            "market.instruments.instrument.equitysymbol": "/apimarketdata/instruments/instrument/symbol",
            "market.instruments.instrument.futuresymbol": "/apimarketdata/instruments/instrument/futureSymbol",
            "market.instruments.instrument.optionsymbol": "/apimarketdata/instruments/instrument/optionsymbol",
            "market.instruments.instrument.optiontype": "/apimarketdata/instruments/instrument/optionType",
            "market.instruments.instrument.expirydate": "/apimarketdata/instruments/instrument/expiryDate",
        },
    }

    XTS_MASTER_DETAIL_COLLECTION_INFO: ClassVar[dict[str, Any]] = {
        "DB_NAME": "XTS_DB_MASTER_DETAIL",
        "COLL_NAME": {
            "FUT_IDX": "FUT_IDX_DETAILS",
            "FUT_STK": "FUT_STK_DETAILS",
            "OPT_IDX": "OPT_IDX_DETAILS",
            "OPT_STK": "OPT_STK_DETAILS",
        },
        "uri": "localhost",
        "port": "27017",
    }

    XTS_RESPONSE_CODE_MAP: ClassVar[tuple[int, ...]] = (
        1501,
        1502,
        1505,
        1510,
        1512,
        1105,
    )

    MARKET_SUBSCRIPTION_PAYLOAD: ClassVar[tuple[dict[str, int], ...]] = (
        {"exchangeSegment": 2, "exchangeInstrumentID": 59144},
        {"exchangeSegment": 2, "exchangeInstrumentID": 45308},
        {"exchangeSegment": 2, "exchangeInstrumentID": 44818},
        {"exchangeSegment": 2, "exchangeInstrumentID": 43118},
        {"exchangeSegment": 2, "exchangeInstrumentID": 48515},
    )

    MONGODB_CONFIG: ClassVar[dict[str, Any]] = {
        "XTS": {
            "MASTER_DETAIL": {
                "COLL_NAME": {
                    "FUT_IDX_NAME": "FUT_IDX_DETAILS",
                    "OPT_IDX_NAME": "OPT_IDX_DETAILS",
                    "FUT_STK_NAME": "FUT_STK_DETAILS",
                    "OPT_STK_NAME": "OPT_STK_DETAILS",
                    "CM_NAME": "CM_DETAILS",
                },
                "DB_NAME": "XTS_DB_MASTER_DETAIL",
            }
        },
        "host": "localhost",
        "port": 27017,
    }

    MONTHS_IDX: ClassVar[tuple[str, ...]] = (
        "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    )

    QUEUE_SIZE_CONFIG: ClassVar[dict[str, int]] = {
        "max_size": 10_000,
    }

    def __post_init__(self) -> None:
        self.env_file_path = self.base_dir / ".env"

    @property
    def auth_details(self) -> dict[str, Any]:
        return self.AUTH_DETAILS

    @property
    def market_data_api(self) -> dict[str, Any]:
        return self.MARKET_DATA_API

    @property
    def products_constants(self) -> dict[str, dict[str, str]]:
        return self.PRODUCT_CONSTANTS

    @property
    def routes(self) -> dict[str, dict[str, str]]:
        return self.ROUTES

    @property
    def xts_master_detail_collection_info(self) -> dict[str, Any]:
        return self.XTS_MASTER_DETAIL_COLLECTION_INFO

    @property
    def xts_response_code_map(self) -> tuple[int, ...]:
        return self.XTS_RESPONSE_CODE_MAP

    @property
    def market_subscription_payload(self) -> tuple[dict[str, int], ...]:
        return self.MARKET_SUBSCRIPTION_PAYLOAD

    @property
    def mongodb_config(self) -> dict[str, Any]:
        return self.MONGODB_CONFIG

    @property
    def months_idx(self) -> tuple[str, ...]:
        return self.MONTHS_IDX

    @property
    def queue_size_config(self) -> dict[str, int]:
        return self.QUEUE_SIZE_CONFIG

    @property
    def mongo_client(self) -> MongoClient:
        if self._mongo_client is None:
            self._mongo_client = MongoClient(
                self.mongo.uri,
                maxPoolSize=32,
                minPoolSize=1,
                serverSelectionTimeoutMS=3000,
                connectTimeoutMS=3000,
                socketTimeoutMS=3000,
                retryWrites=True,
            )
        return self._mongo_client

    def close(self) -> None:
        if self._mongo_client is not None:
            self._mongo_client.close()
            self._mongo_client = None

    def _xts_db(self) -> Database:
        return self.mongo_client["XTS_DB"]

    @staticmethod
    def _resolve_option_collection_name(exchange_segment: int, asset_type: int) -> str | None:
        if exchange_segment != 2:
            return None
        if asset_type == 0:
            return "OPT_STK_DETAILS"
        if asset_type == 1:
            return "OPT_IDX_DETAILS"
        return None

    def _resolve_option_collection(self, exchange_segment: int, asset_type: int) -> Collection | None:
        coll_name = self._resolve_option_collection_name(exchange_segment, asset_type)
        if coll_name is None:
            return None
        return self._xts_db()[coll_name]

    def find_minimum_strike_difference(
        self,
        name: str,
        exchange_segment: int,
        asset_type: int,
    ) -> float:
        """
        Returns:
            - smallest positive strike gap as float
            - 0.0 if fewer than two unique strikes exist
            - -1.0 on database/query failure
        """
        collection = self._resolve_option_collection(exchange_segment, asset_type)
        if collection is None:
            return 0.

        try:
            cursor = collection.aggregate(
                [
                    {"$match": {"Name": name.upper()}},
                    {"$project": {"_id": 0, "StrikePrice": {"$toDouble": "$StrikePrice"}}},
                    {"$group": {"_id": "$StrikePrice"}},
                    {"$sort": {"_id": 1}},
                ],
                allowDiskUse=False,
            )

            prev: float | None = None
            min_diff = float("inf")

            for doc in cursor:
                current = doc["_id"]
                if prev is not None:
                    diff = current - prev
                    if 0.0 < diff < min_diff:
                        min_diff = diff
                prev = current

            return 0.0 if min_diff == float("inf") else min_diff

        except Exception:
            return -1.0

    
