import os
import sys
import json
import time
import logging
import threading
from datetime import datetime, date, time as dtime
from typing import Dict, Any, List, Optional, Callable, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import redis
import pymongo
from pymongo.database import Database
import socketio
from dotenv import load_dotenv

# ────────────────────────────────────────────────
# Performance & scientific stack
# ────────────────────────────────────────────────
import numpy as np
from numba import njit, float64, int64

# ────────────────────────────────────────────────
# Project imports (assuming structure)
# ────────────────────────────────────────────────
# Adjust paths according to your real layout
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import Config               # type: ignore
from logger import LoggerBase           # type: ignore
from md_api import md_api_func  # type: ignore

# ────────────────────────────────────────────────
# Environment & constants
# ────────────────────────────────────────────────
load_dotenv(os.path.join(parent_dir, "auth", ".env"))

ROOT_URL           = os.getenv("ROOT_URL", "")
SECRET_UNIQUE_KEY  = os.getenv("SECRET_UNIQUE_KEY", "")
USER_ID            = os.getenv("USER_ID", "")

HOLIDAY_DATES = {
    date(2025, 2, 26), date(2025, 3, 14), date(2025, 3, 31), date(2025, 4, 10),
    date(2025, 4, 14), date(2025, 4, 18), date(2025, 5, 1),  date(2025, 8, 15),
    date(2025, 8, 27), date(2025, 10, 2), date(2025, 10, 21), date(2025, 10, 22),
    date(2025, 11, 5), date(2025, 12, 25),
}

HOLIDAY_ORDINALS       = np.array([d.toordinal() for d in HOLIDAY_DATES], dtype=np.int64)
MARKET_OPEN_MINUTES    = 9*60 + 15      # 555
MARKET_CLOSE_MINUTES   = 15*60 + 30     # 930
TRADING_MINUTES_PER_DAY = MARKET_CLOSE_MINUTES - MARKET_OPEN_MINUTES  # 375
TRADING_DAYS_PER_YEAR   = 252.0

DATA_PACKET_SEGMENT_MAP = {
                        1: "market_data", 
                        2: "option_data"
}
