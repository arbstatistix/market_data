import numpy as np
from numba import njit, float64, int64
from datetime import datetime, date, time as dtime


HOLIDAY_DATES = {
    date(2025, 2, 26), date(2025, 3, 14), date(2025, 3, 31), date(2025, 4, 10),
    date(2025, 4, 14), date(2025, 4, 18), date(2025, 5, 1),  date(2025, 8, 15),
    date(2025, 8, 27), date(2025, 10, 2), date(2025, 10, 21), date(2025, 10, 22),
    date(2025, 11, 5), date(2025, 12, 25),
}

HOLIDAY_ORDINALS        = np.array([d.toordinal() for d in HOLIDAY_DATES], dtype=np.int64)
MARKET_OPEN_MINUTES     = 9*60 + 15      # 555
MARKET_CLOSE_MINUTES    = 15*60 + 30     # 930
TRADING_MINUTES_PER_DAY = MARKET_CLOSE_MINUTES - MARKET_OPEN_MINUTES  # 375
TRADING_DAYS_PER_YEAR   = 252.0

DATA_PACKET_SEGMENT_MAP = {1: "market_data", 2: "option_data"}

# ────────────────────────────────────────────────
# Numba optimized core functions
# ────────────────────────────────────────────────

@njit(cache=True, fastmath=True)
def is_weekend(weekday: int) -> bool:
    return weekday >= 5


@njit(cache=True, fastmath=True)
def count_trading_minutes(
    start_ordinal: np.int64, # type: ignore
    end_ordinal: np.int64,
    holiday_ordinals: np.ndarray,
    current_minutes: np.float64,
    include_today: bool
) -> np.float64:
    total = 0.0
    for ordinal in range(start_ordinal, end_ordinal + 1):
        weekday = (ordinal + 1) % 7
        if is_weekend(weekday):
            continue

        is_holiday = False
        for h in holiday_ordinals:
            if ordinal == h:
                is_holiday = True
                break
        if is_holiday:
            continue

        if ordinal == start_ordinal and include_today:
            if current_minutes < MARKET_OPEN_MINUTES:
                total += TRADING_MINUTES_PER_DAY
            elif current_minutes >= MARKET_CLOSE_MINUTES:
                pass
            else:
                total += MARKET_CLOSE_MINUTES - current_minutes
        else:
            total += TRADING_MINUTES_PER_DAY

    return total


@njit(cache=True, fastmath=True)
def time_to_expiry_numba(
    today_ordinal: np.int64,
    expiry_ordinal: np.int64,
    holidays: np.ndarray,
    now_minutes: np.float64
) -> np.float64:
    if expiry_ordinal <= today_ordinal:
        return 0.0
    minutes = count_trading_minutes(today_ordinal, expiry_ordinal, holidays, now_minutes, True)
    return (minutes / TRADING_MINUTES_PER_DAY) / TRADING_DAYS_PER_YEAR

