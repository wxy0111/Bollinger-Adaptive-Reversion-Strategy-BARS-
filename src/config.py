"""Runtime configuration for the OKX Bollinger mean-reversion strategy.

The live strategy currently uses 15-minute Bollinger-band breakouts on
``ETH-USDT-SWAP``. Parameters that are kept for experiments but are not wired
into the live strategy are commented out in the "Reserved" sections.
"""

import os
from dotenv import load_dotenv

load_dotenv()


# API credentials.
API_KEY = os.getenv("OKX_API_KEY", "")
SECRET_KEY = os.getenv("OKX_SECRET_KEY", "")
PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")
FLAG = os.getenv("OKX_FLAG", "1")  # 1=demo trading, 0=live trading.


# Instrument settings.
INST_ID = "ETH-USDT-SWAP"
CT_VAL = 0.1  # Contract value: 1 contract = 0.1 ETH.
BAR_15M = "15m"
LEVER = 50

# Reserved: not used by the current live strategy.
# BAR_1W = "1W"
# MGN_MODE = "cross"


# Bollinger-band settings.
BOLL_PERIOD = 20
BOLL_STD = 2.0
BOLL_INCLUDE_CURRENT = True
KLINE_LIMIT = 300
MIN_BOLL_WIDTH_USD = 15
MIN_BOLL_WIDTH_PCT = 0.006
BOLL_WIDTH_BASE_PRICE = 2000.0
BOLL_WIDTH_BASE_USD = 15.0
MIN_BOLL_WIDTH_FLOOR_USD = 10.0
BOLL_WIDTH_GAP_MULT = 2.5

# Reserved: these breakout-quality filters are not wired into the current live
# signal path. The live signal only checks whether mark price is outside the
# Bollinger band, plus width and no-new-extreme filters.
# MIN_BREAK_USD = 1
# MIN_BREAK_ATR_MULT = 0.15
# MIN_REBOUND_RATIO = 0.25

NO_NEW_EXTREME_TICKS = 2
REPRICE_GAP_USD = 0.5
INSIDE_BAND_CANCEL_KLINES = 2

# Reserved: first-batch time-based expiry is disabled. Entry orders are now
# maintained by candle updates and price thresholds instead of a wall-clock TTL.
# PROBE_ORDER_TTL_SEC = 45


# Pin-bar utilities.
#
# These are used by ``src.indicators.detect_pin`` only. The current live
# strategy does not call that function.
PIN_WICK_RATIO = 0.4
PIN_BODY_INSIDE = True


# Batch-entry settings.
MAX_ENTRY_BATCHES = 12
MAX_TOTAL_ENTRY_RATIO = 0.8
FIRST_BATCH_RATIO = 0.15
SECOND_BATCH_RATIO = 0.15
DYNAMIC_BASE_ENTRY_RATIO = 0.1
DYNAMIC_MIN_ENTRY_RATIO = 0.05
DYNAMIC_MAX_ENTRY_RATIO = 0.15

# Reserved: legacy fixed-batch optimizer/backtest settings. The live strategy
# now uses the dynamic entry ratios above instead of this fixed ratio list.
BATCH_COUNT = 4
BATCH_SIZE_RATIO = [0.2, 0.2, 0.2, 0.2]
STRATEGY_EQUITY_CAP_USDT = 0.0
MIN_ORDER_CONTRACTS = 0.01
CONTRACT_STEP = 0.01
BATCH_SPACING = [0.0, 0.2, 0.4, 0.6]


# Exit settings.
TP_PROFIT_USD = 10.0
TP_TARGET_MARGIN_RETURN = 0.25
DYNAMIC_TP_ENABLED = True
DYNAMIC_TP_ARM_RETURN = 0.235
DYNAMIC_TP_RESTORE_RETURN = 0.22
DYNAMIC_TP_REPRICE_GAP_USD = 0.5
LIQ_STOP_OFFSET_USD = 0.1
LIQ_WARNING_DISTANCE_USD = 10.0
LIQ_WARNING_REPEAT_SEC = 3600
MIN_ENTRY_GAP_USD = 4
MIN_HEAD_LIQ_BUFFER_PCT = 0.03
DYNAMIC_ENTRY_GAP_ENABLED = True
DYNAMIC_ENTRY_GAP_MAX_USD = 40.0
OKX_MAINTENANCE_MARGIN_RATE = 0.004
OKX_LIQ_FEE_RATE = 0.0005

# Reserved: these planned risk controls are not wired into
# ``src.risk.build_batch_plan`` yet.
# SL_BEYOND_MULT = 2.5
# LIQ_BUFFER = 0.012
# MAX_TOTAL_MARGIN = 1.0

MAX_DRAWDOWN = 1.0


# Weekly-trend utilities.
#
# ``WEEKLY_EMA_PERIOD`` is used by ``src.indicators.weekly_trend`` only. The
# current live strategy does not call that function.
WEEKLY_EMA_PERIOD = 10

# Reserved: not used by the current live strategy.
# WEEKLY_FALLBACK = "both"


# Capital management.
#
# After every closed position, the strategy tries to keep the trading account
# at this available USDT balance by transferring profit to the funding account
# or topping up losses from it. Set to 0 to disable rebalancing.
TRADING_ACCOUNT_TARGET = 200.0


# Runtime settings.
PRICE_LOG_INTERVAL = 1
POLL_INTERVAL = 3


# Notification and dashboard settings.
SERVERCHAN_KEY = os.getenv("SERVERCHAN_KEY", "")
WEB_HOST = "0.0.0.0"
WEB_PORT = 8080
