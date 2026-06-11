"""Runtime configuration for the OKX Bollinger mean-reversion strategy.

This file is the main control panel for the live strategy. Parameters are
grouped by behavior module so you can quickly decide what to change.

当前文件是策略主配置面板。参数按功能模块分组，方便查看和控制。
"""

import os

from dotenv import load_dotenv

load_dotenv()


# =============================================================================
# 1. OKX Account And Instrument / OKX账户与交易品种
# =============================================================================

# API credentials are read from .env. Never commit real keys to GitHub.
# API密钥从.env读取，请不要提交真实密钥。
API_KEY = os.getenv("OKX_API_KEY", "")
SECRET_KEY = os.getenv("OKX_SECRET_KEY", "")
PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")

# 1 = demo trading, 0 = live trading.
# 1=模拟盘，0=实盘。
FLAG = os.getenv("OKX_FLAG", "1")

# Trading instrument.
# 交易品种。
INST_ID = "ETH-USDT-SWAP"

# 1 OKX ETH-USDT-SWAP contract = 0.1 ETH.
# 合约面值：1张=0.1 ETH。
CT_VAL = 0.1

# Leverage used for sizing and risk estimates.
# 杠杆倍数：实盘账户也需要设置成一致。
LEVER = 50

# Main candle period for Bollinger logic.
# 策略主K线周期。
BAR_15M = "15m"

# Reserved, not used by the current live strategy.
# 保留参数，当前实盘不使用。
# BAR_1W = "1W"
# MGN_MODE = "cross"


# =============================================================================
# 2. Runtime Rhythm / 运行节奏
# =============================================================================

# Market snapshot log interval in seconds.
# 行情日志记录间隔：1秒记录一次价格和布林带快照。
PRICE_LOG_INTERVAL = 1

# Main strategy loop interval in seconds.
# 策略主循环间隔：同步、判断、下单、撤单、资金管理按这个节奏执行。
POLL_INTERVAL = 3


# =============================================================================
# 3. Bollinger Signal Module / 布林带信号模块
# =============================================================================

# Bollinger lookback period.
# 布林带周期：20表示使用最近20根15m K线。
BOLL_PERIOD = 20

# Bollinger standard-deviation multiplier.
# 标准差倍数：2.0是常见默认值；调小更容易触发，调大更保守。
BOLL_STD = 2.0

# Whether to include the unfinished current candle.
# 是否包含当前未收盘K线：True表示布林带会随盘中价格实时变化。
BOLL_INCLUDE_CURRENT = True

# Number of candles fetched from OKX.
# OKX K线拉取数量，通常不需要改。
KLINE_LIMIT = 300

# Minimum absolute Bollinger width in USDT.
# 最小布林带宽度：低于该宽度不建仓、不补仓。
MIN_BOLL_WIDTH_USD = 15

# Minimum Bollinger width as current-price percentage.
# 最小布林带宽度百分比：0.008=0.8%。
MIN_BOLL_WIDTH_PCT = 0.015

# Dynamic Bollinger-width reference.
# 动态布林宽度基准：价格在2000附近时，参考宽度为15U。
BOLL_WIDTH_BASE_PRICE = 2000.0
BOLL_WIDTH_BASE_USD = 15.0

# Absolute floor for dynamic Bollinger width.
# 动态布林宽度下限，防止价格较低时阈值过小。
MIN_BOLL_WIDTH_FLOOR_USD = 10.0

# Bollinger width can also be constrained by effective entry gap * this value.
# 布林宽度和有效补仓间距联动：有效间距 * 该倍数也会作为宽度候选。
BOLL_WIDTH_GAP_MULT = 2.5

# Require enough Bollinger width to cover the configured take-profit target.
# 例如 25% 保证金收益 / 50x = 0.5% 价格波动；乘以 2.0 后，要求布林宽度至少覆盖约 1.0% 的价格空间。
BOLL_WIDTH_TP_SPACE_ENABLED = False
BOLL_WIDTH_TP_SPACE_MULT = 1.5

# Head-entry maximum Bollinger width filter.
# 头仓最大布林宽度过滤：只限制新开头仓，不限制已有持仓补仓。
# If either threshold is hit, the strategy skips/cancels the first batch.
# 任一阈值触发时，跳过或撤销未成交头仓。
ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED = True
ENTRY_MAX_BOLL_WIDTH_PCT = 0.028
ENTRY_MAX_BOLL_WIDTH_USD = 80.0

# Entry disaster score filter, stacked after the max-width filter.
# 入场灾难评分过滤：在最大布林宽度过滤之后叠加，用于拦截单边趋势破轨。
ENTRY_DISASTER_FILTER_ENABLED = True
ENTRY_DISASTER_SCORE_THRESHOLD = 4
ENTRY_DISASTER_KLINE_COUNT = 3
ENTRY_DISASTER_WIDTH_EXPAND = 1.5
ENTRY_DISASTER_TP_DISTANCE_MULT = 4.0
ENTRY_DISASTER_EXPECTED_RETURN = 0.25

# Recent-price count for no-new-extreme filter.
# 创新高/创新低观察点数：2表示用最近3个价格点判断是否还在继续创新极值。
NO_NEW_EXTREME_TICKS = 2

# Reserved breakout-quality filters; current live signal does not use them.
# 保留参数，当前实盘信号未接入。
# MIN_BREAK_USD = 1
# MIN_BREAK_ATR_MULT = 0.15
# MIN_REBOUND_RATIO = 0.25


# =============================================================================
# 4. Entry Order Maintenance / 开仓挂单维护
# =============================================================================

# Minimum price change before replacing a pending entry order.
# 重挂阈值：新旧挂单价差小于该值时不撤单重挂。
REPRICE_GAP_USD = 0.5

# Kept for compatibility; current strategy mainly uses candle updates and price
# thresholds, not inside-band immediate cancellation.
# 兼容保留：当前主要按K线更新和价格阈值维护挂单。
INSIDE_BAND_CANCEL_KLINES = 2

# Reserved: first-batch wall-clock expiry is disabled.
# 保留参数：45秒未成交撤单逻辑已停用。
# PROBE_ORDER_TTL_SEC = 45


# =============================================================================
# 5. Position Sizing And Batch Ratios / 仓位大小与分批比例
# =============================================================================

# Maximum number of entry batches in one position cycle.
# 单轮最大入场批次数，包含头仓。
MAX_ENTRY_BATCHES = 12

# Maximum total margin ratio used by all batches.
# 最大总入场保证金比例：0.8表示最多使用目标策略资金的80%。
MAX_TOTAL_ENTRY_RATIO = 0.8

# First and second batch margin ratios.
# 头仓和第一次补仓比例：按策略目标资金计算保证金占比。
FIRST_BATCH_RATIO = 0.1

# Second batch dynamic sizing. When enabled, the first add-on ratio is scaled
# by the gap between the head fill price and the candidate add-on price.
SECOND_BATCH_DYNAMIC_BASE_RATIO = 0.14
SECOND_BATCH_DYNAMIC_MIN_RATIO = 0.05
SECOND_BATCH_DYNAMIC_MAX_RATIO = 0.18
SECOND_BATCH_DYNAMIC_FULL_GAP_USD = 10.0

# Dynamic add-on ratio for the 3rd batch and later.
# 第3批及之后动态补仓比例区间。
DYNAMIC_BASE_ENTRY_RATIO = 0.08
DYNAMIC_MIN_ENTRY_RATIO = 0.05
DYNAMIC_MAX_ENTRY_RATIO = 0.15

# Order-size rounding.
# OKX最小下单张数和步进。
MIN_ORDER_CONTRACTS = 0.01
CONTRACT_STEP = 0.01

# Reserved legacy fixed-batch model. Current live strategy uses dynamic ratios.
# 保留旧固定分批模型，当前实盘不依赖这些比例/间距。
BATCH_COUNT = 4
BATCH_SIZE_RATIO = [0.2, 0.2, 0.2, 0.2]
STRATEGY_EQUITY_CAP_USDT = 0.0
BATCH_SPACING = [0.0, 0.2, 0.4, 0.6]


# =============================================================================
# 6. Add-on Gap And Add-on Guards / 补仓间距与补仓限制
# =============================================================================

# Base minimum gap between filled entry batches.
# 基础补仓间距：后一批与上一批真实成交价至少相差该数值。
MIN_ENTRY_GAP_USD = 6

# Required first-entry liquidation buffer after a full ladder.
# 头仓强平缓冲：按最小补仓一路补到上限后，头仓到预估强平至少保留该比例。
MIN_HEAD_LIQ_BUFFER_PCT = 0.05

# Enlarge entry gap when liquidation buffer would be too small.
# 动态强平缓冲间距：如果基础间距不足以保持强平缓冲，会自动提高有效间距。
DYNAMIC_ENTRY_GAP_ENABLED = True
DYNAMIC_ENTRY_GAP_MAX_USD = 40.0

# Dynamic add-on gap multiplier. It only enlarges the gap; it never makes
# add-ons denser than MIN_ENTRY_GAP_USD.
# 动态补仓间距放大：根据布林扩张、头仓逆向幅度、连续K线趋势放大补仓间距。
ADDON_DYNAMIC_GAP_ENABLED = True
ADDON_DYNAMIC_GAP_MAX_USD = 20.0
ADDON_DYNAMIC_GAP_BOLL_START = 1.2
ADDON_DYNAMIC_GAP_BOLL_STRONG = 1.8
ADDON_DYNAMIC_GAP_BOLL_MAX_MULT = 1.5
ADDON_DYNAMIC_GAP_HEAD_START_PCT = 0.015
ADDON_DYNAMIC_GAP_HEAD_STRONG_PCT = 0.03
ADDON_DYNAMIC_GAP_HEAD_MAX_MULT = 1.2
ADDON_DYNAMIC_GAP_TREND_KLINES = 3
ADDON_DYNAMIC_GAP_TREND_MULT = 1.25

# Maximum Bollinger width for add-on orders.
# 补仓最大布林宽度：持仓后布林带过宽时停止新增补仓，并撤销未成交补仓单。
ADDON_MAX_BOLL_WIDTH_FILTER_ENABLED = False
ADDON_MAX_BOLL_WIDTH_PCT = 0.04
ADDON_MAX_BOLL_WIDTH_USD = 80.0

# Add-on take-profit improvement guard.
# 补仓止盈改善守卫：补仓后，目标止盈价必须明显变得更容易触达才允许补仓。
ADDON_TP_IMPROVE_GUARD_ENABLED = True

# Expected take-profit return used by the add-on guard.
# 守卫使用的预期止盈收益：按 25% 保证金收益计算，不跟随当前真实止盈目标 28%。
ADDON_TP_IMPROVE_EXPECTED_RETURN = 0.25

# Required improvement as a fraction of the expected take-profit distance.
# 止盈价改善比例：1.0 表示至少改善一个完整预期止盈距离。
ADDON_TP_IMPROVE_RATIO = 1.0

# Minimum absolute improvement in USDT.
# 最小绝对改善值：防止低价时阈值过小。
ADDON_TP_IMPROVE_MIN_USD = 1.0

# Completed-candle extreme guard for add-on orders.
# 补仓K线极值确认：头仓成交后记录已完成K线极值。
# 多单补仓价必须低于记录低点；空单补仓价必须高于记录高点。
ADDON_EXTREME_GUARD_ENABLED = True

# 24h extreme-distance adjustment for add-on spacing.
# 24h极值距离放大：默认关闭。打开后，头仓离24h极值越远，补仓间距越大。
ENTRY_EXTREME_GAP_ADJUST_ENABLED = False
ENTRY_EXTREME_GAP_BASE_PCT = 0.01
ENTRY_EXTREME_GAP_FULL_PCT = 0.03
ENTRY_EXTREME_GAP_MAX_MULT = 2.0
ENTRY_24H_TICKER_CACHE_SEC = 60


# =============================================================================
# 7. Take Profit Module / 止盈模块
# =============================================================================

# Legacy fixed take-profit distance, kept as fallback only.
# 旧固定止盈距离，当前主要使用TP_TARGET_MARGIN_RETURN。
TP_PROFIT_USD = 10.0

# Target margin return for default take profit.
# 默认止盈收益率：0.25=保证金收益25%。
TP_TARGET_MARGIN_RETURN = 0.28

# Dynamic take-profit lock.
# 动态锁盈：达到启动收益后，如价格不再创新高/低，则按实时价格附近止盈。
DYNAMIC_TP_ENABLED = True
DYNAMIC_TP_ARM_RETURN = 0.22
DYNAMIC_TP_RESTORE_RETURN = 0.18
DYNAMIC_TP_REPRICE_GAP_USD = 0.5

# Bollinger take-profit compression exit.
# 布林压缩止盈：当前默认关闭。持仓有浮盈且布林目标边界压到止盈价内时提前退出。
BOLL_TP_COMPRESSION_ENABLED = False
BOLL_TP_COMPRESSION_MIN_RETURN = 0.12
BOLL_TP_COMPRESSION_EXIT_OFFSET_USD = 0.1


# =============================================================================
# 8. Stop Loss And Risk Guards / 止损与风险守卫
# =============================================================================

# OKX liquidation formula approximations used by live/offline estimates.
# OKX强平估算参数，用于动态间距和风险估算。
OKX_MAINTENANCE_MARGIN_RATE = 0.004
OKX_LIQ_FEE_RATE = 0.0005

# Stop trigger offset from liquidation price.
# 强平线止损触发偏移。
LIQ_STOP_OFFSET_USD = 0.1
LIQ_STOP_REPRICE_GAP_USD = 0.2

# Liquidation warning.
# 强平预警：距离强平价10U以内提醒，同一持仓每小时最多一次。
LIQ_WARNING_DISTANCE_USD = 10.0
LIQ_WARNING_REPEAT_SEC = 3600

# Fixed-loss stop for the active strategy position.
# 固定亏损止损：按当前总仓位重新计算条件止损触发价。
COPY_FIXED_LOSS_STOP_ENABLED = True
COPY_FIXED_LOSS_STOP_USDT = 0.0
COPY_FIXED_LOSS_STOP_RATIO = 0.95

# Keep the fixed-loss stop far enough from first entry.
# 固定止损头仓缓冲：如果补仓会让固定止损进入头仓指定范围内，则跳过补仓。
FIXED_LOSS_HEAD_BUFFER_ENABLED = True
FIXED_LOSS_HEAD_BUFFER_PCT = 0.05

# Disaster stop for one strategy cycle.
# 灾难止损：头仓逆向达到阈值且浮亏达到策略资金比例时，平掉当前仓位并继续运行。
DISASTER_STOP_ENABLED = True
DISASTER_HEAD_DROP_PCT = 0.05
DISASTER_LOSS_RATIO = 0.7

# Bollinger-mid cost stop.
# 均线成本止损：持仓中实时布林中轨穿过持仓均价时，市价平掉当前仓位。
BOLL_MID_COST_STOP_ENABLED = True

# Trend risk guard.
# 趋势风险守卫：开启后进行趋势恶化评分，达到阈值会打印日志并发送微信通知。
TREND_RISK_GUARD_ENABLED = True

# 趋势风险守卫市价平仓开关：False=只提醒不平仓，True=触发后市价平当前仓位。
TREND_RISK_GUARD_CLOSE_ENABLED = False

# 趋势风险守卫冻结补仓：触发后不再新增补仓，并撤销未成交补仓单；头仓和止盈/止损不受影响。
TREND_RISK_FREEZE_ADDON_ENABLED = True
TREND_RISK_SCORE_THRESHOLD = 5
TREND_RISK_HEAD_ADVERSE_PCT = 0.04
TREND_RISK_KLINE_COUNT = 3
TREND_RISK_MIN_HOLD_MIN = 30
TREND_RISK_SLOPE_WINDOW_MIN = 45
TREND_RISK_MID_SLOPE_PCT_PER_HOUR = 0.2
TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR = 0.8
TREND_RISK_WIDTH_EXPAND = 2.5
TREND_RISK_NOTIFY_INTERVAL_SEC = 3600

# Reserved planned risk controls; not wired into src.risk.build_batch_plan.
# 保留风险参数，当前未接入实盘主逻辑。
# SL_BEYOND_MULT = 2.5
# LIQ_BUFFER = 0.012
# MAX_TOTAL_MARGIN = 1.0


# =============================================================================
# 9. Capital And Cross-Copy Protection / 资金固本与带单保护
# =============================================================================

# Strategy target capital.
# 策略目标资金：开仓/补仓按这个目标资金计算；平仓后利润划走，亏损从资金账户补回。
TRADING_ACCOUNT_TARGET = 50

# Rolling compound mode.
# 滚仓模式：开启后盈利不再自动划转到资金账户，开仓/补仓/策略止损按真实有效权益计算。
# 若同时开启 CROSS_COPY_PROTECT_ENABLED，有效权益 = 账户总权益 - CROSS_COPY_PROTECT_EQUITY_USDT。
ROLLING_COMPOUND_ENABLED = False

# Cross-margin copy-protection mode.
# 全仓带单保护：保护固定账户权益，策略只使用可用策略资金部分。
CROSS_COPY_PROTECT_ENABLED = False
CROSS_COPY_PROTECT_EQUITY_USDT = 500.0
CROSS_COPY_DYNAMIC_SIZING_ENABLED = False

# Sizing-equity log threshold.
# 资金计算日志固定下限：实际显示阈值 = max(该值, TRADING_ACCOUNT_TARGET * 10%)，避免小波动刷屏。
SIZING_EQUITY_LOG_THRESHOLD_USDT = 0.5

# Post-close capital calibration.
# 平仓后资金校准：用于减少小数频繁划转。
CAPITAL_REBALANCE_TOLERANCE_USDT = 0.5
CAPITAL_REBALANCE_DELAY_SEC = 2


# =============================================================================
# 10. Experimental Indicator Utilities / 实验指标工具
# =============================================================================

# Pin-bar utilities are not wired into current live entry logic.
# Pin-bar工具参数，当前实盘不调用。
PIN_WICK_RATIO = 0.4
PIN_BODY_INSIDE = True

# Weekly-trend utility is not wired into current live entry logic.
# 周线趋势工具参数，当前实盘不调用。
WEEKLY_EMA_PERIOD = 10

# Reserved, not used by current live strategy.
# 保留参数，当前未使用。
# WEEKLY_FALLBACK = "both"


# =============================================================================
# 11. Notification And Dashboard / 通知与本地看板
# =============================================================================

# ServerChan SendKey from .env.
# ServerChan微信推送密钥。
SERVERCHAN_KEY = os.getenv("SERVERCHAN_KEY", "")

# Local dashboard bind host and port.
# 本地看板地址：http://localhost:8080
WEB_HOST = "0.0.0.0"
WEB_PORT = 8080
