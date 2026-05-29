import os
from dotenv import load_dotenv

load_dotenv()

# ── API 凭证 ────────────────────────────────────────────────
API_KEY    = os.getenv("OKX_API_KEY", "")
SECRET_KEY = os.getenv("OKX_SECRET_KEY", "")
PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")
FLAG       = os.getenv("OKX_FLAG", "1")   # 1=模拟盘 0=实盘

# ── 合约基本参数 ─────────────────────────────────────────────
INST_ID    = "ETH-USDT-SWAP"
CT_VAL     = 0.01          # 每张面值 0.01 ETH
BAR_15M    = "15m"         # 主策略周期
BAR_1W     = "1W"          # 周线过滤周期
LEVER      = 50            # 杠杆倍数
MGN_MODE   = "cross"       # 全仓

# ── 布林带参数 ───────────────────────────────────────────────
BOLL_PERIOD = 20           # 布林带均线周期
BOLL_STD    = 2.0          # 标准差倍数
KLINE_LIMIT = 300          # 拉取K线数量

# ── 插针判定 ─────────────────────────────────────────────────
# 影线穿越布林带的最小比例（影线长度 / K线总高度）
PIN_WICK_RATIO   = 0.4     # 影线占整根K线至少40%
# 实体必须回到布林带内部（实体收盘在带内）
PIN_BODY_INSIDE  = True

# ── 分批加仓参数 ─────────────────────────────────────────────
BATCH_COUNT      = 5       # 最多开仓批次
BATCH_SIZE_RATIO = [0.10, 0.15, 0.20, 0.25, 0.30]  # 各批次占总权益比例
# 第N批相对首批入场价的间距（以布林带宽度为单位）
BATCH_SPACING    = [0.0, 0.3, 0.6, 1.0, 1.5]

# ── 止盈 / 止损 ──────────────────────────────────────────────
TP_PROFIT_USD    = 10.0    # 均价上涨/下跌 10 USDT 即止盈平仓
SL_BEYOND_MULT   = 2.5     # 止损在布林带外 N倍标准差处（防止行情极端延伸）

# ── 风控 ─────────────────────────────────────────────────────
# 强平缓冲：要求强平价与最远批次入场价之间保留至少 X% 空间
LIQ_BUFFER       = 0.05    # 5% 安全垫
MAX_TOTAL_MARGIN = 0.60    # 所有批次合计保证金不超过账户权益 60%
MAX_DRAWDOWN     = 0.60    # 账户最大回撤 60% 停机

# ── 周线趋势过滤 ─────────────────────────────────────────────
WEEKLY_EMA_PERIOD = 10     # 周线 EMA 判断大趋势
# "bull" 只做多插针  "bear" 只做空插针  "both" 双向
# 由程序根据周线自动判断，此参数是fallback
WEEKLY_FALLBACK  = "both"

# ── 运行 ─────────────────────────────────────────────────────
POLL_INTERVAL    = 30      # 主循环间隔（秒）

# ── 通知 ─────────────────────────────────────────────────────
SERVERCHAN_KEY   = os.getenv("SERVERCHAN_KEY", "")
WEB_HOST         = "0.0.0.0"
WEB_PORT         = 8080
