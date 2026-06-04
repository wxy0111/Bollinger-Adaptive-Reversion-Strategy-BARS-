# Bollinger Adaptive Reversion Strategy (BARS)

<p align="center">
  <img src="assets/bars-logo-dark.png" alt="BARS Logo" width="420">
</p>

**BARS** stands for **Bollinger Adaptive Reversion Strategy**. It is an
OKX `ETH-USDT-SWAP` strategy built around Bollinger-band mean reversion,
adaptive staged entries, capital-lock profit handling, and risk guards.

中文名：**布林自适应回归策略**。核心逻辑是布林带极值入场、动态补仓摊平、
按目标保证金收益止盈、盈利固本划转，并通过固定亏损止损、强平缓冲和趋势
风险观察来限制极端行情风险。

这是一个运行在 OKX `ETH-USDT-SWAP` 永续合约上的 15 分钟布林带均值回归策略。程序读取 OKX 15m K 线和实时标记价格，当价格突破布林带外侧并停止继续创新极值时，按动态分批方式建立仓位，并用交易所真实持仓均价管理止盈、止损、补仓、资金固本和微信通知。

项目包含实盘/模拟盘主程序、本地网页看板、ServerChan 微信推送、固本资金管理、风险保护、日志清理工具，以及基于运行日志的参数优化报告工具。

## 重要提醒

本程序涉及高杠杆合约交易，可能快速亏损或强平。默认配置使用 `OKX_FLAG=1`，也就是 OKX 模拟盘。正式使用前，请先在模拟盘确认下单、撤单、止盈、止损、资金划转和通知都符合预期。

不要把 `.env`、真实 API 密钥、运行日志、历史数据、压缩包、优化结果、极端行情模拟文件提交到 GitHub。

## 程序结构

```text
C:\okx
├── main.py                         # 程序入口，启动看板和策略
├── start.bat                       # Windows 一键启动脚本
├── optimize_report.bat             # 手动生成日志参数优化报告
├── requirements.txt                # Python 依赖
├── .env.example                    # 环境变量示例
├── src
│   ├── config.py                   # 策略参数控制面板
│   ├── strategy.py                 # 核心交易策略
│   ├── okx_client.py               # OKX REST API 封装
│   ├── risk.py                     # 批次计划、张数、止盈和强平估算
│   ├── position_manager.py         # 本地持仓、批次和订单状态
│   ├── indicators.py               # K 线整理和布林带指标
│   ├── notify.py                   # ServerChan 微信推送
│   ├── logging_utils.py            # 终端日志分类和颜色
│   └── dashboard.py                # 本地网页看板
├── backtest
│   ├── log_parameter_optimizer.py  # 基于运行日志的参数优化器
│   └── clean_strategy_logs.py      # 日志清理工具
└── logs                            # 运行日志和本地状态，默认不提交
```

## 运行方式

1. 复制 `.env.example` 为 `.env`，填写 OKX API 和 ServerChan 配置。
2. 检查 [src/config.py](src/config.py) 中的策略参数。
3. 运行：

```powershell
start.bat
```

程序启动后会同时运行交易策略和本地看板。看板默认地址：

```text
http://localhost:8080
```

## 微信推送

项目使用 ServerChan 发送微信通知。需要在 `.env` 中配置：

```text
SERVERCHAN_KEY=你的SendKey
```

获取方式：

1. 打开 [ServerChan](https://sct.ftqq.com/)。
2. 使用微信扫码登录。
3. 在 SendKey 页面复制自己的 SendKey。
4. 写入 `.env` 的 `SERVERCHAN_KEY`。

程序会在策略触发的开仓挂单、开仓成交、补仓挂单、补仓成交、平仓、资金不足、资金恢复、BTG 观察、带单保护和强平风险事件中发送通知。强平预警只在距离强平价 `10U` 内提醒，并且同一持仓最多每 1 小时提醒一次。

## 行情采样和交易节奏

当前设置：

```python
PRICE_LOG_INTERVAL = 1
POLL_INTERVAL = 3
```

- 每 `1s` 记录一次价格和布林带快照到日志，用于后续更高精度评估。
- 每 `3s` 执行一次交易主逻辑，包括余额检查、成交同步、开仓、补仓、撤单、止盈、止损和资金管理。
- 终端行情显示仍按主逻辑节奏刷新，避免 1s 行情刷屏。

## 布林带和头仓过滤

当前核心参数：

```python
BOLL_PERIOD = 20
BOLL_STD = 2.0
BOLL_INCLUDE_CURRENT = True

MIN_BOLL_WIDTH_USD = 15
MIN_BOLL_WIDTH_PCT = 0.008
BOLL_WIDTH_BASE_PRICE = 2000.0
BOLL_WIDTH_BASE_USD = 15.0
MIN_BOLL_WIDTH_FLOOR_USD = 10.0
BOLL_WIDTH_GAP_MULT = 2.5

ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED = True
ENTRY_MAX_BOLL_WIDTH_PCT = 0.025
ENTRY_MAX_BOLL_WIDTH_USD = 80.0
```

`BOLL_INCLUDE_CURRENT=True` 表示布林带包含当前未收盘 K 线，布林带会随盘中价格实时变化。

最低布林宽度用于避免窄幅低波动行情开仓；最大布林宽度只限制新开头仓，用来避免在极端扩张或趋势加速阶段开第一单。当前最大宽度过滤为：布林宽度达到价格的 `2.5%` 或绝对宽度达到 `80U` 时，不开新头仓。该规则不影响已有持仓后的补仓。

## 开仓逻辑

没有持仓、也没有正在工作的入场挂单时，程序按下面顺序判断头仓：

```text
价格突破布林带外侧
+ 布林宽度不低于最低阈值
+ 布林宽度不高于头仓最大阈值
+ 价格不再继续创新高/新低
+ 当前 K 线没有开过新计划
+ 入场价和上一套计划价格距离足够
=> 挂第 1 批头仓限价单
```

方向判断：

```text
价格 < 布林下轨 => 做多
价格 > 布林上轨 => 做空
```

同一根 15m K 线最多新增一笔入场批次。头仓成交后，本根 K 线不继续补仓，等待下一根 K 线重新判断。平仓完成后，平仓所在的这根 15m K 线不再开新头仓。

如果未成交头仓挂单后，下一根 K 线判断时布林宽度低于最低阈值，或高于头仓最大宽度阈值，程序会撤销该未成交头仓。

## 补仓逻辑

补仓不再依赖旧的 `BATCH_SPACING` 固定间距，而是根据当前触发价、上一批真实成交价、动态间距和 K 线 guard 判断。

```text
已有持仓
+ 当前没有未成交补仓单
+ 再次触发同方向布林带外侧
+ 布林宽度满足动态阈值
+ 价格不再继续创新高/新低
+ 当前 K 线没有新增过入场批次
+ 补仓价和上一批真实成交价距离 >= 有效补仓间距
+ 补仓价突破头仓后记录的已完成 K 线极值
+ 固定亏损止损不会被推到头仓 5% 以内
=> 按当前 mark_price 挂下一批限价单
```

当前补仓相关参数：

```python
MIN_ENTRY_GAP_USD = 6
MIN_HEAD_LIQ_BUFFER_PCT = 0.03
DYNAMIC_ENTRY_GAP_ENABLED = True
DYNAMIC_ENTRY_GAP_MAX_USD = 40.0

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

ADDON_EXTREME_GUARD_ENABLED = True
FIXED_LOSS_HEAD_BUFFER_ENABLED = True
FIXED_LOSS_HEAD_BUFFER_PCT = 0.05
```

有效补仓间距会根据强平缓冲、布林扩张、头仓逆向波动和连续 K 线趋势自动放大，但不会低于 `MIN_ENTRY_GAP_USD`。

补仓 K 线极值 guard 的逻辑是：从头仓成交后开始记录已完成 15m K 线的极值。多单补仓价必须低于记录低点；空单补仓价必须高于记录高点。

## 动态分批张数

当前策略使用动态补仓比例：

```python
FIRST_BATCH_RATIO = 0.1
SECOND_BATCH_RATIO = 0.15
DYNAMIC_BASE_ENTRY_RATIO = 0.08
DYNAMIC_MIN_ENTRY_RATIO = 0.05
DYNAMIC_MAX_ENTRY_RATIO = 0.15
MAX_TOTAL_ENTRY_RATIO = 0.8
MAX_ENTRY_BATCHES = 12
```

- 第 1 批头仓使用目标策略资金的 `10%` 保证金。
- 第 2 批补仓使用目标策略资金的 `15%` 保证金。
- 第 3 批及之后，根据前后价差动态调整比例。
- 动态补仓比例限制在 `5%` 到 `15%` 之间。
- 全部入场批次占用保证金最多不超过目标策略资金的 `80%`。
- 如果超过 `80%` 上限、有效可用资金不足，或固定止损缓冲不满足，程序会放弃本次补仓，不会缩小比例强行补仓。

旧参数 `BATCH_COUNT`、`BATCH_SIZE_RATIO` 和 `BATCH_SPACING` 仅作为历史兼容保留，不再作为当前实盘补仓模型的核心依据。

## 挂单维护

未成交头仓或补仓单由 K 线更新和价格阈值维护：

```text
K 线更新
+ 仍满足同方向轨外条件
+ 新挂单价和旧挂单价差 >= REPRICE_GAP_USD
=> 撤旧单，按新价格重挂
```

当前重挂阈值：

```python
REPRICE_GAP_USD = 0.5
```

程序不再使用 45 秒未成交自动撤单逻辑。挂单是否撤销主要由 K 线更新、价格阈值、布林宽度阈值和方向条件决定。

## 止盈和动态锁盈

当前默认止盈不是固定 `10U`，而是按保证金收益率计算：

```python
TP_TARGET_MARGIN_RETURN = 0.25
```

止盈距离：

```text
止盈距离 = 平均成本 * TP_TARGET_MARGIN_RETURN / LEVER
```

在 ETH 价格约 `2000`、杠杆 `50x` 时，`25%` 保证金收益约等于 `10U` 价格距离。

动态锁盈参数：

```python
DYNAMIC_TP_ENABLED = True
DYNAMIC_TP_ARM_RETURN = 0.23
DYNAMIC_TP_RESTORE_RETURN = 0.21
DYNAMIC_TP_REPRICE_GAP_USD = 0.5
```

逻辑：

```text
默认挂 25% 动态止盈
浮盈达到 23% 后开始观察
如果多单不再创新高，或空单不再创新低：
    撤原止盈单
    按当前实时价格附近挂 reduce-only 止盈单
如果实时价止盈未成交，且浮盈回落到 21% 以下：
    撤实时价止盈
    恢复 25% 动态止盈
```

每次有新批次成交后，程序会退出动态锁盈状态，按交易所真实持仓均价重新计算默认止盈，并重挂 reduce-only 止盈单。

`BOLL_TP_COMPRESSION_ENABLED` 当前默认关闭。

## 止损和风险守卫

强平线作为最终风险边界，程序会挂条件止损单：

```python
LIQ_STOP_OFFSET_USD = 0.1
LIQ_WARNING_DISTANCE_USD = 10.0
LIQ_WARNING_REPEAT_SEC = 3600
```

固定亏损止损：

```python
COPY_FIXED_LOSS_STOP_ENABLED = True
COPY_FIXED_LOSS_STOP_USDT = 0.0
COPY_FIXED_LOSS_STOP_RATIO = 0.95
```

当 `COPY_FIXED_LOSS_STOP_USDT = 0` 时，本轮固定亏损止损按 `TRADING_ACCOUNT_TARGET * COPY_FIXED_LOSS_STOP_RATIO` 计算。

灾难止损：

```python
DISASTER_STOP_ENABLED = True
DISASTER_HEAD_DROP_PCT = 0.05
DISASTER_LOSS_RATIO = 0.7
```

只有同时满足头仓逆向达到 `5%`，并且本轮浮亏达到 `TRADING_ACCOUNT_TARGET * 70%`，才触发灾难止损。

BTG 布林趋势扩张守卫：

```python
BOLL_TREND_GUARD_OBSERVE_ENABLED = True
BOLL_TREND_GUARD_CONTROL_ENABLED = False
BOLL_TREND_GUARD_HEAD_ADVERSE_PCT = 0.03
BOLL_TREND_GUARD_WIDTH_EXPAND = 2.5
BOLL_TREND_GUARD_WIDTH_PCT = 0.035
```

当前 BTG 是观察模式：只记录和推送，不自动平仓。控制模式只有在 `BOLL_TREND_GUARD_CONTROL_ENABLED=True` 时才会平仓。

## 固本资金和全仓带单保护

当前资金参数：

```python
TRADING_ACCOUNT_TARGET = 200
CROSS_COPY_PROTECT_ENABLED = True
CROSS_COPY_PROTECT_EQUITY_USDT = 500.0
CROSS_COPY_DYNAMIC_SIZING_ENABLED = True
```

全仓带单保护 sizing：

```text
strategy sizing equity = min(TRADING_ACCOUNT_TARGET, account equity - CROSS_COPY_PROTECT_EQUITY_USDT)
```

如果账户权益小于或等于 `CROSS_COPY_PROTECT_EQUITY_USDT`，程序会撤销订单、发送通知并停止新开仓；不会再主动市价平仓。

平仓后程序优先读取真实成交收益：

- 盈利：只把真实利润划转到资金账户。
- 亏损：按真实亏损从资金账户补回。
- 如果资金账户不足以补回目标，程序会发送通知，并暂停新开仓和新补仓，但继续记录行情、同步持仓和管理已有订单。
- 后续你补充资金后，账户恢复到目标以上，程序会发送恢复通知；超过目标的部分会按规则划回资金账户。

## 本地看板

看板地址：

```text
http://localhost:8080
```

看板只读取本地运行状态和日志，不负责下单。页面采用深色交易终端风格，左侧导航分为实时看板和历史日志。

实时看板显示：

- 当前标记价格、布林带位置和布林宽度。
- 账户权益、峰值、回撤和今日收益。
- 当前持仓方向、均价、张数、浮盈亏、止盈价和强平价。
- 批次状态和最近成交流水。

历史日志页支持读取 `logs/boll_pin_*.log`，显示价格、布林带、关键交易点、单日开单情况和实际固本收益。历史复盘图会标注：

- 价格线和布林上轨/中轨/下轨。
- 布林带上下轨之间的波动区域。
- 头仓点、补仓点和平仓/固本划转点。
- 鼠标悬停时显示时间、价格、布林宽度和交易事件。

历史实际收益优先来自固本划转日志，例如：

```text
[Capital] Profit +13.1408 USDT
```

如果没有固本划转记录，才回退使用成交批次、止盈挂单价格和 `Position closed` 记录估算收益。

## 日志清理和参数优化

手动运行参数优化：

```powershell
optimize_report.bat
```

或直接运行：

```powershell
python backtest\log_parameter_optimizer.py --sample-sec 3
```

优化器默认读取：

```text
logs/boll_pin_*.log
```

当前优化器会回放实盘保护模型，包括：

- 固本和全仓带单 sizing
- 最大总入场比例限制
- 补仓 K 线极值 guard
- 固定亏损止损
- 灾难止损
- 动态补仓间距
- 头仓最大布林宽度过滤

当前优化器默认只搜索核心风险/收益参数，其他配置固定为实盘当前值参与回放：

```text
ENTRY_MAX_BOLL_WIDTH_PCT
MIN_ENTRY_GAP_USD
BOLL_WIDTH_TP_SPACE_MULT
FIRST_BATCH_RATIO
SECOND_BATCH_DYNAMIC_BASE_RATIO
SECOND_BATCH_DYNAMIC_MIN_RATIO
SECOND_BATCH_DYNAMIC_MAX_RATIO
SECOND_BATCH_DYNAMIC_FULL_GAP_USD
DYNAMIC_BASE_ENTRY_RATIO
DYNAMIC_MIN_ENTRY_RATIO
DYNAMIC_MAX_ENTRY_RATIO
MAX_TOTAL_ENTRY_RATIO
COPY_FIXED_LOSS_STOP_RATIO
FIXED_LOSS_HEAD_BUFFER_PCT
DISASTER_HEAD_DROP_PCT
DISASTER_LOSS_RATIO
ADDON_DYNAMIC_GAP_MAX_USD
```

行情日志会额外记录 `kline`、`width`、`width_pct` 和 `trading_balance`，用于后续检查回测和实盘状态是否对齐；旧的 `price=... Boll[...]` 格式仍然保留，现有回放脚本可以继续解析。

报告跑完后，终端会显示参数同步菜单。同步前需要再次输入确认，脚本会先生成 `src/config.py.bak` 备份。

如果想把中英文混合的行情日志行统一成英文格式，可以先生成清理副本：

```powershell
python backtest\clean_strategy_logs.py --log-dir logs --out-dir logs_cleaned
python backtest\log_parameter_optimizer.py --log-dir logs_cleaned --sample-sec 3
```

`logs_cleaned/` 是生成副本，默认不提交到 GitHub。

## 常用参数

主要参数集中在 [src/config.py](src/config.py)：

```python
INST_ID = "ETH-USDT-SWAP"
LEVER = 50

PRICE_LOG_INTERVAL = 1
POLL_INTERVAL = 3

BOLL_PERIOD = 20
BOLL_STD = 2.0
BOLL_INCLUDE_CURRENT = True

MIN_BOLL_WIDTH_USD = 15
MIN_BOLL_WIDTH_PCT = 0.008
ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED = True
ENTRY_MAX_BOLL_WIDTH_PCT = 0.025
ENTRY_MAX_BOLL_WIDTH_USD = 80.0

MIN_ENTRY_GAP_USD = 6
REPRICE_GAP_USD = 0.5

FIRST_BATCH_RATIO = 0.1
SECOND_BATCH_RATIO = 0.15
DYNAMIC_BASE_ENTRY_RATIO = 0.08
DYNAMIC_MIN_ENTRY_RATIO = 0.05
DYNAMIC_MAX_ENTRY_RATIO = 0.15
MAX_TOTAL_ENTRY_RATIO = 0.8

TP_TARGET_MARGIN_RETURN = 0.25
DYNAMIC_TP_ARM_RETURN = 0.23
DYNAMIC_TP_RESTORE_RETURN = 0.21

COPY_FIXED_LOSS_STOP_RATIO = 0.95
DISASTER_HEAD_DROP_PCT = 0.05
DISASTER_LOSS_RATIO = 0.7

TRADING_ACCOUNT_TARGET = 200
CROSS_COPY_PROTECT_ENABLED = True
CROSS_COPY_PROTECT_EQUITY_USDT = 500.0
CROSS_COPY_DYNAMIC_SIZING_ENABLED = True
```

## GitHub 注意事项

`.gitignore` 默认应排除：

```text
.env
logs/
logs_cleaned/
.idea/
*.zip
backtest/results/
src/config.py.bak
backtest/extreme_log_simulator.py
```

提交前建议检查：

```powershell
git status --short
git diff --cached --check
```

确认没有 API 密钥、运行日志、清理日志副本、优化结果、极端行情模拟文件或大型数据文件后再推送。
