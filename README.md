# OKX ETH-USDT-SWAP Bollinger Strategy

这是一个运行在 OKX `ETH-USDT-SWAP` 永续合约上的 15 分钟布林带均值回归策略程序。程序会读取 OKX 15m K 线和实时标记价格，当价格突破布林带外侧并停止继续创新极值时，按动态分批方式建立仓位，并用真实持仓均价管理止盈、止损和固本资金划转。

项目包含实盘/模拟盘交易主程序、本地网页看板、ServerChan 微信推送、固本资金管理、日志清理工具和基于运行日志的参数优化报告工具。

## 重要提醒

本程序涉及高杠杆合约交易，存在快速亏损和强平风险。默认配置使用 `OKX_FLAG=1`，即 OKX 模拟盘。正式使用前请先在模拟盘运行，确认下单、撤单、止盈、资金划转和通知都符合预期。

不要把 `.env`、运行日志、历史数据、压缩包、优化结果、真实 API 密钥或极端行情模拟文件提交到 GitHub。

## 程序结构

```text
C:\okx
├── main.py                         # 程序入口，启动本地看板和交易策略
├── start.bat                       # Windows 一键启动脚本
├── optimize_report.bat             # 手动生成日志参数优化报告
├── requirements.txt                # Python 依赖
├── .env.example                    # 环境变量示例
├── src
│   ├── config.py                   # 策略参数、账户参数、日志和看板参数
│   ├── strategy.py                 # 核心交易策略逻辑
│   ├── okx_client.py               # OKX REST API 封装
│   ├── risk.py                     # 分批计划、张数、止盈和强平估算
│   ├── position_manager.py         # 本地持仓、批次和订单状态管理
│   ├── indicators.py               # K 线整理、布林带等指标
│   ├── notify.py                   # ServerChan 微信推送
│   ├── logging_utils.py            # 终端日志颜色和类别
│   └── dashboard.py                # 本地网页看板
├── backtest
│   ├── log_parameter_optimizer.py  # 基于运行日志的参数优化报告
│   └── clean_strategy_logs.py      # 日志行情行清理和格式统一
└── logs                            # 运行日志和本地状态文件，默认不提交
```

## 运行方式

1. 复制 `.env.example` 为 `.env`，填写 OKX API、ServerChan 等配置。
2. 确认 `src/config.py` 中的策略参数。
3. 双击或运行：

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

程序会在策略触发的开仓挂单、开仓成交、补仓挂单、补仓成交、平仓、资金不足、资金恢复和强平风险事件中发送通知。强平预警只在距离强平价 `10U` 内发送，且最多每 1 小时发送一次。

## 行情采样和交易节奏

当前设置：

```python
PRICE_LOG_INTERVAL = 1
POLL_INTERVAL = 3
```

- 每 `1s` 记录一次价格和布林带快照到日志文件，用于后续更高精度评估。
- 每 `3s` 执行一次交易主逻辑，包括余额检查、成交同步、持仓检查、开仓、补仓、撤单、重挂、止盈和资金管理。
- 终端按主逻辑节奏显示行情和策略判断，避免 1s 行情刷屏。

## 布林带和入场过滤

当前核心参数：

```python
BOLL_PERIOD = 20
BOLL_STD = 2.0
BOLL_INCLUDE_CURRENT = True
MIN_BOLL_WIDTH_USD = 15
MIN_BOLL_WIDTH_PCT = 0.006
BOLL_WIDTH_BASE_PRICE = 2000.0
BOLL_WIDTH_BASE_USD = 15.0
MIN_BOLL_WIDTH_FLOOR_USD = 10.0
BOLL_WIDTH_GAP_MULT = 2.5
```

`BOLL_INCLUDE_CURRENT=True` 表示布林带会包含当前未收盘 K 线，因此布林带会随盘中价格动态变化。

布林宽度使用动态阈值：以 `2000 USDT` 价格对应 `15U` 布林宽度为基准，价格变化时按比例调整，同时结合当前有效补仓间距乘以 `BOLL_WIDTH_GAP_MULT`，取更严格的阈值。

## 开仓逻辑

当没有持仓、也没有正在工作的入场挂单时，程序判断头仓条件：

```text
价格突破布林带外
+ 布林带宽度满足动态阈值
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

同一根 15m K 线最多新增一笔入场批次。头仓成交后，本根 K 线不继续补仓，等待下一根 K 线重新判断。

平仓完成后，平仓所在的这根 15m K 线不再开新的头仓。程序会把最近平仓 K 线记录到 `logs/close_cooldown.json`，所以平仓后如果立刻重启，只要仍在同一根 K 线内，也会继续等待下一根 K 线再允许开仓。

## 补仓逻辑

补仓不再依赖旧的 `BATCH_SPACING` 固定间距，而是按当前触发价格和最近批次真实成交价动态判断。

```text
已有持仓
+ 当前没有未成交补仓单
+ 再次触发同方向布林带外
+ 布林带宽度满足动态阈值
+ 价格不再继续创新高/新低
+ 当前 K 线没有新增过入场批次
+ 和上一批真实成交价距离 >= 有效入场间距
=> 按当前 mark_price 挂下一批限价单
```

基础入场间距：

```python
MIN_ENTRY_GAP_USD = 4
MIN_HEAD_LIQ_BUFFER_PCT = 0.03
DYNAMIC_ENTRY_GAP_ENABLED = True
DYNAMIC_ENTRY_GAP_MAX_USD = 40.0
```

有效入场间距会根据头仓价格估算：如果按最小补仓一路补到 `MAX_TOTAL_ENTRY_RATIO` 后，头仓到预估强平价的距离不足 `3%`，程序会自动提高补仓间距。

## 动态分批张数

当前策略使用动态补仓比例：

```python
FIRST_BATCH_RATIO = 0.15
SECOND_BATCH_RATIO = 0.15
DYNAMIC_BASE_ENTRY_RATIO = 0.10
DYNAMIC_MIN_ENTRY_RATIO = 0.05
DYNAMIC_MAX_ENTRY_RATIO = 0.15
MAX_TOTAL_ENTRY_RATIO = 0.80
MAX_ENTRY_BATCHES = 12
```

- 第 1 批头仓使用目标交易资金的 `15%` 保证金。
- 第 2 批补仓使用目标交易资金的 `15%` 保证金。
- 第 3 批及之后，根据“最近两批成交价差”和“当前触发价到上一批成交价的价差”动态调整比例。
- 单次动态补仓比例限制在 `5%` 到 `15%` 之间。
- 全部入场批次占用保证金最多不超过目标交易资金的 `80%`。
- 如果超过 `80%` 上限或有效可用资金不足，程序会放弃本次补仓，不会缩小比例强行补。

旧参数 `BATCH_COUNT`、`BATCH_SIZE_RATIO` 和 `BATCH_SPACING` 仅作为历史保留/兼容，不再作为当前实时补仓模型的核心依据。

## 挂单维护

未成交头仓或补仓单由 K 线更新和价格阈值维护：

```text
K 线更新
+ 仍满足同方向轨外条件
+ 新挂单价和旧挂单价差距 >= REPRICE_GAP_USD
=> 撤旧单，按新价格重挂
```

当前重挂阈值：

```python
REPRICE_GAP_USD = 0.5
```

如果挂单后布林带宽度低于阈值，会撤销未成交入场单。程序不再使用 45 秒未成交自动撤单逻辑。

## 止盈、动态锁盈和止损

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
DYNAMIC_TP_ARM_RETURN = 0.235
DYNAMIC_TP_RESTORE_RETURN = 0.22
DYNAMIC_TP_REPRICE_GAP_USD = 0.5
```

逻辑：

```text
默认挂 25% 动态止盈
浮盈达到 23.5% 后开始观察
多单如果不再创新高，或空单如果不再创新低：
    撤原止盈单
    按当前实时价格挂 reduce-only 止盈单
如果实时价止盈未成交，且浮盈回落到 22% 以下：
    撤实时价止盈
    恢复 25% 动态止盈
```

每次有新批次成交后，程序会退出动态锁盈状态，按新的交易所真实均价重新计算 25% 止盈，并重挂 reduce-only 止盈单。

强平线作为最终风险边界，程序会挂条件止损单。

```python
LIQ_STOP_OFFSET_USD = 0.1
LIQ_WARNING_DISTANCE_USD = 10.0
LIQ_WARNING_REPEAT_SEC = 3600
```

## 固本资金管理

策略采用固定交易账户可用资金的方式：

```python
TRADING_ACCOUNT_TARGET = 200.0
```

平仓后程序会检查交易账户 USDT 可用余额：

```text
交易账户可用余额 > 目标值：
    将多出的利润从交易账户划转到资金账户

交易账户可用余额 < 目标值：
    尝试从资金账户补回交易账户
```

如果资金账户不足以补回目标值，程序会发送微信通知，并暂停新开仓和新补仓，但继续记录行情、同步持仓和管理已有订单。之后如果你补充资金并且交易账户余额恢复到目标值，程序会发送恢复通知并继续运行。若补充后交易账户余额超过目标值，超过部分会划转回资金账户。

## 本地看板

看板地址：

```text
http://localhost:8080
```

看板只读取本地运行状态和日志，不负责下单。历史日志页支持读取 `logs/boll_pin_*.log`，显示价格、布林带、关键交易点、单日开单情况、实际固本收益、估算收益和累计收益。

历史实际收益优先来自固本划转日志，例如：

```text
[Capital] Profit +13.1408 USDT
```

如果没有固本划转记录，才退回使用真实成交批次、最近一次止盈挂单价格和 `Position closed` 记录估算收益。

## 日志清理和参数优化

手动运行参数优化：

```powershell
optimize_report.bat
```

或直接运行：

```powershell
python backtest\log_parameter_optimizer.py
```

优化器默认读取：

```text
logs/boll_pin_*.log
```

默认 `--sample-sec 0`，表示使用所有解析到的 tick，不做重采样。由于当前日志中混有 1s 和 3s 行情，建议需要更保守、接近 3s 主交易循环的评估时使用：

```powershell
python backtest\log_parameter_optimizer.py --sample-sec 3
```

如果想把中英文混合的行情行统一成英文格式，先生成清理副本：

```powershell
python backtest\clean_strategy_logs.py --log-dir logs --out-dir logs_cleaned
python backtest\log_parameter_optimizer.py --log-dir logs_cleaned --sample-sec 3
```

`logs_cleaned/` 是生成的清理副本，默认不提交到 GitHub。

当前优化重点包括：

```text
BOLL_STD
MIN_BOLL_WIDTH_USD
MIN_ENTRY_GAP_USD
REPRICE_GAP_USD
FIRST_BATCH_RATIO
SECOND_BATCH_RATIO
DYNAMIC_BASE_ENTRY_RATIO
DYNAMIC_MIN_ENTRY_RATIO
DYNAMIC_MAX_ENTRY_RATIO
MAX_TOTAL_ENTRY_RATIO
BOLL_WIDTH_BASE_USD
BOLL_WIDTH_GAP_MULT
TP_TARGET_MARGIN_RETURN
DYNAMIC_TP_ARM_RETURN
DYNAMIC_TP_RESTORE_RETURN
MIN_HEAD_LIQ_BUFFER_PCT
```

报告跑完后，终端会出现参数同步菜单。同步前需要再次输入确认，脚本会先生成 `src/config.py.bak` 备份。

## 常用参数

主要参数集中在 `src/config.py`：

```python
INST_ID = "ETH-USDT-SWAP"
LEVER = 50

PRICE_LOG_INTERVAL = 1
POLL_INTERVAL = 3

BOLL_PERIOD = 20
BOLL_STD = 2.0
BOLL_INCLUDE_CURRENT = True

MIN_BOLL_WIDTH_USD = 15
MIN_BOLL_WIDTH_PCT = 0.006
BOLL_WIDTH_BASE_PRICE = 2000.0
BOLL_WIDTH_BASE_USD = 15.0
BOLL_WIDTH_GAP_MULT = 2.5

MIN_ENTRY_GAP_USD = 4
REPRICE_GAP_USD = 0.5

FIRST_BATCH_RATIO = 0.15
SECOND_BATCH_RATIO = 0.15
DYNAMIC_BASE_ENTRY_RATIO = 0.10
DYNAMIC_MIN_ENTRY_RATIO = 0.05
DYNAMIC_MAX_ENTRY_RATIO = 0.15
MAX_TOTAL_ENTRY_RATIO = 0.80

TP_TARGET_MARGIN_RETURN = 0.25
DYNAMIC_TP_ARM_RETURN = 0.235
DYNAMIC_TP_RESTORE_RETURN = 0.22

TRADING_ACCOUNT_TARGET = 200.0
```

## GitHub 注意事项

`.gitignore` 默认排除：

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
