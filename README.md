# OKX ETH-USDT-SWAP Bollinger Strategy

这是一个运行在 OKX `ETH-USDT-SWAP` 永续合约上的 15 分钟布林带均值回归策略程序。程序会读取 OKX 15m K 线和标记价格，当价格突破布林带外侧并停止继续创新极值时，按分批方式建立仓位，并以真实持仓均价外固定 `10 USDT` 距离挂止盈单。

项目包含实盘/模拟盘交易主程序、本地网页看板、ServerChan 微信推送、固本资金管理和基于运行日志的参数优化报告工具。

## 重要提醒

本程序涉及高杠杆合约交易，存在快速亏损和强平风险。默认配置使用 `OKX_FLAG=1`，即 OKX 模拟盘。正式使用前请先在模拟盘运行，确认下单、撤单、止盈、资金划转和通知都符合预期。

不要把 `.env`、运行日志、历史数据、压缩包或真实 API 密钥提交到 GitHub。

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
│   └── log_parameter_optimizer.py  # 基于运行日志的参数优化报告
└── logs                            # 运行日志和本地状态文件，默认不提交
```

## 运行原理

### 行情采样和交易节奏

程序现在把“行情记录”和“交易主逻辑”分开：

```python
PRICE_LOG_INTERVAL = 1
POLL_INTERVAL = 3
```

- 每 `1s` 记录一次价格和布林带快照到日志文件，便于后续用更高精度的历史日志做评估。
- 每 `3s` 执行一次交易主逻辑，包括余额检查、成交同步、持仓检查、开仓、补仓、撤单、重挂、止盈和资金管理。
- 终端只显示每 `3s` 的主逻辑行情和策略判断，避免 1s 行情刷屏。

### 布林带

当前布林带参数：

```python
BOLL_PERIOD = 20
BOLL_STD = 2
BOLL_INCLUDE_CURRENT = True
```

`BOLL_INCLUDE_CURRENT=True` 表示布林带会包含当前未收盘 K 线，因此布林带会随盘中价格动态变化。

当前入场宽度过滤：

```python
MIN_BOLL_WIDTH_USD = 15
MIN_BOLL_WIDTH_PCT = 0.006
```

实际要求是：

```text
布林带宽度 >= 15 USDT
且
布林带宽度 / 当前价格 >= 0.6%
```

`_boll_width_ok()` 只做纯判断，不直接打印日志。具体场景会打印更明确的原因，例如：

```text
CHECK | 开仓跳过：布林宽度不足 width=14.30 < 15.00 width_pct=0.72% threshold=0.60%
CHECK | 补仓跳过：布林宽度不足 width=14.30 < 15.00 width_pct=0.72% threshold=0.60%
```

## 开仓逻辑

当没有持仓、也没有正在工作的入场挂单时，程序判断是否满足头仓条件：

```text
价格突破布林带外
+ 布林带宽度满足要求
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

## 补仓逻辑

头仓成交后，后续补仓不再依赖旧的 `BATCH_SPACING` 固定间距，而是按当前触发价格和最近批次成交价动态判断。

补仓条件：

```text
已有持仓
+ 当前没有未成交补仓单
+ 再次触发同方向布林带外
+ 布林带宽度满足要求
+ 价格不再继续创新高/新低
+ 当前 K 线没有新增过入场批次
+ 和上一批真实成交价距离 >= MIN_ENTRY_GAP_USD
=> 按当前 mark_price 挂下一批限价单
```

当前补仓最小间距：

```python
MIN_ENTRY_GAP_USD = 3
```

做多时，下一批价格必须低于或等于上一批真实成交价，并至少相差 `3U`。做空时相反。

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

含义：

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

## 止盈和风控

程序会根据交易所真实持仓均价重新计算止盈价：

```python
TP_PROFIT_USD = 10.0
```

做多：

```text
止盈价 = 真实平均成本 + 10U
```

做空：

```text
止盈价 = 真实平均成本 - 10U
```

每次有新批次成交后，程序会撤掉旧止盈单，并重新挂 reduce-only 止盈单。

强平线作为最终风险边界，程序会挂条件止损单。强平预警只在距离强平价 `10U` 内发送，且最多每 1 小时发送一次：

```python
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
    从资金账户划转补足交易账户
```

OKX 账户编号：

```text
6  = 资金账户
18 = 交易账户
```

如果资金账户不足以补足交易账户，程序会：

- 发送微信通知。
- 暂停新开仓和新补仓。
- 继续记录行情、布林带和持仓信息。
- 持续检查交易账户余额。
- 当补充资金后交易账户恢复到目标值，会发送恢复通知。
- 如果补充后超过目标值，多余部分会划转回资金账户。

`TRADING_ACCOUNT_TARGET = 0` 时关闭固本资金管理。

## 终端日志设计

终端按重要性分层显示：

```text
MARKET  白色  每 3s 显示一次主逻辑行情
CHECK   黄色  策略判断，例如宽度不足、间距不足、创新低暂不挂单
ACTION  红色  真实操作，例如下单、撤单、止盈、止损、平仓、资金划转
```

同时，日志文件会每 `1s` 记录行情快照：

```text
price=1981.23  Boll[1975.78 | 1982.52 | 1989.25]  position=long  equity=167.17
```

每 `3s` 的主逻辑行情会显示在终端：

```text
MARKET | 价格=1981.12  布林[1975.78 | 1982.52 | 1989.25]  持仓=long  权益=167.17
```

这样可以兼顾：

- 日志文件保留 1s 精度，方便后续评估采样频率对收益的影响。
- 终端不被 1s 行情刷屏，只显示 3s 主逻辑和重要操作。

## 微信推送

程序使用 ServerChan 进行微信推送。当前会推送：

```text
程序挂出头仓单
程序挂出补仓单
头仓/补仓实际成交
平仓完成
资金不足
资金恢复
强平距离预警
最大回撤触发
```

如果没有配置 `SERVERCHAN_KEY`，程序会静默跳过推送，不影响交易。

## 安装方法

建议使用 Python 虚拟环境：

```powershell
cd C:\okx
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 配置方法

复制 `.env.example` 为 `.env`：

```powershell
Copy-Item .env.example .env
```

编辑 `.env`：

```env
OKX_API_KEY=your_api_key_here
OKX_SECRET_KEY=your_secret_key_here
OKX_PASSPHRASE=your_passphrase_here
OKX_FLAG=1
SERVERCHAN_KEY=your_serverchan_key_here
```

### 获取 SERVERCHAN_KEY

`SERVERCHAN_KEY` 用于把开仓、加仓、平仓和风险事件推送到微信。当前程序使用的是 Server 酱新版接口：

```text
https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send
```

获取方法：

1. 打开 Server 酱官网：[https://sct.ftqq.com/](https://sct.ftqq.com/)
2. 使用微信扫码登录。
3. 按页面提示绑定消息通道。一般选择默认的微信/方糖服务号通道即可。
4. 在官网后台找到 `SendKey` 页面。
5. 复制你的 `SendKey`，填入 `.env`：

```env
SERVERCHAN_KEY=你的SendKey
```

注意：`SendKey` 等同于推送密钥，不要提交到 GitHub，也不要发给别人。如果不需要微信推送，可以留空：

```env
SERVERCHAN_KEY=
```

说明：

```text
OKX_FLAG=1  模拟盘
OKX_FLAG=0  实盘
```

补充：如果你使用的是 Server 酱 3，它的入口通常是 [https://sc3.ft07.com](https://sc3.ft07.com)，但当前程序默认适配的是 `sct.ftqq.com` 这一版的 `SendKey`。

## 启动方法

方式一：直接运行 Python。

```powershell
cd C:\okx
.\.venv\Scripts\python.exe main.py
```

方式二：双击运行：

```text
start.bat
```

启动后，本地看板地址：

```text
http://localhost:8080
```

运行日志：

```text
logs/boll_pin_YYYY-MM-DD.log
```

本地策略状态：

```text
logs/runtime_state.json
```

## 本地看板

看板包含：

- 实时价格、布林带、权益、持仓、止盈、强平价。
- 当前批次和最近成交。
- 历史日志读取与价格/布林带展示。

看板只读取本地运行状态和日志，不负责下单。

## 日志参数优化报告

手动运行：

```powershell
.\.venv\Scripts\python.exe backtest\log_parameter_optimizer.py
```

也可以直接运行：

```text
optimize_report.bat
```

优化脚本会读取 `logs/boll_pin_*.log`，用历史运行日志回放多组参数，输出 CSV 和 Markdown 报告到：

```text
backtest/results/log_parameter_optimizer/
```

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
```

报告跑完后，终端会出现参数同步菜单：

```text
输入 1-20：把对应排名的参数写入 src/config.py
输入 0 或直接回车：保持当前策略参数不变
```

同步前需要再次输入 `y` 确认。脚本会先生成 `src/config.py.bak` 备份。

## 常用参数

主要参数集中在 `src/config.py`：

```python
INST_ID = "ETH-USDT-SWAP"
LEVER = 50

PRICE_LOG_INTERVAL = 1
POLL_INTERVAL = 3

BOLL_PERIOD = 20
BOLL_STD = 2
BOLL_INCLUDE_CURRENT = True
MIN_BOLL_WIDTH_USD = 15
MIN_BOLL_WIDTH_PCT = 0.006
NO_NEW_EXTREME_TICKS = 2

MIN_ENTRY_GAP_USD = 3
REPRICE_GAP_USD = 0.5

FIRST_BATCH_RATIO = 0.15
SECOND_BATCH_RATIO = 0.15
DYNAMIC_BASE_ENTRY_RATIO = 0.1
DYNAMIC_MIN_ENTRY_RATIO = 0.05
DYNAMIC_MAX_ENTRY_RATIO = 0.15
MAX_TOTAL_ENTRY_RATIO = 0.8
MAX_ENTRY_BATCHES = 12

TP_PROFIT_USD = 10.0
TRADING_ACCOUNT_TARGET = 200.0
```

## GitHub 注意事项

`.gitignore` 默认排除：

```text
.env
logs/
.idea/
*.zip
backtest/results/
backtest/results_current_check/
```

提交前建议检查：

```powershell
git status --short
git diff --cached --check
```

确认没有 API 密钥、日志、大型数据文件后再推送。

## 当前策略特点

```text
1s 文件行情采样：用于积累更高精度日志
3s 交易主逻辑：避免账户和订单接口过度请求
低波动过滤：布林带宽度太窄不建仓/不补仓
轨外均值回归：突破上下轨后等待不再创新极值再进场
动态补仓：后续补仓按价差动态调整比例
同 K 限制：每根 15m K 线最多新增一批
固定止盈：按真实平均成本外 10U 止盈
固本策略：平仓后保持交易账户目标可用资金，多余利润转资金账户
资金不足保护：资金不足时暂停新入场，继续记录行情
微信通知：挂单、成交、平仓和风险事件可通知
彩色终端：MARKET/CHECK/ACTION 分层显示
```
