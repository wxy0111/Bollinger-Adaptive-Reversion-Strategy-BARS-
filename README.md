# OKX ETH-USDT-SWAP Bollinger Strategy

这是一个运行在 OKX ETH-USDT 永续合约上的 15 分钟布林带均值回归策略程序。程序会监听标记价格和 15m K 线布林带，当价格突破布林带外侧并停止继续创新极值时，按分批方式建立仓位，并在平均成本外固定 10 USDT 距离挂止盈单。

项目包含实盘/模拟盘交易主程序、本地网页看板、微信推送、固本资金管理和日志参数报告工具。

## 重要提醒

本程序涉及高杠杆合约交易，存在快速亏损和强平风险。默认配置使用 `OKX_FLAG=1`，即 OKX 模拟盘。正式使用前请先在模拟盘运行，确认下单、撤单、止盈、资金划转和通知都符合预期。

请不要把 `.env`、日志、历史数据、压缩包或真实 API 密钥提交到 GitHub。

## 程序结构

```text
C:\okx
├── main.py                         # 程序入口，启动网页看板和交易策略
├── start.bat                       # Windows 一键启动脚本
├── optimize_report.bat             # 手动生成日志参数报告
├── requirements.txt                # Python 依赖
├── .env.example                    # 环境变量示例
├── src
│   ├── config.py                   # 策略参数、账户参数、看板参数
│   ├── strategy.py                 # 核心交易策略逻辑
│   ├── okx_client.py               # OKX API 封装
│   ├── risk.py                     # 分批计划、张数、止盈和强平估算
│   ├── position_manager.py         # 本地持仓和批次状态管理
│   ├── indicators.py               # K 线整理、布林带等指标
│   ├── notify.py                   # ServerChan 微信推送
│   └── dashboard.py                # 本地网页看板
├── backtest
│   └── log_parameter_optimizer.py  # 基于运行日志的参数报告
└── logs                            # 运行日志和本地状态文件，默认不提交
```

## 运行原理

### 1. 行情与指标

程序每 `POLL_INTERVAL = 3` 秒执行一次主循环：

1. 从 OKX 拉取 ETH-USDT-SWAP 的 15m K 线。
2. 计算 20 周期、2 倍标准差布林带。
3. 读取当前标记价格、账户余额、持仓和挂单状态。
4. 根据策略状态决定是否开仓、补仓、重挂、止盈或资金再平衡。

当前配置中：

```python
BOLL_PERIOD = 20
BOLL_STD = 2.0
BOLL_INCLUDE_CURRENT = True
POLL_INTERVAL = 3
```

`BOLL_INCLUDE_CURRENT=True` 表示布林带会包含当前未收盘 K 线，因此布林带会随盘中价格变化。

### 2. 开仓逻辑

当没有持仓、也没有正在工作的挂单时，程序会判断是否满足头仓条件：

```text
价格突破布林带外
+ 布林带宽度满足要求
+ 价格不再继续创新高/新低
+ 当前 K 线没有开过一套计划
+ 入场价和上一套计划价格距离足够
=> 挂第 1 批头仓限价单
```

方向判断：

```text
价格 < 布林下轨 => 做多
价格 > 布林上轨 => 做空
```

当前布林带宽度过滤：

```python
MIN_BOLL_WIDTH_USD = 15
MIN_BOLL_WIDTH_PCT = 0.006
```

实际生效门槛是：

```text
布林带宽度 >= max(15 USDT, 当前价格 * 0.006)
```

### 3. 加仓逻辑

头仓成交后，后续补仓不再依赖 `BATCH_SPACING` 计算固定补仓位置，而是按当前触发价格补仓。

补仓条件：

```text
已有持仓
+ 当前没有未成交补仓单
+ 再次触发同方向布林带外
+ 布林带宽度满足要求
+ 价格不再继续创新高/新低
+ 当前 K 线没有新增过入场批次
+ 和上一批成交价距离 >= MIN_ENTRY_GAP_USD
=> 按当前 mark_price 挂下一批限价单
```

当前补仓最小间距：

```python
MIN_ENTRY_GAP_USD = 4.0
```

也就是：

```text
做多：下一批价格必须低于或等于上一批成交价，并至少相差 4U
做空：下一批价格必须高于或等于上一批成交价，并至少相差 4U
```

同一根 K 线只能新增一批。头仓成交后，本根 K 线不会继续补仓。

### 4. 挂单重挂逻辑

未成交的头仓或补仓单不会因为 45 秒超时直接撤单。当前维护方式是：

```text
K 线更新
+ 仍满足同方向轨外条件
+ 新挂单价和旧挂单价差距 >= REPRICE_GAP_USD
=> 撤旧单，按新价格重挂
```

当前重挂阈值：

```python
REPRICE_GAP_USD = 1.0
```

如果挂单连续回到布林带内，程序会等待指定 K 线数量后撤单：

```python
INSIDE_BAND_CANCEL_KLINES = 2
```

### 5. 止盈和平仓

程序会根据交易所真实持仓均价重新计算止盈价：

```python
TP_PROFIT_USD = 10.0
```

做多：

```text
止盈价 = 平均成本 + 10U
```

做空：

```text
止盈价 = 平均成本 - 10U
```

每次有新批次成交后，程序会撤旧止盈单并重新挂新的 reduce-only 止盈单。

### 6. 固本资金管理

策略采用固定交易账户资金的思路：

```python
TRADING_ACCOUNT_TARGET = 200.0
```

平仓后程序会检查交易账户 USDT 可用余额：

```text
交易账户余额 > 200U：
    多出来的利润从交易账户划转到资金账户

交易账户余额 < 200U：
    从资金账户划转补足交易账户
```

OKX 账户编号：

```text
6  = 资金账户
18 = 交易账户
```

如果 `TRADING_ACCOUNT_TARGET = 0`，则关闭固本资金管理。

### 7. 微信推送

程序使用 ServerChan 进行微信推送。当前会推送：

```text
程序成功挂出头仓单
程序成功挂出加仓单
头仓/加仓实际成交
普通止盈/平仓
最大回撤触发紧急平仓
强平距离预警
最大回撤预警
```

如果没有配置 `SERVERCHAN_KEY`，程序会静默跳过推送，不影响交易。

## 安装方法

建议使用 Python 虚拟环境。

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

然后编辑 `.env`：

```env
OKX_API_KEY=your_api_key_here
OKX_SECRET_KEY=your_secret_key_here
OKX_PASSPHRASE=your_passphrase_here
OKX_FLAG=1
SERVERCHAN_KEY=your_serverchan_key_here
```

说明：

```text
OKX_FLAG=1  模拟盘
OKX_FLAG=0  实盘
```

`SERVERCHAN_KEY` 是可选项，不需要微信推送可以不填。

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

看板包含两个视图：

```text
实时：查看当前价格、布林带、账户、持仓、批次和最近成交
历史日志：读取 logs/boll_pin_*.log，查看历史价格和布林带变化
```

日志会写入：

```text
logs/boll_pin_YYYY-MM-DD.log
```

策略状态会保存在：

```text
logs/runtime_state.json
```

## 常用参数

主要参数集中在 `src/config.py`。

```python
INST_ID = "ETH-USDT-SWAP"
LEVER = 50
POLL_INTERVAL = 3

MIN_BOLL_WIDTH_USD = 15
MIN_BOLL_WIDTH_PCT = 0.006
NO_NEW_EXTREME_TICKS = 2

BATCH_COUNT = 4
BATCH_SIZE_RATIO = [0.2, 0.25, 0.25, 0.25]
MIN_ENTRY_GAP_USD = 4.0
REPRICE_GAP_USD = 1.0

TP_PROFIT_USD = 10.0
TRADING_ACCOUNT_TARGET = 200.0
```

参数含义：

```text
MIN_BOLL_WIDTH_USD      布林带绝对宽度过滤
MIN_BOLL_WIDTH_PCT      布林带百分比宽度过滤
NO_NEW_EXTREME_TICKS    防止价格仍在连续创新高/新低时进场
BATCH_COUNT             最大分批数量
BATCH_SIZE_RATIO        每批使用的资金比例
MIN_ENTRY_GAP_USD       补仓和上一批成交价的最小距离
REPRICE_GAP_USD         未成交挂单重挂阈值
TP_PROFIT_USD           平均成本外固定止盈距离
TRADING_ACCOUNT_TARGET  固本交易账户目标余额
```

## 日志参数报告

日志参数报告：

```powershell
.\.venv\Scripts\python.exe backtest\log_parameter_optimizer.py
```

也可以直接运行：

```text
optimize_report.bat
```

这个脚本会读取 `logs/boll_pin_*.log`，用所有历史日志测试多组开仓参数，并输出 CSV 和 Markdown 报告到 `backtest/results/log_parameter_optimizer/`。

报告跑完后，终端会显示参数同步菜单：

```text
输入 1-20：把对应排名的参数写入 src/config.py
输入 0 或直接回车：保持当前策略参数不变
```

同步前需要再次输入 `y` 确认，脚本会先生成 `src/config.py.bak` 备份。

## 上传 GitHub 时的注意事项

`.gitignore` 已默认排除：

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
低波动行情过滤：布林带宽度太窄不建仓
轨外均值回归：突破上下轨后等待不再创新极值再进场
动态补仓：补仓按当前触发价格，不再依赖固定 BATCH_SPACING
同 K 限制：每根 15m K 线最多新增一批
固定止盈：按真实平均成本外 10U 止盈
固本策略：平仓后保持交易账户约 200U，多余利润转资金账户
微信通知：挂单、成交、平仓和风险事件均可通知
```
