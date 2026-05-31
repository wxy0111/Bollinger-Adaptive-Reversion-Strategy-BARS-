"""
OKX ETH/USDT 数据提供程序

【历史数据说明】
  OKX API 最细历史粒度为 1min（3s K线无历史接口，任何交易所均不保存）
  本脚本提供：
    ① 1年 1min K线（价格序列，最细可用粒度）
    ② 1年 15min K线 + 布林带（20周期）
    ③ 合并CSV：每根1min K线附带对应时段的15min布林带值
    ④ 实时录制模式：从现在开始持续写入 3s 真实数据到 CSV

【实时模式】
  WebSocket 订阅 candle3s + candle15m，每3s触发回调

使用方式：
    python okx_data_provider.py --mode history    # 下载1年历史→合并CSV
    python okx_data_provider.py --mode realtime   # 实时WebSocket回调
    python okx_data_provider.py --mode record     # 持续录制3s数据到CSV
    python okx_data_provider.py --mode all        # history + realtime
"""

import asyncio
import aiohttp
import json
import time
import argparse
import csv
import os
from datetime import datetime, timedelta, timezone
from collections import deque
import numpy as np

# ─── 配置 ───────────────────────────────────────────────────────────────────────
SYMBOL     = "ETH-USDT"
BB_PERIOD  = 20
BB_STD     = 2.0
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
REST_BASE  = "https://www.okx.com"
WS_PUBLIC  = "wss://ws.okx.com:8443/ws/v5/public"
HISTORY_YEARS = 1
# ────────────────────────────────────────────────────────────────────────────────


def calc_bollinger(closes, period=BB_PERIOD, std_mult=BB_STD):
    if len(closes) < period:
        return None
    data = np.array(closes[-period:], dtype=float)
    mid  = float(np.mean(data))
    std  = float(np.std(data, ddof=0))
    return mid, mid + std_mult * std, mid - std_mult * std


# ─── REST 分页拉取 ───────────────────────────────────────────────────────────────
async def fetch_candles_page(session, bar, after_ms=None, limit=300, use_history=True):
    endpoint = "history-candles" if use_history else "candles"
    params = {"instId": SYMBOL, "bar": bar, "limit": str(limit)}
    if after_ms:
        params["after"] = str(after_ms)
    url = f"{REST_BASE}/api/v5/market/{endpoint}"
    async with session.get(url, params=params) as resp:
        data = await resp.json()
    if data.get("code") != "0":
        raise RuntimeError(f"OKX API error [{bar}]: {data.get('msg', data)}")
    return data["data"]


async def download_history(bar, years=HISTORY_YEARS, filename=None):
    """分页拉取历史K线，保存为CSV（时间升序）"""
    if filename is None:
        filename = os.path.join(OUTPUT_DIR, f"ETH_USDT_{bar}_history.csv")

    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=365 * years)).timestamp() * 1000)
    print(f"\n[{bar}] 下载历史数据 → {os.path.basename(filename)}")
    print(f"[{bar}] 范围: {datetime.fromtimestamp(cutoff_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d')} ~ 今天")

    all_rows = []
    after_ms = None

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                candles = await fetch_candles_page(session, bar, after_ms=after_ms)
            except Exception as e:
                print(f"\n[{bar}] 请求失败: {e}，3秒后重试...")
                await asyncio.sleep(3)
                continue

            if not candles:
                break

            reached_cutoff = False
            for c in candles:
                ts = int(c[0])
                if ts < cutoff_ms:
                    reached_cutoff = True
                    break
                all_rows.append(c)

            after_ms = int(candles[-1][0]) - 1
            oldest = datetime.fromtimestamp(after_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
            print(f"[{bar}] {len(all_rows):,} 根  最早: {oldest}", end="\r")

            if reached_cutoff:
                break

            await asyncio.sleep(0.12)  # OKX 限速

    all_rows.sort(key=lambda x: int(x[0]))
    print(f"\n[{bar}] 共 {len(all_rows):,} 根K线")

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "datetime_utc", "open", "high", "low", "close", "vol"])
        for c in all_rows:
            ts = int(c[0])
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([ts, dt, c[1], c[2], c[3], c[4], c[5]])

    print(f"[{bar}] ✅ 保存: {filename}")
    return filename, all_rows


def merge_1m_with_15m_bb(rows_1m, rows_15m, out_file):
    """
    合并1min价格 + 15min布林带 → 单一CSV
    每根1min K线找到它所在的15min时段，附加对应的布林带值
    """
    print(f"\n[合并] 1min价格 + 15min布林带 → {os.path.basename(out_file)}")

    # 构建15min布林带映射：ts_start → (mid, upper, lower)
    bb_map = {}
    closes_15m = []
    for c in rows_15m:
        closes_15m.append(float(c[4]))
        bb = calc_bollinger(closes_15m)
        if bb:
            bb_map[int(c[0])] = (round(bb[0], 4), round(bb[1], 4), round(bb[2], 4))

    # 15min时段对齐：给定1min时间戳，找其所在15min段的起始时间
    def align_15m(ts_ms):
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        floored = dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)
        return int(floored.timestamp() * 1000)

    written = 0
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "datetime_utc",
                         "open", "high", "low", "close", "vol",
                         "bb_mid", "bb_upper", "bb_lower"])
        for c in rows_1m:
            ts   = int(c[0])
            dt   = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            slot = align_15m(ts)
            bb   = bb_map.get(slot, ("", "", ""))
            writer.writerow([ts, dt, c[1], c[2], c[3], c[4], c[5], bb[0], bb[1], bb[2]])
            written += 1

    print(f"[合并] ✅ {written:,} 行 → {out_file}")


# ─── 实时 WebSocket ──────────────────────────────────────────────────────────────
class OKXRealtimeProvider:
    """
    实时数据提供器
    on_tick(price, ts_ms)                               每3s触发
    on_bband(mid, upper, lower, price, ts_ms)           每3s更新布林带
    preload_csv: 历史15min CSV路径，启动即有布林带窗口
    """

    def __init__(self, on_tick=None, on_bband=None, preload_csv=None):
        self.on_tick  = on_tick
        self.on_bband = on_bband
        self._15m_closes = deque(maxlen=BB_PERIOD + 50)
        self._current_price = None

        if preload_csv and os.path.exists(preload_csv):
            self._preload(preload_csv)

    def _preload(self, csv_path):
        closes = []
        with open(csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    closes.append(float(row["close"]))
                except (ValueError, KeyError):
                    pass
        for c in closes[-(BB_PERIOD + 50):]:
            self._15m_closes.append(c)
        print(f"[预加载] {len(self._15m_closes)} 根15min收盘价，布林带窗口就绪")

    def _emit_bb(self, price, ts_ms):
        bb = calc_bollinger(list(self._15m_closes))
        if bb and self.on_bband:
            self.on_bband(bb[0], bb[1], bb[2], price, ts_ms)
        return bb

    async def run(self):
        import websockets
        print(f"[WS] 连接 {WS_PUBLIC} ...")
        while True:
            try:
                async with websockets.connect(WS_PUBLIC, ping_interval=20, ping_timeout=10) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": [
                        {"channel": "candle3s",  "instId": SYMBOL},
                        {"channel": "candle15m", "instId": SYMBOL},
                    ]}))
                    print(f"[WS] ✅ 已订阅 {SYMBOL} candle3s + candle15m")
                    async for raw in ws:
                        self._handle(json.loads(raw))
            except Exception as e:
                print(f"[WS] 断线: {e}，5秒后重连...")
                await asyncio.sleep(5)

    def _handle(self, msg):
        if "event" in msg:
            return
        channel = msg.get("arg", {}).get("channel", "")
        data    = msg.get("data", [])
        if not data:
            return

        if channel == "candle3s":
            c     = data[0]
            ts_ms = int(c[0])
            price = float(c[4])
            self._current_price = price
            if self.on_tick:
                self.on_tick(price, ts_ms)
            self._emit_bb(price, ts_ms)

        elif channel == "candle15m":
            c       = data[0]
            confirm = str(c[8]) if len(c) > 8 else str(c[-1])
            if confirm == "1":
                self._15m_closes.append(float(c[4]))
            if self._current_price:
                self._emit_bb(self._current_price, int(c[0]))


# ─── 持续录制3s数据到CSV ─────────────────────────────────────────────────────────
async def record_3s(filename=None):
    """从现在开始持续录制真实3s数据，Ctrl+C停止"""
    import websockets
    if filename is None:
        filename = os.path.join(OUTPUT_DIR, "ETH_USDT_3s_recorded.csv")

    print(f"\n[录制] 开始录制3s数据 → {filename}")
    print("[录制] 按 Ctrl+C 停止\n")

    file_exists = os.path.exists(filename)
    f = open(filename, "a", newline="", encoding="utf-8")
    writer = csv.writer(f)
    if not file_exists:
        writer.writerow(["timestamp_ms", "datetime_utc", "open", "high", "low", "close", "vol"])

    count = 0
    try:
        while True:
            try:
                async with websockets.connect(WS_PUBLIC, ping_interval=20) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": [
                        {"channel": "candle3s", "instId": SYMBOL}
                    ]}))
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("arg", {}).get("channel") == "candle3s":
                            c     = msg["data"][0]
                            ts    = int(c[0])
                            dt    = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                            writer.writerow([ts, dt, c[1], c[2], c[3], c[4], c[5]])
                            f.flush()
                            count += 1
                            print(f"[录制] {count:,} 条  price={c[4]}  {dt}", end="\r")
            except Exception as e:
                print(f"\n[录制] 断线: {e}，重连...")
                await asyncio.sleep(3)
    except KeyboardInterrupt:
        f.close()
        print(f"\n[录制] 停止，共录制 {count:,} 条 → {filename}")


# ─── 默认回调（控制台输出）──────────────────────────────────────────────────────
def default_on_tick(price, ts_ms):
    dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).strftime('%H:%M:%S')
    print(f"[TICK] {dt}  price={price:.2f}")


def default_on_bband(mid, upper, lower, price, ts_ms):
    dt  = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).strftime('%H:%M:%S')
    bw  = round((upper - lower) / mid * 100, 2)
    pct = round((price - lower) / (upper - lower) * 100, 1) if upper != lower else 50
    signal = "▲突破上轨" if price > upper else ("▼跌破下轨" if price < lower else "  区间内")
    print(f"[BB]   {dt}  {signal}  price={price:.2f}  "
          f"mid={mid:.2f}  upper={upper:.2f}  lower={lower:.2f}  BW={bw}%  %B={pct}%")


# ─── 入口 ────────────────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="OKX ETH/USDT 数据提供程序")
    parser.add_argument("--mode", choices=["history", "realtime", "record", "all"],
                        default="all")
    parser.add_argument("--years", type=int, default=HISTORY_YEARS)
    args = parser.parse_args()

    csv_15m   = os.path.join(OUTPUT_DIR, "ETH_USDT_15m_history.csv")
    csv_1m    = os.path.join(OUTPUT_DIR, "ETH_USDT_1m_history.csv")
    csv_merge = os.path.join(OUTPUT_DIR, "ETH_USDT_1m_with_bb.csv")

    if args.mode in ("history", "all"):
        print("=" * 60)
        print("【历史数据下载】1年 1min + 15min K线")
        print("  注：OKX历史接口最细粒度为1min，3s历史不可获取")
        print("=" * 60)
        _, rows_15m = await download_history("15m", years=args.years, filename=csv_15m)
        _, rows_1m  = await download_history("1m",  years=args.years, filename=csv_1m)
        merge_1m_with_15m_bb(rows_1m, rows_15m, csv_merge)
        print(f"\n✅ 主文件: ETH_USDT_1m_with_bb.csv")
        print("   列: timestamp_ms, datetime_utc, open, high, low, close, vol, bb_mid, bb_upper, bb_lower")

    if args.mode == "record":
        await record_3s()

    if args.mode in ("realtime", "all"):
        print("\n" + "=" * 60)
        print("【实时模式】WebSocket 3s价格 + 15min布林带")
        print("  按 Ctrl+C 退出")
        print("=" * 60)
        preload = csv_15m if os.path.exists(csv_15m) else None
        provider = OKXRealtimeProvider(
            on_tick=default_on_tick,
            on_bband=default_on_bband,
            preload_csv=preload,
        )
        await provider.run()


if __name__ == "__main__":
    asyncio.run(main())
