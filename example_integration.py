"""
接入示例：把 OKXRealtimeProvider 接入你的以太坊交易程序
"""
import asyncio
from okx_data_provider import OKXRealtimeProvider

# ─── 你的交易逻辑写在这里 ───────────────────────────────────────────────────────

def on_tick(price: float, ts_ms: int):
    """每3秒触发一次，price 是 ETH/USDT 当前价"""
    # TODO: 接入你的程序
    pass


def on_bband(mid: float, upper: float, lower: float, price: float, ts_ms: int):
    """
    每3秒触发一次，布林带基于15min K线（20周期）实时更新
    mid   : 中轨
    upper : 上轨
    lower : 下轨
    price : 当前3s价格
    """
    # 示例：简单突破策略
    if price > upper:
        print(f"价格 {price:.2f} 突破上轨 {upper:.2f}，考虑做空/止盈")
    elif price < lower:
        print(f"价格 {price:.2f} 跌破下轨 {lower:.2f}，考虑做多/止损")
    else:
        pct_b = (price - lower) / (upper - lower) * 100
        print(f"price={price:.2f}  mid={mid:.2f}  upper={upper:.2f}  lower={lower:.2f}  %B={pct_b:.1f}%")


# ─── 启动 ───────────────────────────────────────────────────────────────────────
async def main():
    provider = OKXRealtimeProvider(
        on_tick=on_tick,
        on_bband=on_bband,
        preload_csv="ETH_USDT_15m_history.csv",  # 预加载历史数据，确保启动即有布林带
    )
    await provider.run()


if __name__ == "__main__":
    asyncio.run(main())
