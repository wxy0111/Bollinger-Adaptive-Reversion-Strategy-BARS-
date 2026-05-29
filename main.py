"""入口：同时启动网页看板和交易策略。"""
import asyncio
import sys
from loguru import logger
from src.strategy import BollPinStrategy
from src.dashboard import start_dashboard

logger.remove()
logger.add(
    sys.stdout,
    format="{time:HH:mm:ss} | {level:<8} | {message}",
    level="INFO",
    colorize=True,
)
logger.add(
    "logs/boll_pin_{time:YYYY-MM-DD}.log",
    rotation="00:00",
    retention="30 days",
    level="DEBUG",
    encoding="utf-8",
)


async def main():
    await start_dashboard()          # 先启动看板
    strategy = BollPinStrategy()
    try:
        await strategy.run()
    except KeyboardInterrupt:
        strategy.stop()
        logger.info("用户中断，退出")


if __name__ == "__main__":
    asyncio.run(main())
