"""Loguru helpers for readable terminal categories."""

from loguru import logger


def setup_log_levels() -> None:
    """Register custom log levels used by the trading terminal."""
    for name, level_no, color in (
        ("MARKET", 20, "<white>"),
        ("CHECK", 21, "<yellow>"),
        ("ACTION", 22, "<red>"),
    ):
        try:
            logger.level(name)
        except ValueError:
            logger.level(name, no=level_no, color=color)


def log_market(message: str, *args, terminal: bool = False, **kwargs) -> None:
    """Log passive market snapshots."""
    setup_log_levels()
    logger.bind(terminal=terminal).log("MARKET", message, *args, **kwargs)


def log_check(message: str, *args, **kwargs) -> None:
    """Log strategy checks and decisions."""
    setup_log_levels()
    logger.log("CHECK", message, *args, **kwargs)


def log_action(message: str, *args, **kwargs) -> None:
    """Log real exchange actions such as orders and transfers."""
    setup_log_levels()
    logger.log("ACTION", message, *args, **kwargs)
