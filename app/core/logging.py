from __future__ import annotations

import sys
from loguru import logger


def setup_logging(level: str = "INFO", service: str = "unknown"):
    """
    结构化日志（JSON），便于后续接入 Loki/ELK。
    每个服务进程独立运行，因此无需按 service 过滤。
    """
    logger.remove()
    logger.add(
        sys.stdout,
        level=level,
        serialize=True,
        backtrace=False,
        diagnose=False,
        enqueue=True,
    )
    return logger.bind(service=service)
