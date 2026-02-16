import asyncio
from loguru import logger

from app.core.config import settings
from app.core.logging import setup_logging
from app.db.session import engine
from app.db.models import Base


async def main() -> None:
    setup_logging(settings.LOG_LEVEL, service="initdb")
    logger.info("Creating tables (idempotent)...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
