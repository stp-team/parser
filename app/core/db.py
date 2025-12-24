from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from stp_database import create_engine, create_session_pool
from stp_database.repo.STP import MainRequestsRepo

from app.core.config import settings

# Database engines
stp_engine = create_engine(
    host=settings.DB_HOST,
    port=settings.DB_PORT,
    username=settings.DB_USER,
    password=settings.DB_PASSWORD,
    db_name=settings.DB_STP_NAME,
)

stats_engine = create_engine(
    host=settings.DB_HOST,
    port=settings.DB_PORT,
    username=settings.DB_USER,
    password=settings.DB_PASSWORD,
    db_name=settings.DB_STATS_NAME,
)

stp_session_pool = create_session_pool(stp_engine)
stats_session_pool = create_session_pool(stats_engine)


@asynccontextmanager
async def get_stp_session() -> AsyncGenerator[AsyncSession, None]:
    """Get STP database session context manager with NON-BLOCKING isolation"""
    async with stp_session_pool() as session:
        # Устанавливаем NON-BLOCKING isolation level
        await session.execute(
            text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        )
        await session.execute(text("SET SESSION innodb_lock_wait_timeout = 1"))
        yield session


@asynccontextmanager
async def get_stats_session() -> AsyncGenerator[AsyncSession, None]:
    """Get Stats database session context manager with NON-BLOCKING isolation"""
    async with stats_session_pool() as session:
        # Устанавливаем NON-BLOCKING isolation level
        await session.execute(
            text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        )
        await session.execute(text("SET SESSION innodb_lock_wait_timeout = 1"))
        yield session


@asynccontextmanager
async def get_stp_repo() -> AsyncGenerator[MainRequestsRepo, None]:
    """Get STP repository as proper async context manager"""
    async with stp_session_pool() as session:
        yield MainRequestsRepo(session)
