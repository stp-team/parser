from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession
from stp_database import create_engine, create_session_pool

from src.core.config import settings

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

questions_engine = create_engine(
    host=settings.DB_HOST,
    port=settings.DB_PORT,
    username=settings.DB_USER,
    password=settings.DB_PASSWORD,
    db_name=settings.DB_QUESTIONS_NAME,
)

stp_session_pool = create_session_pool(stp_engine)
stats_session_pool = create_session_pool(stats_engine)
questions_session_pool = create_session_pool(questions_engine)


@asynccontextmanager
async def get_stp_session() -> AsyncGenerator[AsyncSession, None]:
    """Get STP database session context manager"""
    async with stp_session_pool() as session:
        yield session


@asynccontextmanager
async def get_stats_session() -> AsyncGenerator[AsyncSession, None]:
    """Get Stats database session context manager"""
    async with stats_session_pool() as session:
        yield session


@asynccontextmanager
async def get_questions_session() -> AsyncGenerator[AsyncSession, None]:
    """Get Questions database session context manager"""
    async with questions_session_pool() as session:
        yield session
