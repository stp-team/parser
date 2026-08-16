from redis.asyncio import Redis

from src.core.config import settings


_redis_client: Redis | None = None


def _build_redis_url() -> str:
    if settings.REDIS_PASSWORD:
        return (
            f"redis://:{settings.REDIS_PASSWORD}"
            f"@{settings.REDIS_HOST}:"
            f"{settings.REDIS_PORT}/"
            f"{settings.REDIS_DB}"
        )

    return (
        f"redis://{settings.REDIS_HOST}:"
        f"{settings.REDIS_PORT}/"
        f"{settings.REDIS_DB}"
    )


async def init_redis() -> Redis:
    global _redis_client

    if _redis_client is None:
        _redis_client = Redis.from_url(
            _build_redis_url(),
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=5,
            socket_timeout=15,
            health_check_interval=30,
        )

        await _redis_client.ping()

    return _redis_client


async def get_redis() -> Redis:
    if _redis_client is None:
        return await init_redis()

    return _redis_client


async def close_redis() -> None:
    global _redis_client

    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None