import re

import requests
from aiohttp import ClientSession

from app.core.config import settings

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:145.0) Gecko/20100101 Firefox/145.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Origin": settings.OKC_BASE_URL,
    "DNT": "1",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Priority": "u=0, i",
    "TE": "trailers",
}


async def get_csrf(session: ClientSession) -> tuple[requests.Session, str]:
    """Получает CSRF токен со страницы авторизации.

    Returns:
        Сессия и csrf токен
    """
    session.headers.update(BASE_HEADERS)

    response = await session.get(f"{settings.OKC_BASE_URL}/site/login")
    response_text = await response.text()

    if response.status != 200:
        raise RuntimeError(f"Failed to get login page: HTTP {response.status}")

    # Достаем CSRF
    csrf_pattern = r'name=["\']_csrf["\'][^>]*value=["\']([^"\']+)["\']'
    match = re.search(csrf_pattern, response_text)

    if not match:
        raise RuntimeError("Could not find CSRF token in login page")

    csrf_token = match.group(1)
    return csrf_token


async def authenticate(username: str, password: str, session: ClientSession) -> bool:
    """Производит авторизацию и возвращает сессию.

    Args:
        username: Имя пользователя OKC
        password: Пароль пользователя OKC
        session: Сессия для авторизации

    Returns:
        Авторизованная сессия при успешном входе, иначе None
    """
    csrf_token = await get_csrf(session=session)

    session.headers.update({"Content-Type": "application/x-www-form-urlencoded"})
    session.headers.update({"Referer": settings.OKC_BASE_URL + "/site/login"})

    payload = {
        "_csrf": csrf_token,
        "LoginForm[username]": username,
        "LoginForm[password]": password,
        "login-button": "",
    }

    response = await session.post(settings.OKC_BASE_URL + "/site/login", data=payload)

    # После авторизации идет переход на другую страницу, проверяем на 302 код
    if response.status not in [200, 302]:
        raise RuntimeError(f"Login failed: HTTP {response.status}")

    return True
