from typing import Any

from aiohttp import ClientResponse, ClientSession

from app.core.config import settings


class BaseAPI:
    def __init__(self, session: ClientSession):
        self.session = session
        self.base_url = settings.OKC_BASE_URL.rstrip("/")
        self.username = settings.OKC_USERNAME
        self.password = settings.OKC_PASSWORD

        self.default_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
        }

    def _build_url(self, endpoint: str) -> str:
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        return self.base_url + endpoint

    def _merge_headers(self, custom_headers: dict[str, str] | None) -> dict[str, str]:
        return {**self.default_headers, **(custom_headers or {})}

    async def get(self, endpoint: str, **kwargs) -> ClientResponse:
        headers = self._merge_headers(kwargs.pop("headers", None))
        url = self._build_url(endpoint)
        return await self.session.get(url, headers=headers, **kwargs)

    async def post(
        self, endpoint: str, json: Any | None = None, **kwargs
    ) -> ClientResponse:
        headers = self._merge_headers(kwargs.pop("headers", None))
        url = self._build_url(endpoint)
        return await self.session.post(url, headers=headers, json=json, **kwargs)

    async def put(
        self, endpoint: str, json: Any | None = None, **kwargs
    ) -> ClientResponse:
        headers = self._merge_headers(kwargs.pop("headers", None))
        url = self._build_url(endpoint)
        return await self.session.put(url, headers=headers, json=json, **kwargs)

    async def delete(self, endpoint: str, **kwargs) -> ClientResponse:
        headers = self._merge_headers(kwargs.pop("headers", None))
        url = self._build_url(endpoint)
        return await self.session.delete(url, headers=headers, **kwargs)
