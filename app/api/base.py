import requests
from requests import Response

from app.core.config import settings


class BaseAPI:
    def __init__(self, session):
        self.session: requests.Session = session
        self.base_url: str = settings.OKC_BASE_URL
        self.username: str = settings.OKC_USERNAME
        self.password: str = settings.OKC_PASSWORD

    def get(self, endpoint: str, **kwargs) -> Response:
        response = self.session.get(
            url=f"{self.base_url}{endpoint}",
            **kwargs,
        )
        return response

    def post(self, endpoint: str, **kwargs) -> Response:
        response = self.session.post(
            url=f"{self.base_url}{endpoint}",
            **kwargs,
        )
        return response
