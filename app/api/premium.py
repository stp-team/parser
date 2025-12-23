import logging

from app.api.base import BaseAPI
from app.models.premium import HeadPremiumResponse, SpecialistPremiumResponse

logger = logging.getLogger(__name__)


class PremiumAPI(BaseAPI):
    def __init__(self, session):
        super().__init__(session)
        self.service_url = "premium"
        self.logger = logging.getLogger(self.__class__.__name__)

    async def get_specialist_premium(
        self,
        period: str,
        subdivision_id=None,
        heads_id=None,
        employees_id=None,
        division: str = "НТП1",
    ) -> SpecialistPremiumResponse | None:
        if employees_id is None:
            employees_id = []
        if heads_id is None:
            heads_id = []
        if subdivision_id is None:
            subdivision_id = []

        endpoint = ""
        match division:
            case "НТП1":
                endpoint = f"{self.service_url}/ntp1/get-premium-spec-month"
            case "НТП2":
                endpoint = f"{self.service_url}/ntp2/get-premium-spec-month"
            case "НЦК":
                endpoint = f"{self.service_url}/ntp-nck/get-premium-spec-month"

        response = await self.post(
            endpoint=endpoint,
            json={
                "period": period,
                "subdivisionId": subdivision_id,
                "headsId": heads_id,
                "employeesId": employees_id,
            },
        )

        try:
            data = await response.json()
            premium = SpecialistPremiumResponse.model_validate(data)
            return premium
        except Exception as e:
            logger.error(
                f"[API] [Premium] Ошибка получения премиума для специалистов: {e}"
            )
            return None

    async def get_head_premium(
        self,
        period: str,
        subdivision_id=None,
        heads_id=None,
        employees_id=None,
        division: str = "НТП",
    ) -> HeadPremiumResponse | None:
        if employees_id is None:
            employees_id = []
        if heads_id is None:
            heads_id = []
        if subdivision_id is None:
            subdivision_id = []

        endpoint = ""
        match division:
            case "НТП":
                endpoint = f"{self.service_url}/ntpo/get-premium-head-month"
            case "НЦК":
                endpoint = f"{self.service_url}/ntp-nck/get-premium-head-month"

        response = await self.post(
            endpoint=endpoint,
            json={
                "period": period,
                "subdivisionId": subdivision_id,
                "headsId": heads_id,
                "employeesId": employees_id,
            },
        )

        try:
            data = await response.json()
            premium = HeadPremiumResponse.model_validate(data)
            return premium
        except Exception as e:
            logger.error(
                f"[API] [Premium] Ошибка получения премиума для руководителей: {e}"
            )
            return None
