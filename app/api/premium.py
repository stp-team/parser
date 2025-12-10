from app.api.base import BaseAPI


class PremiumAPI(BaseAPI):
    def __init__(self, session):
        super().__init__(session)
        self.service_url = "premium"

    # async def get_specialist_premium(
    #     self, period: str, subdivision_id=None, heads_id=None, employees_id=None
    # ) -> dict | None:
    #     if employees_id is None:
    #         employees_id = []
    #     if heads_id is None:
    #         heads_id = []
    #     if subdivision_id is None:
    #         subdivision_id = []
    #
    #     response = self.post(
    #         endpoint=f"{self.service_url}/ntp-nck/get-premium-spec-month",
    #         data={
    #             "period": period,
    #             "subdivisionId": subdivision_id,
    #             "headsId": heads_id,
    #             "employeesId": employees_id,
    #         },
    #     )
    #
    #     if response.status_code == 200:
    #         return response.json()
    #     return None
