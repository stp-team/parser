from app.api.base import BaseAPI


class KpiAPI(BaseAPI):
    def __init__(self, session):
        super().__init__(session)
