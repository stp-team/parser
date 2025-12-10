from datetime import datetime, timedelta

from app.api.base import BaseAPI


class KpiAPI(BaseAPI):
    unites = {
        "НТП1": 5,
        "НТП2": 6,
        "НЦК": 7,
    }
    reports = {
        "НТП1": {"FLR": 23, "AHT": 16, "": 1},
        "НТП2": {"FLR": 24, "AHT": 16, "CS": 1},
        "НЦК": {"FLR": 25, "AHT": 11, "CS": 1},
    }

    def __init__(self, session):
        super().__init__(session)
        self.service_url = "ure"

    async def get_report(
        self,
        units: int | list[int],
        start_date: str,
        stop_date: str,
        report: int,
        subdivisions=None,
        heads=None,
        employees=None,
        report_type: int = 1,
    ):
        if employees is None:
            employees = []
        if heads is None:
            heads = []
        if subdivisions is None:
            subdivisions = []

        units_list = units if isinstance(units, list) else [units]

        response = await self.post(
            endpoint=f"{self.service_url}/api/get-report",
            json={
                "units": units_list,
                "subdivisions": subdivisions,
                "heads": heads,
                "employees": employees,
                "startDate": start_date,
                "stopDate": stop_date,
                "reportType": report_type,
                "report": report,
            },
        )

        try:
            data = await response.json()
            return data
        except Exception as e:
            print("Parsing error:", e)
            return None

    async def _get_flr(self): ...

    async def get_day_kpi(
        self,
        division: str,
        report: str,
    ):
        stop_date = datetime.today()
        start_date = stop_date - timedelta(days=1)

        # Get units and report ID from class attributes
        units = self.unites.get(division)
        report_id = self.reports.get(division, {}).get(report)

        if not units or not report_id:
            return None

        day_kpi = await self.get_report(
            units=units,
            start_date=start_date.strftime("%Y-%m-%d"),
            stop_date=stop_date.strftime("%Y-%m-%d"),
            report=report_id,
        )
        return day_kpi

    async def get_week_kpi(self, division: str, report: str):
        stop_date = datetime.today()
        start_date = stop_date - timedelta(days=7)

        # Get units and report ID from class attributes
        units = self.unites.get(division)
        report_id = self.reports.get(division, {}).get(report)

        if not units or not report_id:
            return None

        week_kpi = await self.get_report(
            units=units,
            start_date=start_date.strftime("%Y-%m-%d"),
            stop_date=stop_date.strftime("%Y-%m-%d"),
            report=report_id,
        )
        return week_kpi

    async def get_month_kpi(self, division: str, report: str):
        stop_date = datetime.today()
        start_date = stop_date - timedelta(days=31)

        # Get units and report ID from class attributes
        units = self.unites.get(division)
        report_id = self.reports.get(division, {}).get(report)

        if not units or not report_id:
            return None

        month_kpi = await self.get_report(
            units=units,
            start_date=start_date.strftime("%Y-%m-%d"),
            stop_date=stop_date.strftime("%Y-%m-%d"),
            report=report_id,
        )
        return month_kpi
