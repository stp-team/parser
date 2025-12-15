import logging
from datetime import datetime, timedelta

from app.api.base import BaseAPI
from app.models.kpi import (
    AHTDataRecord,
    CSIDataRecord,
    DelayDataRecord,
    FLRDataRecord,
    GenericKPIDataRecord,
    KPIDataRecord,
    KPIResponse,
    PaidServiceRecord,
    POKDataRecord,
    SalesDataRecord,
    SalesPotentialDataRecord,
    TypedKPIResponse,
)
from app.services.constants import time_format

logger = logging.getLogger(__name__)


class KpiAPI(BaseAPI):
    unites = {
        "НТП1": 5,
        "НТП2": 6,
        "НЦК": 7,
    }
    reports = {
        "НТП1": {
            "AHT": 16,
            "FLR": 59,  # С правками - 59, без - 23
            "CSI": 12,  # С правками - 12, без - 33
            "POK": 8,  # С правками - 8, без - 34
            "DELAY": 39,  # Используется нерабочее время
            "Sales": 61,
            "SalesPotential": 69,
            "PaidService": 62,
        },
        "НТП2": {
            "AHT": 16,
            "FLR": 36,  # С правками - 36, без - 24
            "CSI": 12,  # С правками - 12, без - 33
            "POK": 8,  # С правками - 8, без - 34
            "DELAY": 39,  # Используется нерабочее время
            "Sales": 61,
            "SalesPotential": 69,
            "PaidService": 64,
        },
        "НЦК": {
            "AHT": 11,
            "FLR": 25,
            "CSI": 10,  # С правками - 10, без - 32
            "POK": 9,
            "DELAY": 54,  # Используется время доработки
            "Sales": 68,
            "SalesPotential": 87,
            "PaidService": 86,
        },
    }

    def __init__(self, session):
        super().__init__(session)
        self.service_url = "ure"

    def _get_report_model(self, report_type: str) -> type[KPIDataRecord]:
        """Get the appropriate Pydantic model for the report type."""
        report_models = {
            "AHT": AHTDataRecord,
            "FLR": FLRDataRecord,
            "CSI": CSIDataRecord,
            "POK": POKDataRecord,
            "DELAY": DelayDataRecord,
            "Sales": SalesDataRecord,
            "SalesPotential": SalesPotentialDataRecord,
            "PaidService": PaidServiceRecord,
        }
        return report_models.get(report_type, GenericKPIDataRecord)

    def _parse_response(self, data: dict, report_type: str) -> TypedKPIResponse | None:
        """Parse API response data into typed KPI response."""
        if not data:
            return None

        try:
            # First, parse as generic response
            kpi_response = KPIResponse(**data)

            # Get the appropriate model for data records
            model_class = self._get_report_model(report_type)

            # Parse each data record with the appropriate model
            typed_data = []
            for record in kpi_response.data:
                try:
                    typed_record = model_class(**record)
                    typed_data.append(typed_record)
                except Exception as e:
                    logger.warning(
                        f"Error parsing record with {model_class.__name__}: {e}"
                    )
                    # Fallback to generic model
                    typed_record = GenericKPIDataRecord(**record)
                    typed_data.append(typed_record)

            # Return typed response
            return TypedKPIResponse(
                data=typed_data,
                headers=kpi_response.headers,
                metricsHref=kpi_response.metrics_href,
            )
        except Exception as e:
            logger.error(f"Error parsing KPI response: {e}")
            return None

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
    ) -> dict | None:
        """Основной метод получения показателей из отчетов.

        Args:
            units: Список направлений
            start_date: Дата начала отчета
            stop_date: Дата конца отчета
            report: Отчет
            subdivisions: Список подразделений
            heads: Список руководителей
            employees: Список сотрудников
            report_type: Тип отчета (всегда 1)

        Returns:
            Словарь с полученными данными из отчета
        """
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
            logger.error(f"Parsing error: {e}")
            return None

    async def get_period_kpi(
        self, division: str, report: str, days: int
    ) -> TypedKPIResponse | None:
        """Базовый метод получения KPI за любой период.

        Args:
            division: Направление специалиста
            report: Тип отчета
            days: Дней с текущего дня

        Returns:
            Типизированный отчет
        """
        stop_date = datetime.today()
        start_date = stop_date - timedelta(days=days)

        # Get units and report ID from class attributes
        units = self.unites.get(division)
        report_id = self.reports.get(division, {}).get(report)

        if not units or not report_id:
            return None

        kpi_data = await self.get_report(
            units=units,
            start_date=start_date.strftime(time_format),
            stop_date=stop_date.strftime(time_format),
            report=report_id,
        )
        return self._parse_response(kpi_data, report)

    async def get_custom_period_kpi(
        self,
        division: str,
        report: str,
        start_date: datetime,
        end_date: datetime | None = None,
        use_week_period: bool = False
    ) -> TypedKPIResponse | None:
        """Получает KPI за кастомный период.

        Args:
            division: Направление специалиста
            report: Тип отчета
            start_date: Дата начала периода
            end_date: Дата окончания периода (если None, то текущая дата)
            use_week_period: Если True, использует конец текущей недели как end_date

        Returns:
            Типизированный отчет
        """
        if end_date is None:
            if use_week_period:
                # Для недельных данных используем конец недели
                from app.services.helpers import get_week_end_date
                end_date = get_week_end_date()
            else:
                end_date = datetime.now()

        # Get units and report ID from class attributes
        units = self.unites.get(division)
        report_id = self.reports.get(division, {}).get(report)

        if not units or not report_id:
            return None

        kpi_data = await self.get_report(
            units=units,
            start_date=start_date.strftime(time_format),
            stop_date=end_date.strftime(time_format),
            report=report_id,
        )
        return self._parse_response(kpi_data, report)

    async def get_day_kpi(self, division: str, report: str) -> TypedKPIResponse | None:
        """Получает показатели за последний день.

        Args:
            division: Направление специалиста
            report: Тип отчета

        Returns:
            Показатели за последний день
        """
        return await self.get_period_kpi(division, report, 1)

    async def get_week_kpi(self, division: str, report: str) -> TypedKPIResponse | None:
        """Получает показатели за последние 7 дней.

        Args:
            division: Направление специалиста
            report: Тип отчета

        Returns:
            Показатели за последнюю неделю
        """
        return await self.get_period_kpi(division, report, 7)

    async def get_month_kpi(
        self, division: str, report: str
    ) -> TypedKPIResponse | None:
        """Получает показатели за последние 31 дней.

        Args:
            division: Направление специалиста
            report: Тип отчета

        Returns:
            Показатели за последний месяц
        """
        return await self.get_period_kpi(division, report, 31)
