from pydantic import TypeAdapter

from app.api.base import BaseAPI
from app.models.employees import Employee, EmployeeData


class EmployeesAPI(BaseAPI):
    def __init__(self, session):
        super().__init__(session)
        self.service_url = "dossier"

    async def get_employees(self) -> list[Employee] | None:
        employee_list_adapter = TypeAdapter(list[Employee])

        response = self.post(endpoint=f"{self.service_url}/api/get-employees")

        try:
            return employee_list_adapter.validate_python(response.json())
        except Exception as e:
            print("Parsing error:", e)
            return None

    async def get_employee(
        self,
        employee_id: int | None = None,
        employee_fullname: str | None = None,
        show_kpi: bool = True,
        show_criticals: bool = True,
    ) -> EmployeeData | None:
        if employee_id is None and employee_fullname:
            employees = await self.get_employees()
            if not employees:
                return None

            for employee in employees:
                if employee.fullname == employee_fullname:
                    employee_id = employee.id
                    break

        if employee_id is None:
            return None

        response = self.post(
            endpoint=f"{self.service_url}/api/get-dossier",
            data={
                "employee": employee_id,
                "showKpi": show_kpi,
                "showCriticals": show_criticals,
            },
        )

        try:
            employee = EmployeeData.model_validate(response.json())
            return employee
        except Exception as e:
            print("Parsing error:", e)
            return None
