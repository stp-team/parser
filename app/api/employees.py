from pydantic import TypeAdapter

from app.api.base import BaseAPI
from app.models.employees import Employee, EmployeeData


class EmployeesAPI(BaseAPI):
    def __init__(self, session):
        super().__init__(session)
        self.service_url = "dossier"

    async def get_employees(self, exclude_fired: bool = False) -> list[Employee] | None:
        employee_list_adapter = TypeAdapter(list[Employee])

        response = await self.post(f"{self.service_url}/api/get-employees")

        try:
            data = await response.json()

            employees = employee_list_adapter.validate_python(data)

            if exclude_fired:
                employees = [e for e in employees if not e.fired_date]

            return employees
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

        response = await self.post(
            endpoint=f"{self.service_url}/api/get-dossier",
            json={
                "employee": employee_id,
                "showKpi": show_kpi,
                "showCriticals": show_criticals,
            },
        )

        try:
            data = await response.json()
            employee = EmployeeData.model_validate(data)
            return employee
        except Exception as e:
            print("Parsing error:", e)
            return None
