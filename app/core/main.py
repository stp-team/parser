import asyncio

from app.api.employees import EmployeesAPI
from app.core.auth import authenticate
from app.core.config import settings


async def main():
    session = await authenticate(
        username=settings.OKC_USERNAME, password=settings.OKC_PASSWORD
    )

    employees = EmployeesAPI(session)
    employees = await employees.get_employees()
    print(employees)


if __name__ == "__main__":
    asyncio.run(main())
