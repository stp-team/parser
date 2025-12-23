import logging
from urllib.parse import urlencode

from pydantic import TypeAdapter

from app.api.base import BaseAPI
from app.models.tests import AssignedTest


class TestsAPI(BaseAPI):
    def __init__(self, session):
        super().__init__(session)
        self.service_url = "testing/api"
        self.logger = logging.getLogger(self.__class__.__name__)

    async def get_assigned_tests(
        self, start_date: str, stop_date: str, subdivisions: list[int] | None = None
    ) -> list[AssignedTest] | None:
        """
        Получить список назначенных тестов.

        Args:
            start_date: Дата начала в формате DD.MM.YYYY
            stop_date: Дата окончания в формате DD.MM.YYYY
            subdivisions: Список ID подразделений

        Returns:
            Список назначенных тестов или None в случае ошибки
        """
        assigned_tests_adapter = TypeAdapter(list[AssignedTest])

        # Подготовка данных формы в URL-encoded формате
        form_params = [
            ("startDate", start_date),
            ("stopDate", stop_date),
        ]

        # Добавляем подразделения в формате subdivisions[]=id
        if subdivisions:
            for subdivision_id in subdivisions:
                form_params.append(("subdivisions[]", str(subdivision_id)))

        # Кодируем данные в URL-encoded формат
        encoded_data = urlencode(form_params)

        # Заголовки для form-encoded запроса
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }

        # Отправляем POST запрос с данными формы
        response = await self.post(
            f"{self.service_url}/get-assigned-tests",
            data=encoded_data.encode("utf-8"),
            headers=headers,
        )

        try:
            data = await response.json()
            tests = assigned_tests_adapter.validate_python(data)
            return tests
        except Exception as e:
            self.logger.error(f"Error parsing tests response: {e}")
            return None
