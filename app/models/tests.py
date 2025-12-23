from pydantic import BaseModel, Field


class AssignedTest(BaseModel):
    """Модель для назначенного теста."""

    id: str = Field(description="Идентификатор теста")
    test_name: str = Field(description="Название теста")
    user_name: str = Field(description="ФИО пользователя")
    head_name: str | None = Field(description="ФИО руководителя")
    creator_name: str | None = Field(description="ФИО создателя теста")
    status_name: str = Field(description="Статус теста")
    active_from: str = Field(description="Дата назначения теста")
    start_date: str | None = Field(description="Дата начала теста")
