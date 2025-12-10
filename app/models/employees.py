from pydantic import BaseModel, Field


class Employee(BaseModel):
    id: int
    fullname: str = Field(alias="name")
    fired_date: str | None = Field(alias="firedDate")


class EmployeeInfo(BaseModel):
    id: str = Field(alias="EMPLOYEE_ID", description="Идентификатор сотрудника")
    fullname: str = Field(alias="FIO", description="ФИО сотрудника")
    position: str = Field(alias="POST_NAME", description="Должность сотрудника")
    division: str = Field(
        alias="SUBDIVISION_NAME", description="Направление сотрудника"
    )
    unit_id: str = Field(alias="UNIT_ID", description="Идентификатор направления")
    unit_name: str = Field(alias="UNIT_NAME", description="Название направления")
    head_fullname: str = Field(
        alias="HEAD_NAME", description="ФИО руководителя сотрудника"
    )
    employment_date: str = Field(
        alias="EMPLOYMENT_DATE", description="День трудоустройства сотрудника"
    )
    transfer_date: str = Field(
        alias="TRANSFER_DATE", description="День изменения должности сотрудника"
    )
    birthday: str = Field(alias="BIRTHDAY", description="День рождения сотрудника")
    photo: str = Field(alias="PHOTO", description="Фотография сотрудника")
    city: str = Field(alias="CITY_NAME", description="Город сотрудника")
    trainee_id: int | None = Field(
        alias="TRAINEE_ID", description="Идентификатор стажера"
    )
    form_id: int | None = Field(alias="FORM_ID")


class PostHistoryItem(BaseModel):
    ID: str
    TRANSFER_DATE: str
    POST_NAME: str


class EmployeeData(BaseModel):
    employeeInfo: EmployeeInfo
    postsHistory: list[PostHistoryItem]
