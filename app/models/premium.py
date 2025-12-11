from typing import Any

from pydantic import BaseModel, Field, model_validator


class BasePremiumData(BaseModel):
    """Base class for common premium data fields"""

    # Базовые идентификаторы
    core_id: int = Field(..., alias="CORE_ID")
    person_id: int = Field(..., alias="PERSON_ID")
    employee_id: int = Field(..., alias="EMPLOYEE_ID")
    user_fullname: str = Field(..., alias="USER_FIO")
    head_id: int | None = Field(None, alias="HEAD_ID")
    head_fullname: str | None = Field(None, alias="HEAD_FIO")
    period: str = Field(..., alias="PERIOD")

    # Организация
    subdivision_id: int = Field(..., alias="SUBDIVISION_ID")
    subdivision_name: str = Field(..., alias="SUBDIVISION_NAME")
    post_id: int = Field(..., alias="POST_ID")
    post_name: str = Field(..., alias="POST_NAME")
    user_type_id: int = Field(..., alias="USER_TYPE_ID")
    user_type_description: str = Field(..., alias="USER_TYPE_DESCRIPTION")

    # ГОК
    gok: float = Field(..., alias="GOK")
    gok_normative: float | None = Field(None, alias="GOK_NORMATIVE")
    gok_normative_rate: float | None = Field(None, alias="NORM_GOK")
    gok_premium: int = Field(..., alias="PERC_GOK")

    # FLR
    flr: float = Field(..., alias="FLR")
    flr_normative: float | None = Field(None, alias="FLR_NORMATIVE")
    flr_normative_rate: float | None = Field(None, alias="NORM_FLR")
    flr_premium: int = Field(..., alias="PERC_FLR")

    # Спец. цель
    pers_target_type_id: int | None = Field(None, alias="PERS_TARGET_TYPE_ID")
    target: float | None = Field(None, alias="PERS_FACT")
    target_type: str | None = Field(None, alias="PERS_TARGET_TYPE_NAME")
    target_normative_first: float | None = Field(None, alias="PERS_PLAN_1")
    target_normative_second: float | None = Field(None, alias="PERS_PLAN_2")
    target_normative_rate_first: float | None = Field(None, alias="PERS_RESULT_1")
    target_normative_rate_second: float | None = Field(None, alias="PERS_RESULT_2")
    target_premium: int | None = Field(None, alias="PERS_PERCENT")
    pers_target_manual: int | None = Field(None, alias="PERS_TARGET_MANUAL")

    # Результаты
    head_adjust_premium: float | None = Field(None, alias="HEAD_ADJUST")
    total_premium: float = Field(..., alias="TOTAL_PREMIUM")
    commentary: str | None = Field(None, alias="COMMENTARY")


class SpecialistPremiumData(BasePremiumData):
    """Model for specialist premium data"""

    # Оценка
    csi: float = Field(..., alias="CSI")
    csi_normative: float | None = Field(None, alias="CSI_NORMATIVE")
    csi_normative_rate: float | None = Field(None, alias="NORM_CSI")
    csi_premium: int = Field(..., alias="PERC_CSI")

    # Отклик
    csi_response: float | None = Field(None, alias="CSI_RESPONSE")
    csi_response_normative: float | None = Field(None, alias="CSI_RESPONSE_NORMATIVE")
    csi_response_normative_rate: float | None = Field(None, alias="NORM_CSI_RESPONSE")

    # Прочие показатели
    discipline_premium: int = Field(..., alias="PERC_DISCIPLINE")
    tests_premium: int = Field(..., alias="PERC_TESTING")
    thanks_premium: int = Field(..., alias="PERC_THANKS")
    tutors_premium: float = Field(..., alias="PERC_TUTORS")

    # Специфичные для специалистов поля
    total_contacts: int = Field(...)

    @model_validator(mode="before")
    @classmethod
    def normalize_total_contacts(cls, values: dict[str, Any]) -> dict[str, Any]:
        for key in ("TOTAL_CHATS", "TOTAL_CALLS"):
            if key in values:
                values["total_contacts"] = values[key]
                break
        return values


class HeadPremiumData(BasePremiumData):
    """Model for head (supervisor) premium data"""

    sl: float | None = Field(None, alias="SL_FACT")
    sl_normative_first: float | None = Field(None, alias="SL_PLAN_1")
    sl_normative_second: float | None = Field(None, alias="SL_PLAN_2")
    sl_normative_rate_first: float | None = Field(None, alias="SL_RESULT_1")
    sl_normative_rate_second: float | None = Field(None, alias="SL_RESULT_2")
    sl_premium: int = Field(..., alias="SL_PERCENT")


class HeadPremiumResponse(BaseModel):
    """Wrapper for head premium response with not eligible employees"""

    premium: list[HeadPremiumData] = Field(...)


class SpecialistPremiumResponse(BaseModel):
    """Response model for specialist premium data"""

    items: list[SpecialistPremiumData] = Field(...)

    @classmethod
    def model_validate(cls, data):
        """Validate from API response data"""
        if isinstance(data, list):
            # If data is a list, wrap it in the items field
            return cls(
                items=[SpecialistPremiumData.model_validate(item) for item in data]
            )
        elif isinstance(data, dict) and "items" in data:
            # If data already has items field
            return super().model_validate(data)
        else:
            # Fallback to standard validation
            return super().model_validate(data)


class HeadPremiumDataList(BaseModel):
    """Wrapper for handling list of HeadPremiumData"""

    items: list[HeadPremiumData] = Field(..., alias="root")

    @classmethod
    def from_list(cls, data_list: list[dict]):
        """Create from a list of dictionaries"""
        return cls(root=[HeadPremiumData(**item) for item in data_list])
