from pydantic import Field, BaseModel
from typing import Optional

from src.pipeline import PipelineSettings
from src.schemas.google_sheets_schemas import SheetsValuesOut
from src.schemas.ozon_schemas import Datum, Remainder


class SheetsData(BaseModel):
    """
    SheetsData:
        Args:
            - existed_sheets (dict[str, str]): существующие таблицы;
            - extracted_values (list[SheetsValuesOut]): извлечённые данные из листа;
            - table_data_for_backup (dict): данные всей таблицы для бэкапа.
    """
    existed_sheets: dict[str, int]
    extracted_values: list[SheetsValuesOut]
    table_data_for_backup: dict

class Item(BaseModel):
    sku_id: int    # 1990519270
    title: str     # описание товара
    price: float   # цена
    status: str    #"delivering", "cancelled", "delivered", "awaiting_deliver" и тд
    quantity: int  # количество

class PostingsDataByDeliveryModel(BaseModel):
    model:Optional[str] = Field(default=str) # acc_name_FBO или acc_name_AI_FBS
    items: Optional[list[Item]] = Field(default_factory=list)

class PostingsProductsCollection(BaseModel):
    postings_fbs: Optional[PostingsDataByDeliveryModel] = Field(default_factory=PostingsDataByDeliveryModel)
    postings_fbo: Optional[PostingsDataByDeliveryModel] = Field(default_factory=PostingsDataByDeliveryModel)

class MonthlyStats(BaseModel):
    month: str
    datum: list[Datum]

class AccountMonthlyStatsPostingsBase(BaseModel):
    ctx: PipelineSettings


class AccountMonthlyStatsRemainders(AccountMonthlyStatsPostingsBase):
    skus: list[int]
    remainders: list[Remainder]


class AccountMonthlyStatsPostings(AccountMonthlyStatsPostingsBase):
    postings: PostingsProductsCollection


class AccountMonthlyStatsAnalytics(AccountMonthlyStatsPostingsBase):
    monthly_analytics: list[MonthlyStats]


class AccountMonthlyStats(AccountMonthlyStatsPostingsBase):
    skus: list[int]
    monthly_analytics: list[MonthlyStats]


class CollectionStats(AccountMonthlyStatsPostingsBase):
    monthly_analytics: list[MonthlyStats]
    remainders: list[Remainder]
    postings: PostingsProductsCollection

"""
    для реализации строкового представления класса
    чтобы пайдентик вместо строки мог реальный класс
    подставить
"""