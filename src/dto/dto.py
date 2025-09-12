from dataclasses import dataclass, asdict, field
from typing import Literal, Optional, TYPE_CHECKING

from src.clients.google_sheets.schemas import SheetsValuesOut
from src.clients.ozon.schemas import Datum, Remainder

if TYPE_CHECKING:
    from src.pipeline.pipeline_settings import PipelineSettings # что бы избавиться от подчеркиваний круговой зависимости

@dataclass
class SheetsData:
    """
    SheetsData:
        Args:
            - existed_sheets (dict[str, str]): существующие таблицы;
            - extracted_values (list[SheetsValuesOut]): извлечённые данные из листа;
            - table_data_for_backup (dict): данные всей таблицы для бэкапа.
    """
    existed_sheets: dict[str, str]
    extracted_values: list[SheetsValuesOut]
    table_data_for_backup: dict

    def to_dict(self) -> dict:
        return asdict(self)

@dataclass
class Item:
    sku_id: str                  # "1990519270"
    title: str                   # описание товара
    price: float                 # цена
    status: Literal[
        "delivering", "cancelled", "delivered", "awaiting_deliver"
    ]
    quantity: int                 # количество

@dataclass
class PostingsDataByDeliveryModel:
    model:Optional[str] = field(default=str) # acc_name_FBO или acc_name_AI_FBS
    items: Optional[list[Item]] = field(default_factory=list)

@dataclass
class PostingsProductsCollection:
    postings_fbs: Optional[PostingsDataByDeliveryModel] = field(default_factory=PostingsDataByDeliveryModel)
    postings_fbo: Optional[PostingsDataByDeliveryModel] = field(default_factory=PostingsDataByDeliveryModel)

@dataclass
class MonthlyStats:
    month: str
    datum: list[Datum]

@dataclass
class AccountMonthlyStatsPostingsBase:
    ctx: "PipelineSettings"

@dataclass
class AccountMonthlyStatsRemainders(AccountMonthlyStatsPostingsBase):
    skus: list[int]
    remainders: list[Remainder]

@dataclass
class AccountMonthlyStatsPostings(AccountMonthlyStatsPostingsBase):
    postings: PostingsProductsCollection

@dataclass
class AccountMonthlyStatsAnalytics(AccountMonthlyStatsPostingsBase):
    monthly_analytics: list[MonthlyStats]

@dataclass
class AccountMonthlyStats(AccountMonthlyStatsPostingsBase):
    skus: list[int]
    monthly_analytics: list[MonthlyStats]

@dataclass
class CollectionStats(AccountMonthlyStatsPostingsBase):
    ctx: "PipelineSettings"
    monthly_analytics: list[MonthlyStats]
    remainders: list[Remainder]
    postings: PostingsProductsCollection
