from datetime import datetime
from enum import Enum

from pydantic import Field, BaseModel
from typing import Optional, NamedTuple

from src.pipeline import PipelineSettings
from src.schemas.google_sheets_schemas import SheetsValuesOut
from src.schemas.onec_schemas import OneCProductInfo, OnecNomenclature
from src.schemas.ozon_schemas import Datum, Remainder


class RemaindersByStock(BaseModel):
    warehouse_name: str = Field(default_factory=str)
    warehouse_id: int = Field(default_factory=int)
    remainders: list[Remainder] = Field(default_factory=list)

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
    article: str
    title: str     # описание товара
    price: float   # цена
    status: str    #"delivering", "cancelled", "delivered", "awaiting_deliver" и тд
    quantity: int  # количество

class PostingsDataByDeliveryModel(BaseModel):
    model:Optional[str] = Field(default_factory=str) # acc_name_FBO или acc_name_AI_FBS
    items: Optional[list[Item]] = Field(default_factory=list)

class Interval(str, Enum):
    WEEK = "Week"
    MONTH = "Month"

class Period(BaseModel):
    period_type: Optional[Interval] = Field(default=None)
    month_name: Optional[str] = Field(default_factory=str)
    start_date: Optional[str | datetime] = Field(default=None)
    end_date: Optional[str | datetime] = Field(default=None)

class PostingsProductsCollection(BaseModel):
    postings_fbs: Optional[PostingsDataByDeliveryModel] = Field(default_factory=PostingsDataByDeliveryModel)
    postings_fbo: Optional[PostingsDataByDeliveryModel] = Field(default_factory=PostingsDataByDeliveryModel)
    period: Optional[Period] = Field(default_factory=Period)

class MonthlyStats(BaseModel):
    month:Optional[str] = Field(default_factory=str)
    datum: list[Datum] = Field(default_factory=list)

class AccountStatsBase(BaseModel):
    ctx: PipelineSettings

class AccountStatsRemainders(AccountStatsBase):
    skus: list[int]
    remainders: list[Remainder]

class AccountStatsPostings(AccountStatsBase):
    postings: list[PostingsProductsCollection]

class AccountStatsAnalytics(AccountStatsBase):
    monthly_analytics: list[MonthlyStats]

class AccountStats(AccountStatsBase):
    skus: list[int]
    monthly_analytics: list[MonthlyStats]

class CommonStatsBase(BaseModel):
    monthly_analytics: list[MonthlyStats]
    remainders: list[Remainder]
    postings: list[PostingsProductsCollection]
    onec_nomenclatures: list[OnecNomenclature]

class PostingsByPeriod(BaseModel):
    postings: list[Item] = Field(default_factory=list)
    period: Period
    warehouse_id: int = Field(default_factory=int)
    warehouse_name: str = Field(default_factory=str)

class AccountSortedCommonStats(BaseModel):
    remainders_by_stock: list[RemaindersByStock] = Field(default_factory=list)
    postings_by_period: list[PostingsByPeriod] = Field(default_factory=list)
    monthly_analytics: list[MonthlyStats] = Field(default_factory=list)
    account_name: str
    account_id: str

class SortedCommonStats(BaseModel):
    sorted_stats: list[AccountSortedCommonStats]
    onec_nomenclatures: list[OnecNomenclature]

class CollectionStats(CommonStatsBase, AccountStatsBase):
    ...

class ClusterInfo(NamedTuple):
    cluster_name: str
    cluster_id: int
    remainders_quantity: Optional[int] = None

class SkuInfo(NamedTuple):
    sku: int
    article: str
    prod_name: str
    clusters_info: list[ClusterInfo]

class TurnoverByPeriodSku(NamedTuple):
    period: tuple
    turnover_by_period: int | float

    # def __init__(self, period: tuple, turnover_by_period: int | float):
    #     self.period = period
    #     self.turnover_by_period = turnover_by_period

class AnalyticsSkuByMonths(NamedTuple):
    month: str
    unique_visitors: int
    orders_amount: int | float
    orders_quantity: int

class ProductsByArticle(NamedTuple):
    lk_name: str
    article: str
    remainders_chi6: int
    remainders_msk: int
    cost_price: float
    # total_visitors: int
    # total_turnover_by_article: int
    turnovers_by_periods: list[TurnoverByPeriodSku]
    analytics_by_sku_by_months: list[AnalyticsSkuByMonths]
    total_remainder_count_by_clusters: int
    total_orders_by_period: list[tuple[Period, int]]
    products: list[SkuInfo]
