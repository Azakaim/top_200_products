from dataclasses import dataclass, asdict, field
from typing import Literal, Optional

from src.clients.google_sheets.schemas import SheetsValuesOut


@dataclass
class SheetsData:
    """
    SheetsData:
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

