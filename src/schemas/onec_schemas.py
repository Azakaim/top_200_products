from typing import Optional

from pydantic import BaseModel, Field


class OneCArticleInfo(BaseModel):
    uid: str
    article: str
    name: str
    stock: int
    summ: float

class OneCArticlesResponse(BaseModel):
    done: bool
    code: int
    data: list[OneCArticleInfo]

class Sku(BaseModel):
    personal_account: str = Field(default="", alias="personalAccount")
    name_account: str = Field(default="", alias="nameAccount")
    trading_platform: str = Field(default="", alias="tradingPlatform")
    id: str = Field(default="", alias="ID")
    sku_fbo: str
    sku_fbs: str

    model_config = {
        "populate_by_name": True
    }

class WareHouse(BaseModel):
    uid: Optional[str] = Field(default="")
    name: Optional[str] = Field(default="")
    quantity: Optional[int] = Field(default_factory=int)

class OneCProductInfo(BaseModel):
    uid: Optional[str] = Field(default="")
    article: Optional[str] = Field(default="")
    name: Optional[str] = Field(default="")
    stock: Optional[list[WareHouse]] = Field(default_factory=list)
    skus: Optional[list[Sku]] = Field(default_factory=list, alias="sku")

    model_config = {
        "populate_by_name": True
    }

class OneCProductByUidResponse(BaseModel):
    done: bool
    code: int
    data: OneCProductInfo

class OneCProductsResults(BaseModel):
    onec_responses: list[OneCProductByUidResponse]

class OnecNomenclature(BaseModel):
    article: Optional[str] = Field(default="")
    name: Optional[str] = Field(default="")
    stock: Optional[list[WareHouse]] = Field(default_factory=list)
    skus: Optional[list[Sku]] = Field(default_factory=list, alias="sku")
    cost_price_per_one: Optional[float] = Field(default_factory=float)

    model_config = {
        "populate_by_name": True
    }

class OneCNomenclatureCollection(BaseModel):
    onec_products: list[OneCProductInfo]
