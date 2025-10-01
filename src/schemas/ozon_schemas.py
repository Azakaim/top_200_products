from enum import StrEnum
from typing import List, Optional
from datetime import datetime

from pydantic import BaseModel,  Field


#--------------Enums-----------
class StatusDelivery (StrEnum):
    AWAITING_REGISTRATION = "awaiting_registration" # ожидает регистрации
    ACCEPTANCE_IN_PROGRESS = "acceptance_in_progress" # идёт приёмка
    AWAITING_APPROVE = "awaiting_approve" # ожидает подтверждения
    AWAITING_PACKAGING = "awaiting_packaging" # ожидает упаковки
    AWAITING_DELIVER = "awaiting_deliver" # ожидает отгрузки
    ARBITRATION = "arbitration" # арбитраж
    CLIENT_ARBITRATION = "client_arbitration" # клиентский арбитраж доставки
    DELIVERING = "delivering" # доставляется
    DRIVER_PICKUP = "driver_pickup" # у водителя
    DELIVERED = "delivered" # доставлено
    CANCELLED = "cancelled" # отменено
    NOT_ACCEPTED = "not_accepted" # не принят на сортировочном центре
    # SENT_BY_SELLER = "sent_by_seller" # отправлено продавцом

class AnalyticsMetrics(StrEnum):
    REVENUE = "revenue" # заказано на сумму
    ORDERED_UNITS = "ordered_units" # заказано товаров
    UNKNOWN_METRICS = "unknown_metrics" # неизвестная метрика
    HITS_VIEW_SEARCH = "hits_view_search" # показы в поиске и в категории
    HITS_VIEW_PDP = "hits_view_pdp" # показы на карточке товара
    HITS_VIEW = "hits_view" # всего показов
    HITS_TOCART_SEARCH = "hits_tocart_search" # в корзину из поиска или категории
    HITS_TOCART_PDP = "hits_tocart_pdp" # в корзину из карточки товара
    HITS_TOCART = "hits_tocart" # всего добавлено в корзину
    SESSION_VIEW_SEARCH = "session_view_search" # сессии с показом в поиске или в каталоге. Считаются уникальные посетители с просмотром в поиске или каталоге
    SESSION_VIEW_PDP = "session_view_pdp" # сессии с показом на карточке товара. Считаются уникальные посетители, которые просмотрели карточку товара
    SESSION_VIEW = "session_view" # всего сессий. Считаются уникальные посетители
    CONV_TOCART_SEARCH = "conv_tocart_search" # конверсия в корзину из поиска или категории
    CONV_TOCART_PDP = "conv_tocart_pdp" # конверсия в корзину из карточки товара
    CONV_TOCART = "conv_tocart" # общая конверсия в корзину
    RETURNS = "returns" # возвращено товаров
    CANCELLATION = "cancellation"   # отменено товаров
    DELIVERED_UNITS = "delivered_units" # доставлено товаров
    POSITION_CATEGORY = "position_category" # позиция в поиске и категории
#------------------------------

class Remainder(BaseModel):
    ads: float = Field(...)
    ads_cluster: float = Field(...)
    available_stock_count: int = Field(...)
    cluster_id: int = Field(...)
    cluster_name: str = Field(...)
    days_without_sales: int = Field(...)
    days_without_sales_cluster: int = Field(...)
    excess_stock_count: int = Field(...)
    expiring_stock_count: int = Field(...)
    idc: int = Field(...)
    idc_cluster: int = Field(...)
    item_tags: List[str] = Field(...)
    name: str = Field(...)
    offer_id: str = Field(...)
    other_stock_count: int = Field(...)
    requested_stock_count: int = Field(...)
    return_from_customer_stock_count: int = Field(...)
    return_to_seller_stock_count: int = Field(...)
    sku: int = Field(...)
    stock_defect_stock_count: int = Field(...)
    transit_defect_stock_count: int = Field(...)
    transit_stock_count: int = Field(...)
    turnover_grade: str = Field(...)
    turnover_grade_cluster: str = Field(...)
    valid_stock_count: int = Field(...)
    waiting_docs_stock_count: int = Field(...)
    warehouse_id: int = Field(...)
    warehouse_name: str = Field(...)

class SellerAccount(BaseModel):
    """
    Ozon_cli API settings.
    """
    api_key: str = Field(..., description="API key for Ozon API")
    name: str = Field(..., description="Name of the seller in Ozon")
    client_id: str = Field(..., description="Client ID for Ozon API")

class LastChangedStatusDate(BaseModel):
    date_from: str = Field(default="", alias="from", description="Start date for the last changed status, in ISO 8601 format")
    to: str = Field(default="", description="End date for the last changed status, in ISO 8601 format")

    model_config = {
        "populate_by_name": True
    }

class FilterPosting(BaseModel):
    delivery_method_id: List[str] = Field(default_factory=list, description="List of delivery method IDs to filter by")
    is_quantum: Optional[bool] = Field(default=None, description="Whether the delivery method is quantum or not")
    last_changed_status_date: LastChangedStatusDate = Field(default=None, description="Last changed status date")
    order_id: int = Field(default=0, description="Order ID to filter by")
    provider_id: List[str] = Field(default_factory=list, description="List of provider IDs to filter by")
    since: str # ISO 8601 format, e.g. "2025-10-01T00:00:00Z" -- год-месяц-деньTчасы:минуты:секундыZ
    status: str = Field(default_factory=str)
    to: str # ISO 8601 format, e.g. "2025-10-01T00:00:00Z" -- год-месяц-деньTчасы:минуты:секундыZ
    warehouse_id: List[str] = Field(default_factory=list, description="List of warehouse IDs to filter by")

class With(BaseModel):
    analytics_data: bool = Field(default=False, description="Whether to include analytics data or not")
    barcodes: bool = Field(default=False, description="Whether to include barcodes or not")
    financial_data: bool = Field( default=True, description="Whether to include financial data or not")
    translit: bool = Field(default=False, description="Whether to include transliterated data or not")

class PostingRequestSchema(BaseModel):
    dir: str = Field(default="asc", description="Direction of sorting results, either 'asc' or 'desc'")
    filter: FilterPosting = Field(
        default_factory=lambda: FilterPosting(
            delivery_method_id=[],
            is_quantum=False,
            last_changed_status_date=LastChangedStatusDate(),
            order_id=0,
            provider_id=[],
            since="",
            status="",
            to="",
            warehouse_id=[]
        ),
        description="Filter criteria for the posting request"
    )# default_factory а не default, чтобы избежать мутабельности, если оставить default,
    # то будет использоваться один и тот же объект для всех экземпляров PostingRequest
    limit: int = Field(default=1000, description="Maximum number of records to return")
    offset: int = Field(default=0, description="Offset from which to start the search")
    add_with: With = Field(default=None, alias="with", description="Additional fields to include in the response")

    model_config = {
        "populate_by_name": True
    }

class OzOptional(BaseModel):
    """
    Optional fields related to the posting.
    """
    products_with_possible_mandatory_mark: List[object] = Field(default_factory=list, description="List of product IDs that may require a mandatory mark")

# create classes for the response structure by json above
class Cancellation(BaseModel):
    cancel_reason_id: int = Field(default=0, description="ID of the cancellation reason")
    cancel_reason: str = Field(default="", description="Reason for cancellation")
    cancellation_type: str = Field(default="", description="Type of cancellation")
    cancelled_after_ship: bool = Field(default=False, description="Indicates if the cancellation occurred after shipping")
    affect_cancellation_rating: bool = Field(default=False, description="Indicates if the cancellation affects the rating")
    cancellation_initiator: str = Field(default="", description="Who initiated the cancellation")

class Addressee(BaseModel):
    name: str = Field(..., description="Name of the addressee")
    phone: str = Field(..., description="Phone number of the addressee")

class DeliveryMethod(BaseModel):
    id: int = Field(..., description="ID of the delivery method")
    name: str = Field(..., description="Name of the delivery method")
    warehouse_id: int = Field(..., description="ID of the warehouse associated with the delivery method")
    warehouse: str = Field(..., description="Name of the warehouse associated with the delivery method")
    tpl_provider_id: int = Field(..., description="ID of the TPL provider")
    tpl_provider: str = Field(..., description="Name of the TPL provider")

class LegalInfo(BaseModel):
    company_name: str = Field(..., description="Name of the company")
    inn: str = Field(..., description="INN (Taxpayer Identification Number) of the company")
    kpp: str = Field(..., description="KPP code of the company")

class Product(BaseModel):
    price: str = Field(..., description="Price of the product")
    currency_code: str = Field(..., description="Currency code for the price")
    is_blr_traceable: bool = Field(..., description="Indicates if the product is traceable by Belarusian law")
    is_marketplace_buyout: bool = Field(..., description="Indicates if the product is a marketplace buyout")
    offer_id: str = Field(..., description="Offer ID for the product") # артикул
    name: str = Field(..., description="Name of the product")
    sku: int = Field(..., description="SKU (Stock Keeping Unit) of the product")
    quantity: int = Field(..., description="Quantity of the product in the posting")

class Requirements(BaseModel):
    products_requiring_change_country: List[int] = Field(default_factory=list, description="List of product IDs requiring a change of country")
    products_requiring_gtd: List[int] = Field(default_factory=list, description="List of product IDs requiring GTD (Goods Declaration)")
    products_requiring_country: List[int] = Field(default_factory=list, description="List of product IDs requiring a specific country")
    products_requiring_mandatory_mark: List[int] = Field(default_factory=list, description="List of product IDs requiring a mandatory mark")
    products_requiring_jw_uin: List[int] = Field(default_factory=list, description="List of product IDs requiring JWN (Joint Warehouse Number)")
    products_requiring_jwn: List[int] = Field(default_factory=list, description="List of product IDs requiring JWN (Joint Warehouse Number)")
    products_requiring_rnpt: List[int] = Field(default_factory=list, description="List of product IDs requiring RNPT (Russian National Product Type)")

class Tariffication(BaseModel):
    current_tariff_rate: float = Field(..., description="Current tariff rate")
    current_tariff_type: str = Field(..., description="Type of the current tariff")
    current_tariff_charge: str = Field(..., description="Charge for the current tariff")
    current_tariff_charge_currency_code: str = Field(..., description="Currency code for the current tariff charge")
    next_tariff_rate: float = Field(..., description="Next tariff rate")
    next_tariff_type: str = Field(..., description="Type of the next tariff")
    next_tariff_charge: str = Field(..., description="Charge for the next tariff")
    next_tariff_starts_at: str = Field(..., description="Timestamp when the next tariff starts")
    next_tariff_charge_currency_code: str = Field(..., description="Currency code for the next tariff charge")

class Posting(BaseModel):
    posting_number: str = Field(..., description="Unique identifier for the posting")
    order_id: int = Field(..., description="Order ID associated with the posting")
    order_number: str = Field(..., description="Order number associated with the posting")
    pickup_code_verified_at: Optional[str] = Field(default=None, description="Timestamp when the pickup code was verified")
    status: StatusDelivery = Field(..., description="Current status of the posting")
    substatus: Optional[str] = Field(default=None, description="Substatus of the posting")
    delivery_method: DeliveryMethod = Field(..., description="Details of the delivery method used")
    tracking_number: Optional[str] = Field(default=None, description="Tracking number for the posting")
    tpl_integration_type: Optional[str] = Field(default=None, description="Type of TPL integration used")
    in_process_at: Optional[str] = Field(default=None, description="Timestamp when the posting was last processed")
    shipment_date: Optional[str] = Field(default=None, description="Date when the posting was shipped")
    delivering_date: Optional[str] = Field(default=None, description="Date when the posting is expected to be delivered")
    optional: Optional[OzOptional] = Field(default=None, description="Optional fields related to the posting")
    cancellation: Optional[Cancellation] = Field(default=None, description="Cancellation details if applicable")
    customer: Optional[Addressee] = Field(default=None, description="Customer details for the posting")
    products: List[Product] = Field(default_factory=list, description="List of products in the posting")
    addressee: Optional[Addressee] = Field(default=None, description="Addressee details for the posting")
    barcodes: Optional[List[str]] = Field(default=None, description="List of barcodes associated with the posting")
    analytics_data: Optional[dict] = Field(default=None, description="Analytics data related to the posting")
    financial_data: Optional[dict] = Field(default=None, description="Financial data related to the posting")
    is_express: bool = Field(default=False, description="Indicates if the posting is express delivery")
    legal_info: LegalInfo = Field(..., description="Legal information related to the seller or company")
    quantum_id: int = Field(..., description="Quantum ID associated with the posting")
    requirements: Requirements = Field(..., description="Requirements for the posting")
    tariffication: List[Tariffication] = Field(default_factory=list, description="Tariffication details for the posting")

class Result(BaseModel):
    postings: List[Posting] = Field(default_factory=list, description="List of postings")
    has_next: bool = Field(default=False, description="Indicates if there are more postings to fetch")

class OzonPostingResponse(BaseModel):
    result: Result = Field(default=None, description="Result containing postings and pagination info")

class APIError(RuntimeError):
    """
    Class for handling errors from the Ozon API.
    :param status: HTTP status code of the error.
    :param endpoint: The API endpoint that caused the error.
    :param body: The response body containing the error message.
    :return: None
    """
    def __init__(self, status: int, endpoint: str, body: str):
        super().__init__(f"Ozon API error {status} at {endpoint}: {body}")
        self.status = status
        self.endpoint = endpoint
        self.body = body

class FilterProducts(BaseModel):
    offer_id: list = Field(default_factory=list, description="Offer ID associated with the posting")
    product_id: list = Field(default_factory=list, description="Product ID associated with the posting")
    visibility: str = "ALL"

class SkusRequestShema(BaseModel):
    filter: FilterProducts = Field(default_factory=FilterProducts, description="Filter to apply to the postings")
    last_id: str = Field(default_factory=str, description="Last ID of the posting")
    limit: int = Field(default_factory=int, description="Limit the number of postings")

class ProductItem(BaseModel):
    product_id: int = Field(..., description="Product ID associated with the posting")
    offer_id: str = Field(default_factory=str, description="Offer ID associated with the posting")
    has_fbo_stocks: bool = Field(..., description="Indicates if the product has free stocks")
    has_fbs_stocks: bool = Field(..., description="Indicates if the product has free stocks")
    archived: bool = Field(..., description="Indicates if the product is archived")
    is_discounted: bool = Field(..., description="Indicates if the product is discounted")
    quants: list = Field(default_factory=list, description="List of quantities associated with the posting")

class Products(BaseModel):
    items: list[ProductItem] = Field(default_factory=list, description="List of postings")
    total: int = Field(default_factory=int, description="Total number of postings")
    last_id: str = Field(default_factory=str, description="Last ID of the posting")

class ArticlesResponseShema(BaseModel):
    result: Products = Field(default_factory=Products, description="Result containing products and pagination info")

class Commission(BaseModel):
    delivery_amount: float = Field(default_factory=float)
    percent: float = Field(default_factory=float)
    return_amount: int = Field(default_factory=int)
    sale_schema: str = Field(default_factory=str)
    value: float = Field(default_factory=float)

class ExternalIndexData(BaseModel):
    minimal_price: str = Field(default_factory=str)
    minimal_price_currency: str = Field(default_factory=str)
    price_index_value: float = Field(default_factory=float)

class ModelInfo(BaseModel):
    count: int = Field(default_factory=int)
    model_id: int = Field(default_factory=int)

class OzonIndexData(BaseModel):
    minimal_price: str = Field(default_factory=str)
    minimal_price_currency: str = Field(default_factory=str)
    price_index_value: float = Field(default_factory=float)

class SelfMarketplacesIndexData(BaseModel):
    minimal_price: str = Field(default_factory=str)
    minimal_price_currency: str = Field(default_factory=str)
    price_index_value: float = Field(default_factory=float)

class Source(BaseModel):
    created_at: str = Field(default_factory=str)
    quant_code: str = Field(default_factory=str)
    shipment_type: str = Field(default_factory=str)
    sku: int = Field(default_factory=int)
    source: str = Field(default_factory=str)

class Statuses(BaseModel):
    is_created: bool = Field(default_factory=bool)
    moderate_status: str = Field(default_factory=str)
    status: str = Field(default_factory=str)
    status_description: str = Field(default_factory=str)
    status_failed: str = Field(default_factory=str)
    status_name: str = Field(default_factory=str)
    status_tooltip: str = Field(default_factory=str)
    status_updated_at: str = Field(default_factory=str)
    validation_status: str = Field(default_factory=str)

class PriceIndexes(BaseModel):
    color_index: str = Field(default_factory=str)
    external_index_data: ExternalIndexData = Field(default_factory=ExternalIndexData)
    ozon_index_data: OzonIndexData = Field(default_factory=OzonIndexData)
    self_marketplaces_index_data: SelfMarketplacesIndexData = Field(default_factory=SelfMarketplacesIndexData)

class Promotion(BaseModel):
    is_enabled: bool = Field(default_factory=bool)
    type: str = Field(default="")

class Stock(BaseModel):
    present: int = Field(default_factory=int)
    reserved: int = Field(default_factory=int)
    sku: int = Field(default_factory=int)
    source: str = Field(default_factory=str)

class VisibilityDetails(BaseModel):
    has_price: bool = Field(default_factory=bool)
    has_stock: bool = Field(default_factory=bool)

class Stocks(BaseModel):
    has_stock: bool = Field(default_factory=bool)
    stocks: list[Stock] = Field(default_factory=list)

class ProductInfo(BaseModel):
    barcodes: List[object] = Field(default_factory=list)
    color_image: List[object] = Field(default_factory=list)
    commissions: List[Commission] = Field(default_factory=list)
    created_at: str = Field(default_factory=str)
    currency_code: str = Field(default_factory=str)
    description_category_id: int = Field(default_factory=int)
    discounted_fbo_stocks: int = Field(default_factory=int)
    errors: List[object] = Field(default_factory=list)
    has_discounted_fbo_item: bool = Field(default_factory=bool)
    id: int = Field(default_factory=int)
    images: List[str] = Field(default_factory=list)
    images360: List[object] = Field(default_factory=list)
    is_archived: bool = Field(default_factory=bool)
    is_autoarchived: bool = Field(default_factory=bool)
    is_discounted: bool = Field(default_factory=bool)
    is_kgt: bool = Field(default_factory=bool)
    is_prepayment_allowed: bool = Field(default_factory=bool)
    is_seasonal: bool = Field(default_factory=bool)
    is_super: bool = Field(default_factory=bool)
    marketing_price: str = Field(default_factory=str)
    min_price: str = Field(default_factory=str)
    model_info: ModelInfo = Field(default_factory=ModelInfo)
    name: str = Field(default_factory=str)
    offer_id: str = Field(default_factory=str)
    old_price: str = Field(default_factory=str)
    price: str = Field(default_factory=str)
    price_indexes: PriceIndexes = Field(default_factory=PriceIndexes)
    primary_image: List[str] = Field(default_factory=str)
    promotions: List[Promotion] = Field(default_factory=Promotion)
    sku: int = Field(default_factory=int)
    sources: List[Source] = Field(default_factory=list)
    statuses: Statuses = Field(default_factory=Statuses)
    stocks: Stocks = Field(default_factory=Stocks)
    type_id: int = Field(default_factory=int)
    updated_at: str = Field(default_factory=str)
    vat: str = Field(default_factory=str)
    visibility_details: VisibilityDetails = Field(default_factory=VisibilityDetails)
    volume_weight: float = Field(default_factory=float)

class SkusResponseShema(BaseModel):
    result: ProductInfo = Field(default_factory=Products, description="Result containing products and pagination info")

class Sort(BaseModel):
    key: str = Field(default="")
    order: str = Field(default="")

class AnalyticsRequestSchema(BaseModel):
    date_from: datetime = Field(default="")
    date_to: datetime = Field(default="")
    metrics: List[str] = Field(default_factory=list)
    dimension: List[str] = Field(default_factory=list)
    filters: List[object] = Field(default_factory=list)
    sort: List[Sort] = Field(default_factory=list)
    limit: int = Field(default=1000)
    offset: int = Field(default_factory=int)

    def to_dict(self):
        return self.model_dump(mode='json')

class Dimension(BaseModel):
    id: str = Field("")
    name: str = Field("")

class Datum(BaseModel):
    dimensions: List[Dimension] = Field(default_factory=list)
    metrics: List[int] = Field(default_factory=list)

class AnalyticsResult(BaseModel):
    data: List[Datum] = Field(default_factory=list)
    totals: List[int] = Field(default_factory=list)

class AnalyticsResponseSchema(BaseModel):
    result: AnalyticsResult = Field(default_factory=AnalyticsResult)
    timestamp: str = Field("")
