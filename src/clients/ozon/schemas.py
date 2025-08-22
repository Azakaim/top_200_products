from enum import StrEnum
from typing import List, Optional

from settings import proj_settings

from pydantic import BaseModel,  Field


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

class SellerAccount(BaseModel):
    """
    Ozon_cli API settings.
    """
    api_key: str = Field(..., description="API key for Ozon API")
    name: str = Field(..., description="Name of the seller in Ozon")
    client_id: str = Field(..., description="Client ID for Ozon API")

def extract_sellers() -> list[SellerAccount]:
    """
    Extracts sellers from the environment variables.
    """
    client_ids = proj_settings.OZON_CLIENT_IDS.split(',')
    api_keys = proj_settings.OZON_API_KEYS.split(',')
    names = proj_settings.OZON_NAME_LK.split(',')

    if len(client_ids) != len(api_keys) != len(names):
        raise ValueError("Client IDs, API keys, and names must have the same length.")

    return [
        SellerAccount(api_key=api_keys[i], name=names[i], client_id=client_ids[i])
        for i in range(len(client_ids)) if client_ids[i] and api_keys[i] and names[i]
    ]

class LastChangedStatusDate(BaseModel):
    date_from: str = Field(default="", alias="from", description="Start date for the last changed status, in ISO 8601 format")
    to: str = Field(default="", description="End date for the last changed status, in ISO 8601 format")

    model_config = {
        "populate_by_name": True
    }

class Filter(BaseModel):
    delivery_method_id: List[str] = Field(default_factory=list, description="List of delivery method IDs to filter by")
    is_quantum: Optional[bool] = Field(default=None, description="Whether the delivery method is quantum or not")
    last_changed_status_date: LastChangedStatusDate = Field(default=None, description="Last changed status date")
    order_id: int = Field(default=0, description="Order ID to filter by")
    provider_id: List[str] = Field(default_factory=list, description="List of provider IDs to filter by")
    since: str # ISO 8601 format, e.g. "2025-10-01T00:00:00Z" -- год-месяц-деньTчасы:минуты:секундыZ
    status_alias: List[str] = Field(default_factory=list, description="List of status aliases to filter by")
    to: str # ISO 8601 format, e.g. "2025-10-01T00:00:00Z" -- год-месяц-деньTчасы:минуты:секундыZ
    warehouse_id: List[str] = Field(default_factory=list, description="List of warehouse IDs to filter by")


class With(BaseModel):
    analytics_data: bool = Field(default=False, description="Whether to include analytics data or not")
    barcodes: bool = Field(default=False, description="Whether to include barcodes or not")
    financial_data: bool = Field( default=True, description="Whether to include financial data or not")
    translit: bool = Field(default=False, description="Whether to include transliterated data or not")


class PostingRequestSchema(BaseModel):
    dir: str = Field(default="asc", description="Direction of sorting results, either 'asc' or 'desc'")
    filter: Filter = Field(
        default_factory=lambda: Filter(
            delivery_method_id=[],
            is_quantum=False,
            last_changed_status_date=LastChangedStatusDate(),
            order_id=0,
            provider_id=[],
            since="",
            status_alias=[],
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

# {
#   "result": {
#     "postings": [
#       {
#         "posting_number": "05708065-0029-1",
#         "order_id": 680420041,
#         "order_number": "05708065-0029",
#         "pickup_code_verified_at": "2025-01-17T10:59:26.614Z",
#         "status": "awaiting_deliver",
#         "substatus": "posting_awaiting_passport_data",
#         "delivery_method": {
#           "id": 21321684811000,
#           "name": "Ozon Логистика самостоятельно, Красногорск",
#           "warehouse_id": 21321684811000,
#           "warehouse": "Стим Тойс Нахабино",
#           "tpl_provider_id": 24,
#           "tpl_provider": "Ozon Логистика"
#         },
#         "tracking_number": "",
#         "tpl_integration_type": "ozon",
#         "in_process_at": "2022-05-13T07:07:32Z",
#         "shipment_date": "2022-05-13T10:00:00Z",
#         "delivering_date": null,
#         "optional": {
#           "products_with_possible_mandatory_mark": [
#             0
#           ]
#         },
#         "cancellation": {
#           "cancel_reason_id": 0,
#           "cancel_reason": "",
#           "cancellation_type": "",
#           "cancelled_after_ship": false,
#           "affect_cancellation_rating": false,
#           "cancellation_initiator": ""
#         },
#         "customer": null,
#         "products": [
#           {
#             "price": "1390.000000",
#             "currency_code": "RUB",
#             "is_blr_traceable": true,
#             "is_marketplace_buyout": true,
#             "offer_id": "205953",
#             "name": " Электронный конструктор PinLab Позитроник",
#             "sku": 358924380,
#             "quantity": 1
#           }
#         ],
#         "addressee": null,
#         "barcodes": null,
#         "analytics_data": null,
#         "financial_data": null,
#         "is_express": false,
#         "legal_info": {
#           "company_name": "string",
#           "inn": "string",
#           "kpp": "string"
#         },
#         "quantum_id": 0,
#         "requirements": {
#           "products_requiring_change_country": [],
#           "products_requiring_rnpt": []
#           "products_requiring_gtd": [],
#           "products_requiring_country": [],
#           "products_requiring_mandatory_mark": [],
#           "products_requiring_jwn": []
#         },
#         "tariffication": [
#           {
#             "current_tariff_rate": 0,
#             "current_tariff_type": "",
#             "current_tariff_charge": "",
#             "current_tariff_charge_currency_code": "",
#             "next_tariff_rate": 0,
#             "next_tariff_type": "",
#             "next_tariff_charge": "",
#             "next_tariff_starts_at": "2023-11-13T08:05:57.657Z",
#             "next_tariff_charge_currency_code": ""
#           }
#         ]
#       }
#     ],
#     "has_next": true
#   }
# }

# "optional": {
#           "products_with_possible_mandatory_mark": [
#             0
#           ]


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
    offer_id: str = Field(..., description="Offer ID for the product")
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

class OzonAPIError(RuntimeError):
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

extracted_sellers = extract_sellers()  # Expose the function for external use