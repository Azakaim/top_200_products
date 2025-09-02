from pydantic import BaseModel

from typing import Callable, Awaitable

from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_client import OzonClient, OzonCliBound
from src.clients.ozon.schemas import SellerAccount
from src.services.google_sheets import GoogleSheets
from src.services.ozon import OzonService

BASE_SHEETS_TITLES: list[str] = ["Модель", "SKU", "Наименование",
                                 "Цена", "Статус", "В заявке",
                                 "Дата от", "Дата до",
                                 "Дата обновления"]

class PipelineSettings(BaseModel):
    sheet_titles: list[str] = None
    clusters_names: list[str] = None
    values_range: list[list[str]]
    account_id: str
    account_name: str
    account_api_key: str
    since: str
    to: str
    clear_scope_range: str

async def get_account_stats(ozon: OzonService,
                            context: PipelineSettings,
                            getter: Callable[[str], Awaitable[list]],):
    try:
        account_stats = await getter()

async def run_pipeline(*, ozon_cli: OzonClient,
                       sheets_cli: SheetsCli,
                       accounts: list[SellerAccount],
                       date_since: str,
                       date_to: str):
    # объявляем класс озон сервиса
    ozon = OzonService()
    # получаем данные из Google Sheets
    google_sheets = GoogleSheets(cli=sheets_cli)
    existed_sheets = await google_sheets.get_identity_sheets()
    extracted_dates = await google_sheets.fetch_info()
    pipeline_context = []
    postings_task = []
    remainders_task = []
    for acc in accounts:
        headers = {
                "Client-Id": acc.client_id,
                "Api-Key": acc.api_key ,
                "Content-Type": "application/json",
            }
        ozon_client = OzonCliBound(base=ozon_cli,
                                   headers=headers)
        # Присваиваем клиента
        ozon.cli = ozon_client

        sheet_id = {acc.name: ""}
        if acc.name in existed_sheets:
            sheet_id = {acc.name: existed_sheets[acc.name]}
        is_today_updating, account_table_data = await google_sheets.check_date_update(acc.name,
                                                                        sheets_cli=sheets_cli,
                                                                        extracted_dates=extracted_dates,
                                                                        sheet_id=sheet_id)
        # Если сегодня, то не обновляем таблицу
        if is_today_updating:
            continue

        clear_scope_range = next((
            sheet_name.range
            for sheet_name in extracted_dates
            if sheet_name.range.split('!')[0] == acc.name
        ), None)

        pipeline_context.append(PipelineSettings(
                values_range=account_table_data,
                account_name=acc.name,
                account_id=acc.client_id,
                account_api_key=acc.api_key,
                since=date_since,
                to=date_to,
                clear_scope_range=clear_scope_range
        ))

        ozon.fetch_postings(account_name=acc.name,
                            account_id=acc.client_id,
                            date_since=date_since,
                            date_to=date_to)


