import asyncio
from typing import Optional

from pydantic import BaseModel

from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_client import OzonClient, OzonCliBound
from src.clients.ozon.schemas import SellerAccount
from src.mappers.transformation_functions import collect_stats, enrich_acc_context
from src.services.google_sheets import GoogleSheets
from src.services.ozon import OzonService

BASE_SHEETS_TITLES: list[str] = ["Модель", "SKU", "Наименование",
                                 "Цена", "Статус", "В заявке",
                                 "Дата от", "Дата до",
                                 "Дата обновления"]

class PipelineSettings(BaseModel):
    model_config = {
        "arbitrary_types_allowed": True
    }

    ozon: OzonCliBound
    google_sheets: GoogleSheets
    sheet_titles: Optional[list[str]] = None
    clusters_names: Optional[list[str]] = None
    values_range: Optional[list[list[str]]] = None
    account_id: str
    account_name: str
    account_api_key: str
    since: str
    to: str
    clear_scope_range: Optional[str] = ""


async def get_account_postings(context: PipelineSettings):
    ozon_service = OzonService(cli=context.ozon)
    try:
        postings = await ozon_service.fetch_postings(account_name=context.account_name,
                                                     account_id=context.account_id,
                                                     date_since=context.since,
                                                     date_to=context.to)
    finally:
        pass
    return context, postings

async def get_account_remainders(context: PipelineSettings):
    ozon_service = OzonService(cli=context.ozon)
    try:
        skus = await ozon_service.collect_skus()
        remainders = await ozon_service.get_remainders(skus=skus)
    finally:
        pass
    return context, remainders

async def run_pipeline(*, ozon_cli: OzonClient,
                       sheets_cli: SheetsCli,
                       accounts: list[SellerAccount],
                       date_since: str,
                       date_to: str):
    # получаем данные из Google Sheets
    google_sheets = GoogleSheets(cli=sheets_cli)
    existed_sheets = await google_sheets.get_identity_sheets()
    extracted_dates = await google_sheets.fetch_info()
    pipeline_context = []
    for acc in accounts:
        headers = {
                "Client-Id": acc.client_id,
                "Api-Key": acc.api_key ,
                "Content-Type": "application/json",
            }
        ozon_client = OzonCliBound(base=ozon_cli,
                                   headers=headers)

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

        # может быть пустым т.к нечего очищать на только что созданном листе
        clear_scope_range = next((
            sheet_name.range
            for sheet_name in extracted_dates
            if sheet_name.range.split('!')[0] == acc.name
        ), None)

        pipeline_context.append(PipelineSettings(
                ozon=ozon_client,
                google_sheets=google_sheets,
                values_range=account_table_data,
                account_name=acc.name,
                account_id=acc.client_id,
                account_api_key=acc.api_key,
                since=date_since,
                to=date_to,
                clear_scope_range=clear_scope_range
        ))

    # получаем параллельно остатки и доставки с каждого кабинета
    postings_tasks = [asyncio.create_task(get_account_postings(ctxt)) for ctxt in pipeline_context]
    remainders_tasks = [asyncio.create_task(get_account_remainders(ctxt)) for ctxt in pipeline_context]

    acc_postings, acc_remainders = await asyncio.gather(
        asyncio.gather(*postings_tasks),
        asyncio.gather(*remainders_tasks)
    )

    # собираем всю инфу о заявках, остатках и контексте аккаунта
    acc_stats = [await collect_stats(p, r) for p, r in zip(acc_postings, acc_remainders)]

    for acc_d in acc_stats:
        await enrich_acc_context(acc_d[0],)