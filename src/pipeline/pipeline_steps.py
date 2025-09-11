import asyncio

from src.clients.google_sheets.schemas import SheetsValuesOut
from src.clients.ozon.ozon_bound_client import OzonCliBound
from src.clients.ozon.ozon_client import OzonClient
from src.clients.ozon.schemas import SellerAccount
from src.mappers import get_converted_date
from src.dto.dto import SheetsData
from src.pipeline.pipeline_settings import PipelineSettings, PipelineCxt
from src.services.google_sheets import GoogleSheets
from src.services.ozon import OzonService


async def get_sheets_data(sheets_serv: GoogleSheets) -> SheetsData | None:
    """
    Получение данных из Google Sheets.

    Args:
        sheets_serv (GoogleSheets): сервис для работы с Google Sheets.

    Returns:
        SheetsData | None:
            - existed_sheets (dict[str, str]): существующие таблицы;
            - extracted_values (list[SheetsValuesOut]): извлечённые данные из листа;
            - table_data_for_backup (dict): данные всей таблицы для бэкапа.
    """
    # получаем данные из Google Sheets
    existed_sheets = await sheets_serv.get_identity_sheets()
    extracted_data, table_data_for_backup = await sheets_serv.fetch_info()
    return SheetsData(
        existed_sheets=existed_sheets,
        extracted_values=extracted_data,
        table_data_for_backup=table_data_for_backup,
    )

async def get_pipeline_ctx(ozon_cli: OzonClient,
                           accounts: list[SellerAccount],
                           existed_sheets: dict[str, str],
                           extracted_data: list[SheetsValuesOut],
                           sheets_serv: GoogleSheets,
                           date_since:str,
                           date_to: str) -> list[PipelineCxt] | None:
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
        is_today_updating, account_table_data = await sheets_serv.check_data_update(acc.name,
                                                                                    extracted_dates=extracted_data,
                                                                                    sheet_id=sheet_id)

        # может быть пустым т.к нечего очищать на только что созданном листе
        clear_scope_range = next((
            sheet_name.range
            for sheet_name in extracted_data
            if sheet_name.range.split('!')[0] == acc.name
        ), None)

        # настраиваем контекст и контекст-клиента
        pipeline_settings = PipelineSettings(
            values_range=account_table_data,
            account_name=acc.name,
            account_id=acc.client_id,
            account_api_key=acc.api_key,
            since=date_since,
            to=date_to,
            clear_scope_range=clear_scope_range
        )
        pipeline_cli = PipelineCxt(cxt_config=pipeline_settings,
                                       ozon=ozon_client)
        pipeline_context.append(pipeline_cli)
    return pipeline_context

async def get_account_analytics_data(context: PipelineCxt, analytics_months: list):
    ozon_service = OzonService(cli=context.ozon)
    converted_date_since = await get_converted_date(analytics_months)
    try:
        _tasks = [asyncio.create_task(
            ozon_service.collect_analytics_data(month_name=mname,
                                                date_since=val[0],
                                                date_to=val[1])
        )
            for mname, val in converted_date_since.items()
        ]
        analytics_data= await asyncio.gather(*_tasks)
    finally:
        pass
    return context.cxt_config, analytics_data

async def get_account_postings(context: PipelineCxt):
    ozon_service = OzonService(cli=context.ozon)
    try:
        postings = await ozon_service.fetch_postings(account_name=context.cxt_config.account_name,
                                                     account_id=context.cxt_config.account_id,
                                                     date_since=context.cxt_config.since,
                                                     date_to=context.cxt_config.to)
    finally:
        pass
    return context.cxt_config, postings

async def get_account_remainders_skus(context: PipelineCxt):
    ozon_service = OzonService(cli=context.ozon)
    try:
        skus = await ozon_service.collect_skus()
        remainders = await ozon_service.get_remainders(skus=skus)
    finally:
        pass
    return context.cxt_config, remainders, skus