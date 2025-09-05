import asyncio

from settings import proj_settings
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_client import OzonClient
from src.clients.ozon.ozon_bound_client import OzonCliBound
from src.clients.ozon.schemas import SellerAccount
from src.mappers.transformation_functions import collect_stats, enrich_acc_context, get_converted_date, \
    remove_archived_skus
from src.pipeline.pipeline_settings import PipelineSettings, PipelineCxt
from src.services.google_sheets import GoogleSheets
from src.services.ozon import OzonService


BASE_SHEETS_TITLES: list[str] = proj_settings.GOOGLE_SHEET_BASE_TITLES.split(',')

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
    return analytics_data

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

async def run_pipeline(*, ozon_cli: OzonClient,
                       sheets_cli: SheetsCli,
                       accounts: list[SellerAccount],
                       date_since: str,
                       date_to: str,
                       analytics_months: list):
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
        # настраиваем контекст и клиента контекста
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

    # получаем параллельно остатки и доставки с каждого кабинета
    postings_tasks = [asyncio.create_task(get_account_postings(ctxt)) for ctxt in pipeline_context]
    remainders_tasks = [asyncio.create_task(get_account_remainders_skus(ctxt)) for ctxt in pipeline_context]
    analytics_tasks = [asyncio.create_task(get_account_analytics_data(ctxt, analytics_months)) for ctxt in pipeline_context]

    acc_postings, acc_remainders, all_analytics = await asyncio.gather(
        asyncio.gather(*postings_tasks),
        asyncio.gather(*remainders_tasks),
        asyncio.gather(*analytics_tasks)
    )

    # убираем архивные sku
    skus = acc_remainders[2]
    analytics = await remove_archived_skus(skus, all_analytics)

    # собираем всю инфу о заявках, остатках и контексте аккаунта
    acc_stats = [await collect_stats(p, r) for p, r in zip(acc_postings, acc_remainders)]

    for acc_d in acc_stats:
        remainders = acc_d[2]
        if isinstance(acc_d[0], PipelineSettings):
            p_settings: PipelineSettings = acc_d[0]
            p_settings.clusters_names, p_settings.sheet_titles = await enrich_acc_context(BASE_SHEETS_TITLES,
                                                                                          remainders)
        l = ""
