import asyncio

from src.schemas.google_sheets_schemas import  SheetsValuesOut
from src.clients.ozon.ozon_bound_client import OzonCliBound
from src.clients.ozon.ozon_client import OzonClient
from src.schemas.onec_schemas import OneCProductsResults, OneCNomenclatureCollection
from src.schemas.ozon_schemas import SellerAccount
from src.infrastructure.cache import cache
from src.dto.dto import SheetsData, AccountStatsRemainders, AccountStatsPostings, \
    AccountStatsAnalytics, Period
from src.mappers.transformation_functions import parse_obj_by_type_base_cls, collect_onec_product_info
from src.pipeline.pipeline_settings import PipelineSettings, PipelineCxt
from src.services.google_sheets import GoogleSheets
from src.services.onec import OneCService
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
                           existed_sheets: dict[str, int],
                           extracted_data: list[SheetsValuesOut],
                           sheets_serv: GoogleSheets) -> list[PipelineCxt] | None:
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
            clear_scope_range=clear_scope_range
        )
        pipeline_cli = PipelineCxt(cxt_config=pipeline_settings,
                                   ozon=ozon_client)
        pipeline_context.append(pipeline_cli)
    return pipeline_context

async def get_onec_products(onec_serv: OneCService):
    key_cache = f"common:onec-products:OneCNomenclatureCollection"
    work_cache = await cache.get(key_cache)
    if work_cache is not None:  # проверка на None потому что мож храниться пустая строка и 0
        return await parse_obj_by_type_base_cls(work_cache, OneCNomenclatureCollection)
    onec_products, onec_articles = await onec_serv.run_onec_pipeline()
    onec_nomenclatures = await collect_onec_product_info(onec_products, onec_articles)
    # кэшируем
    await cache.set(key_cache, onec_nomenclatures.model_dump_json(), ex=86400)
    return onec_nomenclatures

async def get_account_analytics_data(context: PipelineCxt, periods: list[Period]):
    key_cache = f"{context.cxt_config.account_id}-acc-id:ozon-postings:AccountStatsAnalytics"
    work_cache = await cache.get(key_cache)
    if work_cache is not None:  # проверка на None потому что мож храниться пустая строка и 0
        return await parse_obj_by_type_base_cls(work_cache, AccountStatsAnalytics)
    ozon_service = OzonService(cli=context.ozon)
    try:
        _tasks = [asyncio.create_task(
            ozon_service.collect_analytics_data(month_name=period.month_name,
                                                date_since=period.start_date,
                                                date_to=period.end_date)
        )
            for period in periods
        ]
        analytics_data= await asyncio.gather(*_tasks)
    finally:
        pass
    analytic_stats = AccountStatsAnalytics(ctx=context.cxt_config,
                                           monthly_analytics=analytics_data)
    await cache.set(key_cache, analytic_stats.model_dump_json(), ex=86400)
    return analytic_stats

async def get_account_postings(context: PipelineCxt,
                               periods: list[Period]) :
    key_cache = (f"{context.cxt_config.account_id}"
                 f"-acc-id:ozon-postings:AccountStatsPostings:")
    work_cache = await cache.get(key_cache)
    if work_cache is not None:  # проверка на None потому что мож храниться пустая строка и 0
        return await parse_obj_by_type_base_cls(work_cache, AccountStatsPostings)
    # делаем таски
    _tasks = []
    for period in periods:
        ozon_service = OzonService(cli=context.ozon)
        _tasks.append(ozon_service.fetch_postings(account_name=context.cxt_config.account_name,
                                                  period=period))
    postings = await asyncio.gather(*_tasks)
    acc_stats_postings = AccountStatsPostings(ctx=context.cxt_config,
                                              postings=postings)
    await cache.set(key_cache, acc_stats_postings.model_dump_json(), ex=86400)  # кэш на сутки
    return acc_stats_postings

async def get_account_remainders_skus(context: PipelineCxt):
    key_cache = f"{context.cxt_config.account_id}-acc-id:ozon-remainders:AccountStatsRemainders"
    work_cache = await cache.get(key_cache)
    if work_cache is not None:
        return await parse_obj_by_type_base_cls(work_cache, AccountStatsRemainders)
    ozon_service = OzonService(cli=context.ozon)
    try:
        skus = await ozon_service.collect_skus()
        remainders = await ozon_service.get_remainders(skus=skus)
    finally:
        pass
    stats_remainders = AccountStatsRemainders(ctx=context.cxt_config,
                                              skus=skus,
                                              remainders=remainders)
    await cache.set(key_cache, stats_remainders.model_dump_json(), ex=86400) # кэш на сутки
    return stats_remainders
