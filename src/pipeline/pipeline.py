import asyncio
import logging

from botocore.client import BaseClient

from settings import proj_settings
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_client import OzonClient
from src.clients.ozon.schemas import SellerAccount
from src.mappers.transformation_functions import collect_stats, enrich_acc_context, \
    remove_archived_skus, check_orders_titles
from src.pipeline.pipeline_steps import get_sheets_data, get_pipeline_ctx, get_account_postings, \
    get_account_analytics_data, get_account_remainders_skus
from src.services.backup import BackupService
from src.services.google_sheets import GoogleSheets
from src.pipeline.pipeline_settings import *


BASE_TOP_SHEET_TITLES: list[str] = proj_settings.GOOGLE_BASE_TOP_SHEET_TITLES.split(',')
BASE_SHEETS_TITLES_BY_ACC: list[str] = proj_settings.GOOGLE_BASE_SHEETS_TITLES_BY_ACC.split(',')

log = logging.getLogger("pipeline")

async def run_pipeline(*,s3_cli: BaseClient,
                       ozon_cli: OzonClient,
                       sheets_cli: SheetsCli,
                       accounts: list[SellerAccount],
                       date_since: str,
                       date_to: str,
                       analytics_months: list,
                       bucket_name: str):

    # получаем данные из Google Sheets
    google_sheets = GoogleSheets(cli=sheets_cli)
    sheets_data = await get_sheets_data(google_sheets)
    existed_sheets = sheets_data.existed_sheets
    extracted_data = sheets_data.extracted_values
    table_data_for_backup = sheets_data.table_data_for_backup

    # объявляем и инициализируем бекап сервис
    backup_service = BackupService(bucket_name=bucket_name,
                                   cli=s3_cli)
    # делаем бекап таблицы с прошлой недели, возвращает хеш тег объекта если все ок
    # мб понадобиться позже пока не использую
    if table_data_for_backup:
        await backup_service.save_parquet(table_data_for_backup)

    # формируем пайплайн контекст для каждого аккаунта для асинхронной выгрузки данных
    pipeline_context = await get_pipeline_ctx(ozon_cli=ozon_cli,
                                              accounts=accounts,
                                              existed_sheets=existed_sheets,
                                              extracted_data=extracted_data,
                                              sheets_serv=google_sheets,
                                              date_since=date_since,
                                              date_to=date_to)

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
    await remove_archived_skus(acc_remainders=acc_remainders,
                               all_analytics=all_analytics)

    # собираем всю инфу о контексте аккаунта, заявках, остатках, аналитике
    acc_stats = [await collect_stats(p, r, a) for p, r, a in zip(acc_postings, acc_remainders, all_analytics)]

    for acc_d in acc_stats:
        p_settings: PipelineSettings = acc_d[0]
        postings = acc_d[1]
        remainders = acc_d[2]
        analytics = acc_d[3]
        p_settings.clusters_names, p_settings.sheet_titles = await enrich_acc_context(BASE_TOP_SHEET_TITLES,
                                                                                      remainders,
                                                                                      analytics_months)
        l = ""
