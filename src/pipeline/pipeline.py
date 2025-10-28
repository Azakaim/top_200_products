import asyncio
import logging
import tracemalloc

from botocore.client import BaseClient

from settings import proj_settings
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.onec.onec_cli import OneCClient
from src.clients.ozon.ozon_client import OzonClient
from src.schemas.ozon_schemas import SellerAccount
from src.mappers.transformation_functions import collect_stats, enrich_acc_context, \
    remove_archived_skus, collect_common_stats, collect_top_products_sheets_values_range, \
    get_handling_period, collect_account_auxiliary_table_values
from src.pipeline.pipeline_steps import get_sheets_data, get_pipeline_ctx, get_account_postings, \
    get_account_analytics_data, get_account_remainders_skus, get_onec_products
from src.services.backup import BackupService
from src.services.google_sheets import GoogleSheets
from src.services.onec import OneCService

# считаем сколько памяти занимают вычисления
tracemalloc.start()

BASE_TOP_SHEET_TITLES: list[str] = proj_settings.GOOGLE_BASE_TOP_SHEET_TITLES.split(',')
BASE_SHEETS_TITLES_BY_ACC: list[str] = proj_settings.GOOGLE_BASE_SHEETS_TITLES_BY_ACC.split(',')

log = logging.getLogger("pipeline")

async def run_pipeline(*, onec: OneCClient,
                       s3_cli: BaseClient,
                       ozon_cli: OzonClient,
                       sheets_cli: SheetsCli,
                       accounts: list[SellerAccount],
                       date_since: str,
                       date_to: str,
                       analytics_month_names: list,
                       bucket_name: str):

    onec_serv = OneCService(cli=onec)

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
                                              sheets_serv=google_sheets)
    # получаем точный период для дат и недели и месяца
    month_period = await get_handling_period(months=analytics_month_names)
    week_period = await get_handling_period(months=[date_since, date_to])

    # коллектим периоды
    period: list = [week_period]
    period.extend(month_period)

    # получаем параллельно остатки и доставки с каждого кабинета
    all_postings_task = [get_account_postings(ctxt, period) for ctxt in pipeline_context]
    remainders_tasks = [get_account_remainders_skus(ctxt) for ctxt in pipeline_context]
    analytics_tasks = [get_account_analytics_data(ctxt, month_period) for ctxt in pipeline_context]
    onec_tasks = [get_onec_products(onec_serv=onec_serv)]

    all_acc_postings, acc_remainders, all_analytics, onec_products = await asyncio.gather(
        asyncio.gather(*all_postings_task),
        asyncio.gather(*remainders_tasks),
        asyncio.gather(*analytics_tasks),
        asyncio.gather(*onec_tasks)
    )

    # коллектим все продукты из 1С
    onec_products_info = [p for p in onec_products[0].onec_products]

    # убираем архивные sku
    await remove_archived_skus(acc_remainders=acc_remainders,
                               all_analytics=all_analytics)

    # собираем всю инфу о контексте аккаунта, заявках, остатках, аналитике
    acc_stats = [await collect_stats(p, r, a, onec_products_info) for p, r, a in zip(all_acc_postings,
                                                                                     acc_remainders,
                                                                                     all_analytics)]

    # собираем общие данные по компании
    collected_stats = await collect_common_stats(onec_products_info,
                                                 acc_stats,
                                                 months_counter=len(analytics_month_names))

    # Собираем значения для топ продуктов (список списков для Google Sheets)
    top_products_values, cluster_count = await collect_top_products_sheets_values_range(collected_stats,
                                                                                        BASE_TOP_SHEET_TITLES,
                                                                                        analytics_month_names,
                                                                                        date_since,
                                                                                        date_to)

    log.info(f"Собрано {len(top_products_values)} строк для таблицы топ продуктов")

    # Записываем топ продукты в Google Sheets
    await google_sheets.push_top_products_to_sheet(
        sheet_name="Top Products",
        values=top_products_values
    )

    # Форматируем таблицу топ продуктов
    await google_sheets.format_top_products_table(
        sheet_name="Top Products",
        values=top_products_values,
        cluster_count=cluster_count
    )

    # Записываем данные во вспомогательные таблицы для каждого кабинета
    log.info("Начинаем запись вспомогательных таблиц по кабинетам")

    for acc_d in acc_stats:
        # собираем заголовки для вспомогательных таблиц отображаемых покабинетно
        acc_d.ctx.clusters_names, acc_d.ctx.sheet_titles = await enrich_acc_context(BASE_SHEETS_TITLES_BY_ACC,
                                                                                    acc_d.remainders)

        # Собираем данные для вспомогательной таблицы по кабинету
        auxiliary_table_values = await collect_account_auxiliary_table_values(
            base_titles=BASE_SHEETS_TITLES_BY_ACC,
            remainders=acc_d.remainders,
            postings=acc_d.postings,
            clusters_names=acc_d.ctx.clusters_names,
            date_since=date_since,
            date_to=date_to
        )

        # Записываем данные в Google Sheets
        await google_sheets.push_auxiliary_table_to_sheet(
            sheet_name=acc_d.ctx.account_name,
            values=auxiliary_table_values
        )

        # Форматируем таблицу
        await google_sheets.format_auxiliary_table(
            sheet_name=acc_d.ctx.account_name,
            values=auxiliary_table_values,
            cluster_count=len(acc_d.ctx.clusters_names)
        )

        log.info(f"Записана и отформатирована вспомогательная таблица для кабинета '{acc_d.ctx.account_name}'")

    current, peak = tracemalloc.get_traced_memory()
    log.info(f"Текущая память: {current / 1024 / 1024:.2f} MB; Пик: {peak / 1024 / 1024:.2f} MB")

    tracemalloc.stop()
