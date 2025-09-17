import asyncio
import logging
from itertools import chain
from typing import List

from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials

from settings import proj_settings
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_cli import OzonCli
from src.schemas.ozon_schemas import APIError, SellerAccount, Remainder
from src.schemas.google_sheets_schemas import SheetsValuesOut
from src.services.reports_pipeline import push_to_sheets, PipelineContext, fetch_postings, check_date_update, \
    get_remainders

# domain/seller_accounts
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

async def save_context(context: PipelineContext):
    # обертка для сохранения контекста
    try:
        res = await fetch_postings(context)
    except Exception as e:
        return context, e
    return context, res

async def get_remainders_by_account(context: PipelineContext, postings):
    # обертка для асинхронного получения остатков сохранением айди аккаунта
    try:
        rems = await get_remainders(context, postings)
    except Exception as e:
        return context, e
    return {context.account_id: rems}

async def collect_titles(*, base_titles: List[str], clusters_names: List[str]) -> List[str]:
    titles = base_titles[:6] + clusters_names + base_titles[6:]
    return titles

async def collect_clusters_names(remainders: List[Remainder]):
    # собираем все имена кластеров
    clusters_names = list(
        set([r.cluster_name for r in remainders
             if (r.cluster_name != "")]))
    return clusters_names


async def main() -> None:
    # Инициализация клиента Google Sheets
    scopes = proj_settings.SERVICE_SCOPES.split(',')
    path_to_credentials = proj_settings.PATH_TO_CREDENTIALS
    spreadsheet_id = proj_settings.GOOGLE_SPREADSHEET_ID
    sheets_cli = SheetsCli(spreadsheet_id=spreadsheet_id,
                           scopes=scopes,
                           path_to_credentials=path_to_credentials)
    creds = Credentials.from_service_account_file(path_to_credentials,
                                                   scopes=scopes)
    creds.refresh(Request())

    # Инициализация клиента Ozon API
    fbs_reports_url = proj_settings.OZON_FBS_POSTINGS_REPORT_URL
    fbo_reports_url = proj_settings.OZON_FBO_POSTINGS_REPORT_URL
    base_url = proj_settings.OZON_BASE_URL
    remain_url = proj_settings.OZON_REMAINS_URL


    # получаем данные из Google Sheets
    existed_sheets = await sheets_cli.get_sheets_info()
    sheets_names = list(existed_sheets.keys())
    re = await sheets_cli.read_value_ranges(range_table=sheets_names)
    extracted_dates = [SheetsValuesOut.model_validate(r) for r in re.valueRanges]
    extracted_sellers = extract_sellers()
    pipline_contexts = []
    base_sheets_titles: List[str] = ["Модель", "SKU", "Наименование",
                               "Цена", "Статус", "В заявке",
                               "Дата от", "Дата до", "Дата обновления"]
    try:
        for acc in extracted_sellers:
            # для каждого клиента TODO: подумать как лучше оптимизировать
            ozon_client = OzonCli(
                fbs_reports_url=fbs_reports_url,
                fbo_reports_url=fbo_reports_url,
                base_url=base_url,
                remain_url=remain_url)
            ozon_client.headers = {
                "client_id": acc.client_id,
                "api_key": acc.api_key,
                "content_type": "application/json"
            }
            # Проверяем, существует ли лист с таким названием
            # Добавляем новый лист в таблицу
            # Получаем ID нового листа
            # Берем значения из таблицы в соответствии с именем листа и кабинета
            # Проверяем, существует ли лист с таким названием
            sheet_id = {acc.name: ""}
            if acc.name in existed_sheets:
                sheet_id ={acc.name: existed_sheets[acc.name]}
            is_today_updating, account_table_data = await check_date_update(acc.name,
                                                        sheets_cli=sheets_cli,
                                                        extracted_dates=extracted_dates,
                                                        sheet_id=sheet_id)
            # Если сегодня, то не обновляем таблицу
            if is_today_updating:
                continue

            range_for_clear = next((ed.range for ed in extracted_dates if ed.range.split('!')[0] == acc.name),None)

            pipline_context = PipelineContext(
                ozon_client=ozon_client,
                sheets_cli=sheets_cli,
                values_range=account_table_data,
                account_name=acc.name,
                account_id=acc.client_id,
                account_api_key=acc.api_key,
                since=proj_settings.DATE_SINCE,
                to=proj_settings.DATE_TO,
                range_for_clear=range_for_clear
            )
            pipline_contexts.append(pipline_context)
        tasks = [asyncio.create_task(save_context(ctx)) for ctx in pipline_contexts]

        all_ctx_postings = await asyncio.gather(*tasks)

        # собираем отправления по всем кабинетам
        fbo_postings = [
            (ctx, next((v for k, v in post.items() if "FBO" in k),None))
            for ctx, post in all_ctx_postings
            ]

        # получаем FBO остатки с кабинета Озон
        remainders_by_accounts = await asyncio.gather(*(get_remainders_by_account(context=context, postings=postings) for context, postings in fbo_postings if postings))

        # закрываем клиентов Озон
        await asyncio.gather(*(context.ozon_client.aclose() for context, _ in all_ctx_postings))

        # создаем заголовки для гугл таблицы
        remainders_batches = [r for d in remainders_by_accounts for r in d.values()]
        remainders = list(chain.from_iterable(remainders_batches))

        clusters_names = await collect_clusters_names(remainders=remainders)
        titles = await collect_titles(base_titles=base_sheets_titles,clusters_names=clusters_names)

        # пушим в таблицу
        for context_postings in all_ctx_postings:
            ctx = context_postings[0]
            ctx.sheet_titles = titles
            ctx.clusters_names = clusters_names
            postings_acc = context_postings[1]
            # остатки для определенного кабинета
            print(ctx.account_name)
            remainders = next(
            (v for d in remainders_by_accounts for k, v in d.items() if k in ctx.account_id),
            None
            )
            await push_to_sheets(context=ctx,
                                 postings=postings_acc,
                                 remainders=remainders)

    except APIError as e:
        print(f"Ошибка при обращении к Ozon API: {e.status} {e.endpoint} - {e.body}")


if __name__ == '__main__':
    asyncio.run(main())


