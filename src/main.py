import asyncio
import logging

from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials

from settings import proj_settings
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_cli import OzonClient
from src.clients.ozon.schemas import OzonAPIError, SellerAccount
from src.services.reports_pipeline import push_to_sheets, PipelineContext, fetch_postings, check_date_update, \
    get_remainders


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
    try:
        res = await fetch_postings(context)
    except Exception as e:
        return context, e
    return context, res

async def main() -> None:
    # Инициализация клиента Google Sheets
    scopes = proj_settings.SERVICE_SCOPES.split(',')
    path_to_credentials = proj_settings.PATH_TO_CREDENTIALS
    spreadsheet_id = proj_settings.GOOGLE_SPREADSHEET_ID
    range_updating_date = proj_settings.GOOGLE_SHEETS_DATE_UPDATING_RANGE
    sheets_cli = SheetsCli(spreadsheet_id=spreadsheet_id,
                           scopes=scopes,
                           path_to_credentials=path_to_credentials)
    creds = Credentials.from_service_account_file(path_to_credentials,
                                                   scopes=scopes)
    creds.refresh(Request())

    # Инициализация клиента Ozon API
    fbs_reports_url = proj_settings.FBS_POSTINGS_REPORT_URL
    fbo_reports_url = proj_settings.FBO_POSTINGS_REPORT_URL
    base_url = proj_settings.OZON_BASE_URL
    remain_url = proj_settings.OZON_REMAIN_URL
    # Инициализация логгера
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("ozon")

    # получаем данные из Google Sheets
    existed_sheets = await sheets_cli.get_sheets_info()
    sheets_names = list(existed_sheets.keys())
    extracted_dates = await sheets_cli.read_table(range_table=sheets_names)
    extracted_sellers = extract_sellers()
    pipline_contexts = []
    postings_by_accounts = []
    try:
        for acc in extracted_sellers:
            # для каждого клиента TODO: подумать как лучше оптимизировать
            ozon_client = OzonClient(
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
            is_today_updating, sheet_values_acc = await check_date_update(acc.name,
                                                        sheets_cli=sheets_cli,
                                                        extracted_dates=extracted_dates,
                                                        sheet_id=sheet_id)
            # Если сегодня, то не обновляем таблицу
            if is_today_updating:
                continue

            pipline_context = PipelineContext(
                ozon_client=ozon_client,
                sheets_cli=sheets_cli,
                values_range=sheet_values_acc,
                #postings=postings,
                account_name=acc.name,
                account_id=acc.client_id,
                account_api_key=acc.api_key,
                since=proj_settings.DATE_SINCE,
                to=proj_settings.DATE_TO,
                range_last_updating_date=range_updating_date,

            )
            pipline_contexts.append(pipline_context)
            # postings_by_accounts.append((pipline_contexts,fetch_postings(pipline_context)))
        tasks = [asyncio.create_task(save_context(ctx)) for ctx in pipline_contexts]
        result = await asyncio.gather(*tasks)

        fbo_postings = [
            (ctx, next((v for k, v in post.items() if "FBO" in k),None))
            for ctx, post in result
            ]

        # получаем остатки с кабинета Озон
        remainders = await asyncio.gather(*(get_remainders(context=context, postings=postings) for context, postings in fbo_postings))

        # закрываем клиентов
        await asyncio.gather(*(context.ozon_client.aclose() for context, _ in result))

        for context_postings in result:
            cntxt = context_postings[0]
            postings_acc = context_postings[1]
            await push_to_sheets(context=cntxt, postings=postings_acc)

    except OzonAPIError as e:
        print(f"Ошибка при обращении к Ozon API: {e.status} {e.endpoint} - {e.body}")


if __name__ == '__main__':
    asyncio.run(main())


