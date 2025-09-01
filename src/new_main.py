import asyncio
import logging

from google.auth.transport.requests import Request

from settings import proj_settings
from src.clients.google_sheets.sheets_cli import SheetsCli

from google.oauth2.service_account import Credentials

from src.clients.ozon import ozon_cli
from src.clients.ozon.ozon_client import OzonClient
from src.domain.seller_accounts import extract_sellers
from src.pipeline.pipeline import run_pipeline


async def main():
    # Инициализация клиента Google Sheets
    scopes = proj_settings.SERVICE_SCOPES.split(',')
    path_to_credentials = proj_settings.PATH_TO_CREDENTIALS
    spreadsheet_id = proj_settings.GOOGLE_SPREADSHEET_ID
    sheets_cli = SheetsCli(spreadsheet_id=spreadsheet_id,
                           scopes=scopes,
                           path_to_credentials=path_to_credentials)
    creds = Credentials.from_service_account_file(path_to_credentials,
                                                  scopes=scopes)
    # обновляем access token чтобы не авторизоваться заново
    creds.refresh(Request())

    # Инициализация клиента Ozon API
    fbs_reports_url = proj_settings.FBS_POSTINGS_REPORT_URL
    fbo_reports_url = proj_settings.FBO_POSTINGS_REPORT_URL
    base_url = proj_settings.OZON_BASE_URL
    remain_url = proj_settings.OZON_REMAINS_URL
    ozon_client = OzonClient(fbs_reports_url=fbs_reports_url,
                             fbo_reports_url=fbo_reports_url,
                             base_url=base_url,
                             remain_url=remain_url)

    # Инициализация логгера
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("ozon")

    # получаем данные из Google Sheets
    existed_sheets = await sheets_cli.get_sheets_info()
    sheets_names = list(existed_sheets.keys())
    extracted_dates = await sheets_cli.read_table(range_table=sheets_names)

    # получаем аккаунты
    client_ids = proj_settings.OZON_CLIENT_IDS.split(',')
    api_keys = proj_settings.OZON_API_KEYS.split(',')
    names = proj_settings.OZON_NAME_LK.split(',')
    extracted_sellers = extract_sellers(client_ids,
                                        api_keys,
                                        names)

    await run_pipeline(ozon_cli=ozon_client,
                       sheets_cli=sheets_cli,
                       accounts=extracted_sellers)

if __name__ == "__main__":
    asyncio.run(main())


