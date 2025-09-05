import asyncio
import logging

from google.auth.transport.requests import Request

from settings import proj_settings
from src.clients.google_sheets.sheets_cli import SheetsCli

from google.oauth2.service_account import Credentials

from src.clients.ozon.ozon_client import OzonClient
from src.domain.seller_accounts import extract_sellers
from src.pipeline.pipeline import run_pipeline


async def main():
    # даты обработки доставок
    since = proj_settings.DATE_SINCE
    until = proj_settings.DATE_TO
    # месяца сбора аналитики
    analytics_months = proj_settings.ANALYTICS_MONTHS.split(',')
    # Инициализация клиента Google Sheets
    scopes = proj_settings.SERVICE_SCOPES.split(',')
    path_to_credentials = proj_settings.PATH_TO_CREDENTIALS
    spreadsheet_id = proj_settings.GOOGLE_SPREADSHEET_ID
    sheets_base_title = proj_settings.GOOGLE_SHEET_BASE_TITLES.split(',')
    sheets_client = SheetsCli(spreadsheet_id=spreadsheet_id,
                              scopes=scopes,
                              path_to_credentials=path_to_credentials,
                              sheets_base_title=sheets_base_title
                              )
    creds = Credentials.from_service_account_file(path_to_credentials,
                                                  scopes=scopes)
    # обновляем access token чтобы не авторизоваться заново
    creds.refresh(Request())

    # Инициализация клиента Ozon API
    fbs_reports_url = proj_settings.OZON_FBS_POSTINGS_REPORT_URL
    fbo_reports_url = proj_settings.OZON_FBO_POSTINGS_REPORT_URL
    base_url = proj_settings.OZON_BASE_URL
    remain_url = proj_settings.OZON_REMAINS_URL
    products_url = proj_settings.OZON_PRODUCTS_URL
    products_info_url = proj_settings.OZON_PRODUCTS_INFO_URL
    analytics_url = proj_settings.OZON_ANALYTICS_URL
    ozon_client = OzonClient(fbs_reports_url=fbs_reports_url,
                             fbo_reports_url=fbo_reports_url,
                             base_url=base_url,
                             remain_url=remain_url,
                             products_url=products_url,
                             products_whole_info_url=products_info_url,
                             analytics_url=analytics_url)

    # Инициализация логгера
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("ozon")

    # получаем аккаунты
    client_ids = proj_settings.OZON_CLIENT_IDS.split(',')
    api_keys = proj_settings.OZON_API_KEYS.split(',')
    names = proj_settings.OZON_NAME_LK.split(',')
    extracted_sellers = extract_sellers(client_ids,
                                        api_keys,
                                        names)

    await run_pipeline(ozon_cli=ozon_client,
                       sheets_cli=sheets_client,
                       accounts=extracted_sellers,
                       date_since=since,
                       date_to=until,
                       analytics_months=analytics_months)

if __name__ == "__main__":
    asyncio.run(main())


