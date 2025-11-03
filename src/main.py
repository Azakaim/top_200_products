import asyncio
import logging
import boto3

from google.auth.transport.requests import Request

from settings import proj_settings
from src.clients.google_sheets.sheets_cli import SheetsCli

from google.oauth2.service_account import Credentials

from src.clients.onec.onec_cli import OneCClient
from src.clients.ozon.ozon_client import OzonClient
from src.domain.seller_accounts import extract_sellers
from src.mappers import get_week_range
from src.pipeline.pipeline import run_pipeline


async def setup_logging():
    # Инициализация логгера
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

async def main():
    # Инициализация логгера
    await setup_logging()

    # Если сегодня не вторник, то не обновляем таблицу
    if not True: # await is_tuesday_today(): #TODO убрать заглушку и будет обновляться только раз в неделю во вт
        log.info("Tuesday not today")
        return None

    # даты обработки доставок
    week_range = await get_week_range()
    since = week_range[0]
    until = week_range[1]

    # месяца сбора аналитики
    analytics_months = proj_settings.ANALYTICS_MONTHS.split(',')

    # инициализация s3
    bucket_name = proj_settings.BUCKET_NAME
    s3_cli = boto3.client(
        's3',
        aws_access_key_id=proj_settings.S3_ACCESS_KEY,
        aws_secret_access_key=proj_settings.S3_SECRET_KEY,
        endpoint_url=proj_settings.S3_ENDPOINT,
    )

    # Инициализация клиента 1 С
    oc_host= proj_settings.ONEC_HOST
    oc_endpoints = proj_settings.ONEC_ENDPOINTS.split(',')
    prod_uid_url = oc_endpoints[0]
    stocks_url = oc_endpoints[1]
    userpass = proj_settings.ONEC_LOGIN_PASS
    oc_headers = proj_settings.ONEC_HEADERS.split(',')
    cont_type_onec_headers = {oc_headers[0].split(':')[0]: oc_headers[0].split(':')[1]}
    auth_onec = {oc_headers[1].split(':')[0]: oc_headers[1].split(':')[1]}
    auth_onec.update(cont_type_onec_headers)
    one_c = OneCClient(
        base_url=oc_host,
        prod_uid_url=prod_uid_url,
        stocks_url=stocks_url,
        headers=auth_onec,
        userpass=userpass,
        concurrency=100,  # количество параллельных запросов
        default_rps=5 # 5 запросов в сек от 5 до 10 к 1С
    )

    # Инициализация клиента Google Sheets
    scopes = proj_settings.SERVICE_SCOPES.split(',')
    path_to_credentials = proj_settings.PATH_TO_CREDENTIALS
    spreadsheet_id = proj_settings.GOOGLE_SPREADSHEET_ID
    sheets_base_title = proj_settings.GOOGLE_BASE_TOP_SHEET_TITLES.split(',')
    sheets_client = SheetsCli(spreadsheet_id=spreadsheet_id,
                              scopes=scopes,
                              path_to_credentials=path_to_credentials,
                              sheets_base_title=sheets_base_title)

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

    # получаем аккаунты
    client_ids = proj_settings.OZON_CLIENT_IDS.split(',')
    api_keys = proj_settings.OZON_API_KEYS.split(',')
    names = proj_settings.OZON_NAME_LK.split(',')
    extracted_sellers = extract_sellers(client_ids,
                                        api_keys,
                                        names)

    await run_pipeline(onec=one_c,
                       s3_cli=s3_cli,
                       ozon_cli=ozon_client,
                       sheets_cli=sheets_client,
                       accounts=extracted_sellers,
                       date_since=since,
                       date_to=until,
                       analytics_month_names=analytics_months,
                       bucket_name=bucket_name)


if __name__ == "__main__":
    asyncio.run(main())
