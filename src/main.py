import asyncio
from typing import AsyncGenerator
import logging

from settings import proj_settings
from src.clients.google_sheets.schemas import SheetsValuesInTo, RequestToTable, RepeatCellRequest, GridRange, CellData, \
    TextFormat, CellFormat, FieldPath, Body, Color
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_cli import OzonClient
from src.clients.ozon.schemas import extracted_sellers, OzonAPIError
import pandas as pd

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials

from src.services.reports_pipeline import push_to_sheets, PipelineContext


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
    # Строим клиент для Drive Activity API
    activity_service = build("driveactivity", "v2", credentials=creds)


    # Делаем запрос activity:query
    resp = activity_service.activity().query(body={
        "itemName": "items/{}".format(spreadsheet_id), # это ID Google Sheets
        "pageSize": 1,  #  дает возможность получить только одну запись активности
        # "filter": "detail.action_detail_case:CREATE"  # фильтр для получения только создания файла
        # "ancestorName": "items/{}".format(spreadsheet_id)  # можно использовать для получения активности по папке
        # "filter": "detail.action_detail_case:EDIT"  # фильтр для получения только редактирования файла
        # "filter": "detail.action_detail_case:DELETE"  # фильтр для получения только удаления файла
    }).execute()

    print(resp)

    # Инициализация клиента Ozon API
    ozon_client = OzonClient()
    delivery_method_fbo = "FBO"
    delivery_method_fbs = "FBS"

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("ozon")

    async def get_reports(reports: list, gen):
        async for row in gen:
            reports.extend(row)


    # Информация о продавце и методе доставки
    acc_name = " "

    postings = {}

    try:
        for acc in extracted_sellers:
            ozon_client.headers = {"client_id":acc.client_id, "api_key": acc.api_key}

            print(f"Обработка аккаунта: {acc.name} (ID: {acc.client_id})")
            acc_method_fbs = f"{acc_name}_{delivery_method_fbs}"
            acc_method_fbo = f"{acc_name}_{delivery_method_fbo}"
            acc_method_fbs = acc_method_fbs.replace(" ", acc.name)
            acc_method_fbo = acc_method_fbo.replace(" ", acc.name)
            postings[acc_method_fbs] = []
            postings[acc_method_fbo] = []

            # Получаем отчеты
            tasks = [
                # Получаем отчеты FBS
                get_reports(reports=postings[acc_method_fbs],
                            gen=ozon_client.generate_reports(delivery_way=delivery_method_fbs,
                                                             since=proj_settings.DATE_SINCE,
                                                             to=proj_settings.DATE_TO)),
                # Получаем отчеты FBO
                get_reports(reports=postings[acc_method_fbo],
                            gen=ozon_client.generate_reports(delivery_way=delivery_method_fbo,
                                                             since=proj_settings.DATE_SINCE,
                                                             to=proj_settings.DATE_TO))
            ]
            await asyncio.gather(*tasks)

            pipline_context = PipelineContext(
                ozon_client=ozon_client,
                sheets_cli=sheets_cli,
                postings=postings,
                account_name=acc.name,
                account_id=acc.client_id,
                account_api_key=acc.api_key,
                since=proj_settings.DATE_SINCE,
                to=proj_settings.DATE_TO,
                range_last_updating_date=range_updating_date,
            )
            await push_to_sheets(context=pipline_context)

            # Здесь можно обработать postings, например, сохранить в Google Sheets

        for acc, posting in postings.items():
            log.info(f"Отчет для {acc}: {len(posting)} записей")
    except OzonAPIError as e:
        print(f"Ошибка при обращении к Ozon API: {e.status} {e.endpoint} - {e.body}")
    finally:
        await ozon_client.aclose()  # Закрываем соединение с Ozon API

    # df = pd.read_excel("/home/user/UralServiceRegion/top_products_month/FBO 07_29_2025.xlsx")
    # headers = list(df.columns)
    # print(f"Заголовки столбцов: {headers}")


if __name__ == '__main__':
    asyncio.run(main())


