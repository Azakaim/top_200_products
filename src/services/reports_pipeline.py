import asyncio
from typing import List

from pydantic import BaseModel

from src.clients.google_sheets.schemas import SheetsValuesInTo, BatchUpdateFormat, GridRange, Color, TextFormat, \
    CellFormat, \
    CellData, FieldPath, RepeatCellRequest, Body, BatchUpdateValues, SheetsValuesOut
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_cli import OzonClient


class PipelineContext(BaseModel):
    DELIVERY_WAY_FBS: str = "FBS"
    DELIVERY_WAY_FBO: str = "FBO"
    ozon_client: OzonClient
    sheets_cli: SheetsCli
    postings: dict = {}
    account_id: int
    account_name: str
    account_api_key: str
    since: str
    to: str
    range_last_updating_date: str

async def check_date_update(since: str, to: str) -> bool:
    """
    Check if the date range for the report is valid.

    :param since: Start date in ISO 8601 format.
    :param to: End date in ISO 8601 format.
    :return: True if the date range is valid, False otherwise.
    """
    ...

async def push_to_sheets(context: PipelineContext ) -> None:
    range_table_read_date = []
    list_name = list(context.postings.keys())[0].split('_')[0] # Получаем нейм кабинета

    # Проверяем, существует ли лист с таким названием
    flag, sheet_id = await context.sheets_cli.check_sheet_exists(title=list_name)

    if not flag:
        # Добавляем новый лист в таблицу
        await context.sheets_cli.add_list(title=list_name)
        print(f"Добавлен новый лист: {list_name}")

    print(f"ID листа 'Лист1': {sheet_id}")
    # Читаем последнюю дату обновления
    last_updating_date = await context.sheets_cli.read_table(range_table=context.range_last_updating_date)
    val = [["--ИНФО--","--ДАТА--"] for _ in range(5)]
    sh_value = SheetsValuesOut(range=f"{list_name}!A1:B5", values=val)
    body_value = BatchUpdateValues(value_input_option="USER_ENTERED",data=[sh_value.model_dump()])
    # Записываем данные в таблицу
    await context.sheets_cli.update_table(sheets_values=body_value)
    # Форматируем ячейки
    req_format = BatchUpdateFormat()
    gr_range = GridRange(sheet_id=int(sheet_id),
                         start_row_index=3,
                         end_row_index=5,
                         start_column_index=2,
                         end_column_index=4)
    background_color = Color(green=0.75223445)
    text_format = TextFormat(bold=True)
    gr_cell_format = CellFormat(text_format=text_format,
                                background_color=background_color)
    gr_cell = CellData(user_entered_format=gr_cell_format)
    gr_fields = [FieldPath.BOLD,FieldPath.BACKGROUND_COLOR]
    req_format.repeat_cell = RepeatCellRequest(range=gr_range,
                                               cell=gr_cell,
                                               fields=gr_fields)
    update_format_data = Body(requests=[req_format])
    # Обновляем формат ячеек
    await context.sheets_cli.update_format(update_format_data)
    ...

async def fetch_postings(context: PipelineContext):
    """
    Fetch postings from Ozon API based on delivery way and date range.

    :param context: PipelineContext containing necessary parameters.
    :return: List of postings.
    """
    context.ozon_client.headers = {"client_id": context.account_id, "api_key": context.account_api_key}
    postings = {}
    print(f"Обработка аккаунта: {context.account_name} (ID: {context.account_id})")
    acc_method_fbs = f"{context.account_name}_{context.DELIVERY_WAY_FBS}"
    acc_method_fbo = f"{context.account_name}_{context.DELIVERY_WAY_FBO}"
    postings[acc_method_fbs] = []
    postings[acc_method_fbo] = []

    # Получаем отчеты
    tasks = [
        # Получаем отчеты FBS
        get_reports(reports=postings[acc_method_fbs],
                    gen=context.ozon_client.generate_reports(delivery_way=context.DELIVERY_WAY_FBS,
                                                     since=context.since,
                                                     to=context.to)),
        # Получаем отчеты FBO
        get_reports(reports=postings[acc_method_fbo],
                    gen=context.ozon_client.generate_reports(delivery_way=context.DELIVERY_WAY_FBO,
                                                     since=context.since,
                                                     to=context.to))
    ]
    await asyncio.gather(*tasks)
    return postings

async def get_reports(reports: list, gen):
    async for row in gen:
        reports.extend(row)

async def precheck_table():
    """
    Pre-checks the Google Sheets table to ensure it is ready for data insertion.

    :return: None
    """
    # Implement pre-check logic here if needed
    pass