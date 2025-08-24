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
    values_range: List[List[str]]
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
    sheet_name = list(context.postings.keys())[0].split('_')[0] # Получаем нейм кабинета


    # Читаем последнюю дату обновления
    sheet_values = await get_sheet_values(context, sheet_name)
    val = [["--ИНФО--","--ДАТА--"] for _ in range(5)]
    d_fill_out = SheetsValuesOut(range=f"{sheet_name}!A1:B5", values=val)
    body_value = BatchUpdateValues(value_input_option="USER_ENTERED",data=[d_fill_out.model_dump()])
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

async def get_sheet_values(context: PipelineContext, sheet_name: str) -> List[List[str]]:
    """
    Fetch the last updating date from the specified Google Sheets range.
    :param context: PipelineContext containing necessary parameters.
    :param sheet_name: Name of the sheet to read from.
    :return: List[str].
    """
    range_sheet = sheet_name
    last_updating_date = await context.sheets_cli.read_table(range_table=range_sheet)
    return [v for v in last_updating_date[0].values] # Возвращаем только значения

async def get_updating_date(context: PipelineContext) -> str:
    ...

async def populate_table():
    """
    Pre-checks the Google Sheets table to ensure it is ready for data insertion.

    :return: None
    """
    # Implement pre-check logic here if needed
    pass