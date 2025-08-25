import asyncio
from copy import deepcopy
from datetime import datetime
from itertools import chain
from typing import List, Tuple

from pydantic import BaseModel

from src.clients.google_sheets.schemas import SheetsValuesInTo, BatchUpdateFormat, GridRange, Color, TextFormat, \
    CellFormat, \
    CellData, FieldPath, RepeatCellRequest, Body, BatchUpdateValues, SheetsValuesOut
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_cli import OzonClient

from dateutil import parser

from src.clients.ozon.schemas import Remainder


class PipelineContext(BaseModel):
    DELIVERY_WAY_FBS: str = "FBS"
    DELIVERY_WAY_FBO: str = "FBO"
    TITLE_SHEETS: List[str] = ["Модель", "SKU", "Наименование",
                                "Цена", "Статус", "В заявке",
                                "Дата от", "Дата до", "Дата обновления"]
    ozon_client: OzonClient
    sheets_cli: SheetsCli
    values_range: List[List[str]]
    account_id: str
    account_name: str
    account_api_key: str
    since: str
    to: str
    range_last_updating_date: str

async def _is_today_updating_date(updating_dates: List[str]) -> bool:
    last_updating_date = None
    uniq_dates = []
    # проверяем дату последнего обновления если она сегодня, то пропускаем обновление
    # в случае ошибки парсинга даты возвращаем False и перезаполняем таблицу
    try:
        if updating_dates:
            for s in updating_dates:
                try:
                    parsed_date = parser.parse(s)
                    if parsed_date not in uniq_dates:
                        uniq_dates.append(parsed_date)
                except (ValueError, OverflowError, TypeError) as e:
                    continue
            # проверяем если дат больше одной была ошибка заполнения
            if len(uniq_dates) > 1:
                return False
            last_updating_date = updating_dates[1]  # берем последнюю дату обновления
        if last_updating_date:
            lud_date_format = parser.parse(last_updating_date).date()
            today_date = datetime.today().date()
            if lud_date_format == today_date:
                return True
    except (ValueError, OverflowError, TypeError) as e:
        print(f"Ошибка при разборе даты: {e}")

    return False

async def check_date_update(acc_name:str,
                            *,
                            sheets_cli: SheetsCli,
                            extracted_dates: List[SheetsValuesOut],
                            sheet_id=None) ->\
    Tuple:
    """
    Check if the date range for the report is valid.

    :param sheets_cli: SheetsCli
    :param extracted_dates: List[SheetsValuesOut].
    :param acc_name: str
    :param sheet_id
    :return: tuple True if the date range is valid, False otherwise.
    """
    if not sheet_id[acc_name]:
        # Добавляем новый лист в таблицу
        await sheets_cli.add_list(title=acc_name)
        # Получаем ID нового листа
        sheet_id[acc_name] = (await sheets_cli.check_sheet_exists(title=acc_name))[1]
        print(f"Добавлен новый лист: {acc_name}")
        # Берем значения из таблицы в соответствии с именем листа и кабинета
        sheet_values_acc = next((n.values for n in extracted_dates if acc_name in n.range), None)
    else:
        # Берем значения из таблицы в соответствии с именем листа и кабинета
        sheet_values_acc = next((n.values for n in extracted_dates if acc_name in n.range), None)

        # проверяем дату последнего обновления если она сегодня, то пропускаем обновление
        updating_dates = next((e for e in sheet_values_acc if "Дата обновления" in e), None)
        if await _is_today_updating_date(updating_dates):
            return True, sheet_values_acc
    print(f"ID листа 'Лист1': {sheet_id}")
    return False, []

async def _collect_values_range_by_model(context: PipelineContext,
                                         model_name: str,
                                         model_posting: dict,
                                         remainders: list[Remainder] = None,
                                         is_title_create: bool = False) -> List[str]:
    values_range_by_model = []
    try:
        for ind, v in enumerate(model_posting):
            if ind == 0 and is_title_create:
                # имена столбцов
                values_range_by_model.append(context.TITLE_SHEETS)
            # работа с остатками
            if remainders:
                remainder_info = [r.cluster_name for r in remainders if str(r.sku) in list(v.keys())]  + [r.available_stock_count for r in remainders if str(r.sku) in list(v.keys())]
                # расплющиваем в одномерный массив наш список
                values = [model_name] +  list(v.keys()) + remainder_info + list(chain.from_iterable(v.values()))
            else:
                # расплющиваем в одномерный массив наш список
                values = [model_name] +  list(v.keys()) + list(chain.from_iterable(v.values()))
            # добавляем остальные данные
            values.extend([context.since, context.to, datetime.now().strftime('%Y-%m-%dT%H:%M')])
            # t = list(chain.from_iterable(val if isinstance(val, list) else [val] for val in values))
            if values:
                values_range_by_model.append(values)
    except (ValueError, OverflowError, TypeError) as e:
        return e
    return values_range_by_model

async def create_values_range(context: PipelineContext, postings: dict) -> List[List[str]]:
    fbs_postings = next((val for key, val in postings.items() if "FBS" in key),None)
    fbo_postings = next((val for key, val in postings.items() if "FBO" in key),None)
    remainders = await get_remainders(context=context, postings=fbo_postings)

    values_range = []
    task = [ _collect_values_range_by_model(context,"FBS", fbs_postings, is_title_create=True),
             _collect_values_range_by_model(context, "FBO", fbo_postings, remainders=remainders),]
    fbs_res, fbo_res = await asyncio.gather(*task, return_exceptions=True)
    values_range.extend(fbs_res)
    values_range.extend(fbo_res)
    return values_range

async def push_to_sheets(context: PipelineContext, postings: dict) -> None:
    val = await create_values_range(context, postings)
    data = SheetsValuesOut(range=context.account_name, values=val)
    body_value = BatchUpdateValues(value_input_option="USER_ENTERED",data=[data.model_dump()])
    # Записываем данные в таблицу
    await context.sheets_cli.update_table(sheets_values=body_value)
    # форматируем таблицу

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

async def get_remainders(context: PipelineContext,postings: list) -> List[Remainder]:
    skus = []
    for s in postings:
        skus.append(next((k for k, v in s.items()), None))
    sorted_skus = list(set(skus))
    remainders = await context.ozon_client.fetch_remainders(sorted_skus)
    if remainders:
        return remainders
    return []

async def format_table():
    """
    Pre-checks the Google Sheets table to ensure it is ready for data insertion.

    :return: None
    """
    # Форматируем ячейки
    # req_format = BatchUpdateFormat()
    # gr_range = GridRange(sheet_id=int(sheet_id),
    #                      start_row_index=3,
    #                      end_row_index=5,
    #                      start_column_index=2,
    #                      end_column_index=4)
    # background_color = Color(green=0.75223445)
    # text_format = TextFormat(bold=True)
    # gr_cell_format = CellFormat(text_format=text_format,
    #                             background_color=background_color)
    # gr_cell = CellData(user_entered_format=gr_cell_format)
    # gr_fields = [FieldPath.BOLD,FieldPath.BACKGROUND_COLOR]
    # req_format.repeat_cell = RepeatCellRequest(range=gr_range,
    #                                            cell=gr_cell,
    #                                            fields=gr_fields)
    # update_format_data = Body(requests=[req_format])
    # # Обновляем формат ячеек
    # await context.sheets_cli.update_format(update_format_data)
    pass