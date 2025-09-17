import logging
from datetime import datetime
from typing import List, Tuple

from pydantic import BaseModel

from dateutil import parser

from src.schemas.google_sheets_schemas import SheetsValuesOut, BatchUpdateValues, ResponseSchemaTableData
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.schemas.ozon_schemas import Remainder
from src.mappers.transformation_functions import create_values_range


log = logging.getLogger("google sheet service")

class GoogleSheets(BaseModel):
    cli: SheetsCli

    async def __parse_date(self, dates: list[str]):
        uniq_dates = []
        if dates:
            for s in dates:
                try:
                    parsed_date = parser.parse(s).strftime("%Y-%m-%d")
                    if parsed_date not in uniq_dates:
                        uniq_dates.append(parsed_date)
                except (ValueError, OverflowError, TypeError) as e:
                    continue
        return uniq_dates

    async def push_to_sheets(self, account_name: str,
                             date_since: str,
                             date_to: str,
                             cluster_names: list,
                             sheet_titles,
                             postings: dict,
                             remainders: list[Remainder],
                             range_scope_clear: str) -> None:
        val = await create_values_range(date_since=date_since,
                                        date_to=date_to,
                                        clusters_names=cluster_names,
                                        sheet_titles=sheet_titles,
                                        postings=postings,
                                        remainders=remainders)
        data = SheetsValuesOut(range=account_name, values=val)
        body_value = BatchUpdateValues(value_input_option="USER_ENTERED", data=[data.model_dump()])
        # Записываем данные в таблицу
        await self.cli.update_table(sheets_values=body_value,range_table=range_scope_clear)
        # форматируем таблицу

    async def get_names_sheets(self):
        existed_sheets = await self.get_identity_sheets()
        return list(existed_sheets.keys())

    async def is_today_updating_date(self, updating_dates: List[str]) -> bool:
        dates = list(set(updating_dates)) if updating_dates else None
        # проверяем дату последнего обновления е)сли она сегодня, то пропускаем обновление
        # в случае ошибки парсинга даты возвращаем False и перезаполняем таблицу
        try:
            uniq_dates = await self.__parse_date(dates)
            # проверяем если дат больше одной была ошибка заполнения
            if (len(uniq_dates) > 1) or (len(uniq_dates) == 0):
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

    async def check_data_update(self, acc_name: str,
                                *,
                                extracted_dates: List[SheetsValuesOut],
                                sheet_id=None) -> Tuple:
        """
        Check if the date range for the report is valid.

        :param sheets_cli: SheetsCli
        :param extracted_dates: List[SheetsValuesOut]
        :param acc_name: str
        :param sheet_id
        :return: tuple True if the date range is valid, False otherwise.
        """
        if not sheet_id[acc_name]:
            # Добавляем новый лист в таблицу
            await self.cli.add_list(title=acc_name)
            # Получаем ID нового листа
            sheet_id[acc_name] = (await self.cli.check_sheet_exists(title=acc_name))[1]
            # Берем значения из таблицы в соответствии с именем листа и кабинета
            sheet_values_acc = next((n.values for n in extracted_dates if acc_name in n.range), None)
        else:
            # Берем значения из таблицы в соответствии с именем листа и кабинета
            sheet_values_acc = next((n.values for n in extracted_dates if acc_name in n.range), None)

            # проверяем дату последнего обновления если она сегодня, то пропускаем обновление
            updating_dates = next((e for e in sheet_values_acc if "Дата обновления" in e), None)
            if await self.is_today_updating_date(updating_dates):
                return True, sheet_values_acc
        return False, sheet_values_acc

    async def check_sheet_exists(self, title: str) -> tuple[bool, str | None]:
        """
        Method to check if a sheet with the given title exists in the spreadsheet

        :param title: str - title of the sheet to check
        :return: bool - True if the sheet exists, False otherwise
        """
        meta = await self.cli.get_sheets_info()
        # Проверяем, есть ли лист с таким названием
        if title in meta:
            return True, meta[title]
        return False, None

    async def get_identity_sheets(self):
        return await self.cli.get_sheets_info()

    async def fetch_info(self)-> tuple[list[SheetsValuesOut], dict]:
        sheets_names = await self.get_names_sheets()
        re = await self.cli.read_table(sheets_names)
        try:
            value = ResponseSchemaTableData(**re)
        except (ValueError, OverflowError, TypeError) as e:
            raise e
        values = [SheetsValuesOut.model_validate(r) for r in value.valueRanges]
        return values, re if re else None

    async def format_table(self):
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
