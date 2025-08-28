from datetime import datetime
from typing import List, Tuple

from pydantic import BaseModel

from dateutil import parser

from src.clients.google_sheets.schemas import SheetsValuesOut
from src.clients.google_sheets.sheets_cli import SheetsCli


class GoogleSheets(BaseModel):
    _cli: SheetsCli

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

    async def check_date_update(self, acc_name: str,
                                *,
                                sheets_cli: SheetsCli,
                                extracted_dates: List[SheetsValuesOut],
                                sheet_id=None) -> \
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
            sheet_id[acc_name] = (await self._cli.check_sheet_exists(title=acc_name))[1]
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
        meta = await self._cli.get_sheets_info()
        # Проверяем, есть ли лист с таким названием
        if title in meta:
            return True, meta[title]
        return False, None

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
