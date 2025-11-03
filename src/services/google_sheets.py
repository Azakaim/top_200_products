import logging
from datetime import datetime
from typing import List, Tuple

from pydantic import BaseModel

from dateutil import parser

from src.schemas.google_sheets_schemas import SheetsValuesOut, BatchUpdateValues, ResponseSchemaTableData
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.schemas.ozon_schemas import Remainder
from src.mappers.transformation_functions import create_values_range
from src.schemas.google_sheets_schemas import (
    Body, BatchUpdateFormat, RepeatCellRequest,
    GridRange, CellData, CellFormat, TextFormat,
    Color, FieldPath
)

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

    async def push_top_products_to_sheet(self,
                                         sheet_name: str,
                                         values: list[list]) -> None:
        """
        Записывает топ продукты в указанный лист Google Sheets

        :param sheet_name: str - название листа для записи
        :param values: list[list] - данные для записи (список списков)
        :return: None
        """
        # Проверяем, существует ли лист
        sheet_exists, sheet_id = await self.check_sheet_exists(title=sheet_name)

        # Если листа нет, создаем его
        if not sheet_exists:
            await self.cli.add_list(title=sheet_name)
            log.info(f"Создан новый лист: {sheet_name}")

        # Подготавливаем данные для записи
        data = SheetsValuesOut(range=sheet_name, values=values)
        body_value = BatchUpdateValues(value_input_option="USER_ENTERED", data=[data.model_dump()])

        # Записываем данные в таблицу (с очисткой перед записью)
        await self.cli.update_table(sheets_values=body_value, range_table=sheet_name)
        log.info(f"Записано {len(values)} строк в лист '{sheet_name}'")

    async def format_top_products_table(self,
                                        sheet_name: str,
                                        values: list[list],
                                        cluster_count: int) -> None:
        """
        Форматирует таблицу Top Products:
        - Заголовки (строка 0): жирный текст
        - Строки артикулов: жирный текст + светло-зеленый фон
        - Колонки складов ЧИ6 и МСК (6-7): желтый фон
        - Колонки кластеров: синий фон

        :param sheet_name: название листа
        :param values: данные таблицы
        :param cluster_count: количество кластеров
        :return: None
        """


        # Получаем ID листа
        sheet_exists, sheet_id = await self.check_sheet_exists(title=sheet_name)
        if not sheet_exists:
            log.warning(f"Лист '{sheet_name}' не найден, форматирование пропущено")
            return

        requests = []

        # 1. Форматирование заголовков (строка 0) - жирный текст
        header_range = GridRange(
            sheet_id=int(sheet_id),
            start_row_index=0,
            end_row_index=1,
            start_column_index=0,
            end_column_index=len(values[0]) if values else 20
        )
        header_format = CellFormat(
            text_format=TextFormat(bold=True),
            horizontal_alignment="CENTER"
        )
        header_cell = CellData(user_entered_format=header_format)
        requests.append(BatchUpdateFormat(
            repeat_cell=RepeatCellRequest(
                range=header_range,
                cell=header_cell,
                fields=[FieldPath.BOLD, FieldPath.HORIZONTAL_ALIGNMENT]
            )
        ))

        # 2. Форматирование колонок складов ЧИ6 и МСК (6-7) - желтый фон для заголовка
        warehouse_range = GridRange(
            sheet_id=int(sheet_id),
            start_row_index=0,
            end_row_index=1,
            start_column_index=6,
            end_column_index=8
        )
        warehouse_format = CellFormat(
            text_format=TextFormat(bold=True),
            background_color=Color(red=1.0, green=1.0, blue=0.0, alpha=0.3),  # желтый
            horizontal_alignment="CENTER"
        )
        warehouse_cell = CellData(user_entered_format=warehouse_format)
        requests.append(BatchUpdateFormat(
            repeat_cell=RepeatCellRequest(
                range=warehouse_range,
                cell=warehouse_cell,
                fields=[FieldPath.BOLD, FieldPath.BACKGROUND_COLOR, FieldPath.HORIZONTAL_ALIGNMENT]
            )
        ))

        # 3. Форматирование колонок кластеров (8 до 8+cluster_count) - синий фон для заголовка
        if cluster_count > 0:
            cluster_range = GridRange(
                sheet_id=int(sheet_id),
                start_row_index=0,
                end_row_index=1,
                start_column_index=8,
                end_column_index=8 + cluster_count
            )
            cluster_format = CellFormat(
                text_format=TextFormat(bold=True),
                background_color=Color(red=0.0, green=0.5, blue=1.0, alpha=0.3),  # синий
                horizontal_alignment="CENTER"
            )
            cluster_cell = CellData(user_entered_format=cluster_format)
            requests.append(BatchUpdateFormat(
                repeat_cell=RepeatCellRequest(
                    range=cluster_range,
                    cell=cluster_cell,
                    fields=[FieldPath.BOLD, FieldPath.BACKGROUND_COLOR, FieldPath.HORIZONTAL_ALIGNMENT]
                )
            ))

        # 4. Находим строки артикулов (где первая колонка не пустая и не является заголовком)
        article_row_indices = []
        for i, row in enumerate(values):
            if i == 0:  # Пропускаем заголовок
                continue
            # Строка артикула: № п/п не пустой (колонка 0), SKU пустой (колонка 3)
            if row and str(row[0]).strip() != "" and (len(row) <= 3 or str(row[3]).strip() == ""):
                article_row_indices.append(i)

        # 5. Форматирование строк артикулов - жирный текст + светло-зеленый фон
        for row_idx in article_row_indices:
            article_row_range = GridRange(
                sheet_id=int(sheet_id),
                start_row_index=row_idx,
                end_row_index=row_idx + 1,
                start_column_index=0,
                end_column_index=len(values[0]) if values else 20
            )
            article_row_format = CellFormat(
                text_format=TextFormat(bold=True),
                background_color=Color(red=0.5, green=0.9, blue=0.5, alpha=0.3)  # светло-зеленый
            )
            article_row_cell = CellData(user_entered_format=article_row_format)
            requests.append(BatchUpdateFormat(
                repeat_cell=RepeatCellRequest(
                    range=article_row_range,
                    cell=article_row_cell,
                    fields=[FieldPath.BOLD, FieldPath.BACKGROUND_COLOR]
                )
            ))

        # Отправляем все запросы на форматирование
        if requests:
            update_format_data = Body(requests=requests)
            await self.cli.update_format(update_format_data)
            log.info(f"Применено {len(requests)} правил форматирования к листу '{sheet_name}'")

    async def push_auxiliary_table_to_sheet(self,
                                             sheet_name: str,
                                             values: list[list]) -> None:
        """
        Записывает данные вспомогательной таблицы в указанный лист Google Sheets

        :param sheet_name: str - название листа для записи (название кабинета)
        :param values: list[list] - данные для записи (список списков)
        :return: None
        """
        # Проверяем, существует ли лист
        sheet_exists, sheet_id = await self.check_sheet_exists(title=sheet_name)

        # Если листа нет, создаем его
        if not sheet_exists:
            await self.cli.add_list(title=sheet_name)
            log.info(f"Создан новый лист: {sheet_name}")

        # Подготавливаем данные для записи
        data = SheetsValuesOut(range=sheet_name, values=values)
        body_value = BatchUpdateValues(value_input_option="USER_ENTERED", data=[data.model_dump()])

        # Записываем данные в таблицу (с очисткой перед записью)
        await self.cli.update_table(sheets_values=body_value, range_table=sheet_name)
        log.info(f"Записано {len(values)} строк в лист '{sheet_name}'")

    async def format_auxiliary_table(self,
                                     sheet_name: str,
                                     values: list[list],
                                     cluster_count: int) -> None:
        """
        Форматирует вспомогательную таблицу по кабинету:
        - Заголовки (строка 0): жирный текст, серый фон
        - Колонки базовые (0-5): голубой фон для заголовка
        - Колонки кластеров (6 до 6+cluster_count): зеленый фон для заголовка
        - Колонки дат (последние 3): желтый фон для заголовка

        :param sheet_name: название листа
        :param values: данные таблицы
        :param cluster_count: количество кластеров
        :return: None
        """
        from src.schemas.google_sheets_schemas import (
            Body, BatchUpdateFormat, RepeatCellRequest,
            GridRange, CellData, CellFormat, TextFormat, Color, FieldPath
        )

        # Получаем ID листа
        sheet_exists, sheet_id = await self.check_sheet_exists(title=sheet_name)
        if not sheet_exists:
            log.warning(f"Лист '{sheet_name}' не найден, форматирование пропущено")
            return

        requests = []
        col_count = len(values[0]) if values else 0

        # 1. Форматирование всех заголовков (строка 0) - жирный текст, серый фон
        header_range = GridRange(
            sheet_id=int(sheet_id),
            start_row_index=0,
            end_row_index=1,
            start_column_index=0,
            end_column_index=col_count
        )
        header_format = CellFormat(
            text_format=TextFormat(bold=True),
            background_color=Color(red=0.85, green=0.85, blue=0.85, alpha=1.0),  # серый
            horizontal_alignment="CENTER"
        )
        header_cell = CellData(user_entered_format=header_format)
        requests.append(BatchUpdateFormat(
            repeat_cell=RepeatCellRequest(
                range=header_range,
                cell=header_cell,
                fields=[FieldPath.BOLD, FieldPath.BACKGROUND_COLOR, FieldPath.HORIZONTAL_ALIGNMENT]
            )
        ))

        # 2. Форматирование базовых колонок (0-5: Модель, SKU, Наименование, Цена, Статус, В заявке)
        base_cols_range = GridRange(
            sheet_id=int(sheet_id),
            start_row_index=0,
            end_row_index=1,
            start_column_index=0,
            end_column_index=6
        )
        base_cols_format = CellFormat(
            text_format=TextFormat(bold=True),
            background_color=Color(red=0.6, green=0.8, blue=1.0, alpha=0.5),  # голубой
            horizontal_alignment="CENTER"
        )
        base_cols_cell = CellData(user_entered_format=base_cols_format)
        requests.append(BatchUpdateFormat(
            repeat_cell=RepeatCellRequest(
                range=base_cols_range,
                cell=base_cols_cell,
                fields=[FieldPath.BOLD, FieldPath.BACKGROUND_COLOR, FieldPath.HORIZONTAL_ALIGNMENT]
            )
        ))

        # 3. Форматирование колонок кластеров (6 до 6+cluster_count) - зеленый фон
        if cluster_count > 0:
            cluster_range = GridRange(
                sheet_id=int(sheet_id),
                start_row_index=0,
                end_row_index=1,
                start_column_index=6,
                end_column_index=6 + cluster_count
            )
            cluster_format = CellFormat(
                text_format=TextFormat(bold=True),
                background_color=Color(red=0.5, green=0.9, blue=0.5, alpha=0.5),  # зеленый
                horizontal_alignment="CENTER"
            )
            cluster_cell = CellData(user_entered_format=cluster_format)
            requests.append(BatchUpdateFormat(
                repeat_cell=RepeatCellRequest(
                    range=cluster_range,
                    cell=cluster_cell,
                    fields=[FieldPath.BOLD, FieldPath.BACKGROUND_COLOR, FieldPath.HORIZONTAL_ALIGNMENT]
                )
            ))

        # 4. Форматирование колонок дат (последние 3: Дата от, Дата до, Дата обновления)
        dates_range = GridRange(
            sheet_id=int(sheet_id),
            start_row_index=0,
            end_row_index=1,
            start_column_index=col_count - 3,
            end_column_index=col_count
        )
        dates_format = CellFormat(
            text_format=TextFormat(bold=True),
            background_color=Color(red=1.0, green=1.0, blue=0.6, alpha=0.5),  # желтый
            horizontal_alignment="CENTER"
        )
        dates_cell = CellData(user_entered_format=dates_format)
        requests.append(BatchUpdateFormat(
            repeat_cell=RepeatCellRequest(
                range=dates_range,
                cell=dates_cell,
                fields=[FieldPath.BOLD, FieldPath.BACKGROUND_COLOR, FieldPath.HORIZONTAL_ALIGNMENT]
            )
        ))

        # Отправляем все запросы на форматирование
        if requests:
            update_format_data = Body(requests=requests)
            await self.cli.update_format(update_format_data)
            log.info(f"Применено {len(requests)} правил форматирования к листу '{sheet_name}'")

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
