from typing import Any, List

from pydantic import BaseModel,  PrivateAttr

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from src.schemas.google_sheets_schemas import Body, BatchUpdateFormat, \
    Properties, AddSheet, BatchUpdateValues, ResponseSchemaTableData


class SheetsCli(BaseModel):
    """
    Class to handle Google Sheets API calls

    :param spreadsheet_id: str
    :param scopes: List[str]
    :param path_to_credentials: str
    :param sheets_base_title: list[str]
    """
    sheets_base_title: list[str] = []
    spreadsheet_id: str
    scopes: list[str]

    path_to_credentials: str
    _service: Any = PrivateAttr(default=None)
    _creds: Credentials = PrivateAttr(default=None)
    _sheet_id: int = PrivateAttr(default=None)

    def model_post_init(self, __context):
        # тут создаём сервис после валидации публичных полей
        self._creds = Credentials.from_service_account_file(self.path_to_credentials,
                                                            scopes=self.scopes)
        self._service = build("sheets", "v4", credentials=self._creds)

    async def add_list(self, title: str) -> None:
        """
        Method to add a new sheet to the spreadsheet

        :param title: str - title of the new sheet
        :return: int - ID of the newly created sheet
        """

        prop = Properties(title=title)
        add_sheet = AddSheet(properties=prop)
        req = BatchUpdateFormat(addSheet=add_sheet)
        body = Body(requests=[req])
        data = body.model_dump(exclude_none=True)
        await self.__move_batch(body_format=data)

    async def get_sheets_info(self) -> dict[str, str]:
        """
        Method to get all sheet titles in the spreadsheet

        :return: List[str] - list of sheet titles
        """
        meta = await self.__move_batch(fields="sheets(properties(sheetId,title))") # маска для получения ID и названий листов
        # Получаем список листов из метаданных
        sheets = meta.get("sheets", {})
        if sheets:
            return {sh["properties"]["title"]:sh["properties"]["sheetId"] for sh in sheets}
        return {}

    # service sheets
    async def check_sheet_exists(self, title: str) -> tuple[bool, str | None]:
        """
        Method to check if a sheet with the given title exists in the spreadsheet

        :param title: str - title of the sheet to check
        :return: bool - True if the sheet exists, False otherwise
        """
        meta = await self.get_sheets_info()
        # Проверяем, есть ли лист с таким названием
        if title in meta:
            return True, meta[title]
        return False, None

    async def update_table(self, sheets_values: BatchUpdateValues, range_table: str=""):
        """
        Method to update the table

        :param sheets_values: SheetsValues
        :param range_table: str - name of the sheet to update
        :return: None
        """
        # очистка перед записью TODO: !!! Внимание мы чистим лист пред записью
        await self.__move_batch(range_table=range_table,clear=True)

        data = sheets_values.model_dump(by_alias=True, exclude_none=True)
        await self.__move_batch(body_values=data)

    async def read_value_ranges(self,range_table: List[str] | str): # TODO удалить после того как main перепишу
        """
        Method to read the table

        :param range_table:
        :return:
        """
        resp = await self.read_table(range_table=range_table)
        value = ResponseSchemaTableData(**resp)
        return value

    async def read_table(self, range_table: List[str] | str) -> dict:
        """
        Method to read the table

        :param range_table:
        :return:
        """
        resp = await self.__move_batch(range_table=range_table)
        return resp

    async def update_format(self, request: Body):
        format_data = request.model_dump()
        await self.__move_batch(body_format=format_data)

    async def __move_batch(self,*,
                           range_table: List[str] | str= None,
                           body_values: dict=None,
                           body_format: dict=None,
                           fields="",
                           clear=False) -> dict:
        """ Method to perform batch update
        :return: None
        """
        # Проверяем, что не указаны одновременно fields и range_table и body не пустой
        if (bool(fields) == bool(range_table)) and not body_values and not body_format and clear is False:
            raise ValueError("Cannot specify both 'fields' and 'range_table' parameters at the same time and "
                             "'body' must not be empty.")
        response = {}
        # Если body не пустой, то выполняем batchUpdate значений таблицы
        if body_values:
            response = self._service.spreadsheets().values().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body_values,
            ).execute()
        # Форматирование таблицы
        if body_format:
            response = self._service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body_format
            ).execute()
        # Чтение значений из таблицы
        if fields:
            response = self._service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id,
                fields=fields
            ).execute()
        # Чтение значений из диапазона
        if range_table:
                _range = range_table
                 # очистить страницу
                if clear:
                    requests = [
                        {"deleteSheet": {"sheetId": "2128434597"}},
                        {"addSheet": {"properties": {"title": _range}}}
                    ]

                    return self._service.spreadsheets().batchUpdate(
                        spreadsheetId=self.spreadsheet_id,
                        body={'requests': requests}
                    ).execute()
                    return self._service.spreadsheets().values().clear(
                        spreadsheetId=self.spreadsheet_id,
                        range=_range
                    ).execute()
                response = self._service.spreadsheets().values().batchGet(
                    spreadsheetId=self.spreadsheet_id,
                    ranges=_range,
                    majorDimension="COLUMNS"
                ).execute()
        return response
