# import json
# from typing import Any, List
#
# from pydantic import BaseModel,  PrivateAttr
#
# from google.oauth2.service_account import Credentials
# from googleapiclient.discovery import build
#
# from src.clients.google_sheets.schemas import SheetsValuesInTo, Body, SheetsValuesOut, BatchUpdateFormat, \
#     Properties, AddSheet
#
#
# class SheetsCli(BaseModel):
#     """
#     Class to handle Google Sheets API calls
#
#     :param spreadsheet_id: str
#     :param scopes: List[str]
#     :param path_to_credentials: str
#     """
#     spreadsheet_id: str
#     scopes: list[str]
#     path_to_credentials: str
#     _service: Any = PrivateAttr(default=None)
#     _creds: Credentials = PrivateAttr(default=None)
#     _sheet_id: int = PrivateAttr(default=None)
#
#     def model_post_init(self, __context):
#         # тут создаём сервис после валидации публичных полей
#         self._creds = Credentials.from_service_account_file(self.path_to_credentials,
#                                                             scopes=self.scopes)
#         self._service = build("sheets", "v4", credentials=self._creds)
#
#     async def add_list(self, title: str) -> None:
#         """
#         Method to add a new sheet to the spreadsheet
#
#         :param title: str - title of the new sheet
#         :return: int - ID of the newly created sheet
#         """
#
#         prop = Properties(title=title)
#         add_sheet = AddSheet(properties=prop)
#         req = BatchUpdateFormat(addSheet=add_sheet)
#         body = Body(requests=[req])
#         await self.__move_batch(body=body.model_dump(by_alias=True, exclude_unset=True))
#
#     async def get_sheet_id_by_title(self, spreadsheet_id: str, title: str) -> int:
#         meta = await self.__move_batch(fields="sheets(properties(sheetId,title))")
#         for sh in meta.get("sheets", []):
#             props = sh["properties"]
#             if props.get("title") == title:
#                 return props["sheetId"]
#         raise ValueError(f"Лист '{title}' не найден")
#
#     async def update_table(self, sheets_values: SheetsValuesInTo):
#         """
#         Method to update the table
#
#         :param sheets_values: SheetsValues
#         :return: None
#         """
#         #Запись (USER_ENTERED — как будто человек вводит ==> т.е формулы будут считаться формулами)
#         self._service.spreadsheets().values().batchUpdate(
#             spreadsheetId=self.spreadsheet_id,
#             # body={
#             #     "valueInputOption": "USER_ENTERED",
#             #     "data": [{"range": "Лист1!A2:C2", "values": [["1", "=A2*10", "ХАХАХААХ"]]}],
#             # },
#             body={
#                 "valueInputOption": "USER_ENTERED",
#                 "data": [sheets_values.model_dump(by_alias=True, exclude_unset=True)],
#             },
#         ).execute()
#
#     async def read_table(self, range_table: List[str]) -> List[SheetsValuesOut]:
#         """
#         Method to read the table
#
#         :param range_table:
#         :return:
#         """
#         resp = await self.__move_batch(range_table=range_table, fields="valueRanges")
#         ranges = resp.get("valueRanges", [])
#         r = [SheetsValuesOut.model_validate(r) for r in ranges]
#         return r
#
#     async def __move_batch(self,*, range_table: List[str]=None, body: dict=None, fields="") -> dict:
#         """ Method to perform batch update
#         :param body: dict - body of the batch update request
#         :return: None
#         """
#         response = {}
#         # Если body не пустой, то выполняем batchUpdate таблицы
#         if body:
#             response = self._service.spreadsheets().batchUpdate(
#                 spreadsheetId=self.spreadsheet_id,
#                 body=body
#             ).execute()
#         # Чтение значений из таблицы
#         if fields:
#             response = self._service.spreadsheets().get(
#                 spreadsheetId=self.spreadsheet_id,
#                 fields=fields
#             ).execute()
#         # Чтение значений из диапазона
#         if range_table:
#             response = self._service.spreadsheets().values().batchGet(
#                 spreadsheetId=self.spreadsheet_id,
#                 ranges=range_table,
#             ).execute()
#         return response
#
#     async def update_format(self, request: Body):
#         format_data = request.model_dump()
#         await self.__move_batch(body=format_data)
import asyncio
import json
from typing import Any, List

from pydantic import BaseModel,  PrivateAttr

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build, Resource

from src.clients.google_sheets.schemas import SheetsValuesInTo, Body, SheetsValuesOut, BatchUpdateFormat, \
    Properties, AddSheet, BatchUpdateValues


class SheetsCli(BaseModel):
    """
    Class to handle Google Sheets API calls

    :param spreadsheet_id: str
    :param scopes: List[str]
    :param path_to_credentials: str
    """
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

    async def update_table(self, sheets_values: BatchUpdateValues):
        """
        Method to update the table

        :param sheets_values: SheetsValues
        :return: None
        """
        data = sheets_values.model_dump(by_alias=True, exclude_none=True)
        await self.__move_batch(body_values=data)

    async def read_table(self, range_table: List[str] | str) -> List[SheetsValuesOut]:
        """
        Method to read the table

        :param range_table:
        :return:
        """
        resp = await self.__move_batch(range_table=range_table)
        ranges = resp.get("valueRanges", [])
        r = [SheetsValuesOut.model_validate(r) for r in ranges]
        return r

    async def update_format(self, request: Body):
        format_data = request.model_dump()
        await self.__move_batch(body_format=format_data)

    async def __move_batch(self,*,
                           range_table: List[str]=None,
                           body_values: dict=None,
                           body_format: dict=None,
                           fields="") -> dict:
        """ Method to perform batch update
        :param body: dict - body of the batch update request
        :return: None
        """
        # Проверяем, что не указаны одновременно fields и range_table и body не пустой
        if (bool(fields) == bool(range_table)) and not body_values and not body_format:
            raise ValueError("Cannot specify both 'fields' and 'range_table' parameters at the same time and "
                             "'body' must not be empty.")
        response = {}
        # Если body не пустой, то выполняем batchUpdate значений таблицы
        if body_values:
            response = self._service.spreadsheets().values().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body_values,
            ).execute()
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
            if isinstance(range_table, str):
                _range = [range_table]
            else:
                _range = range_table
            response = self._service.spreadsheets().values().batchGet(
                spreadsheetId=self.spreadsheet_id,
                ranges=_range,
                majorDimension="COLUMNS"
            ).execute()
        return response
