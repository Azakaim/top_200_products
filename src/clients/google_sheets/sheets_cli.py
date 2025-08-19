import json
from typing import Any, List

from pydantic import BaseModel,  PrivateAttr

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from src.clients.google_sheets.schemas import SheetsValuesInTo, BatchUpdateFormatBody, SheetsValuesOut


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

    async def get_sheet_id_by_title(self, spreadsheet_id: str, title: str) -> int:
        meta = self._service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title))"
        ).execute()
        for sh in meta.get("sheets", []):
            props = sh["properties"]
            if props.get("title") == title:
                return props["sheetId"]
        raise ValueError(f"Лист '{title}' не найден")

    async def update_table(self, sheets_values: SheetsValuesInTo):
        """
        Method to update the table

        :param sheets_values: SheetsValues
        :return: None
        """
        #Запись (USER_ENTERED — как будто человек вводит ==> т.е формулы будут считаться формулами)
        self._service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            # body={
            #     "valueInputOption": "USER_ENTERED",
            #     "data": [{"range": "Лист1!A2:C2", "values": [["1", "=A2*10", "ХАХАХААХ"]]}],
            # },
            body={
                "valueInputOption": "USER_ENTERED",
                "data": [sheets_values.model_dump()],
            },
        ).execute()

    async def read_table(self, range_table: List[str]):
        """
        Method to read the table

        :param range_table:
        :return:
        """

        #Чтение значений
        resp = self._service.spreadsheets().values().batchGet(
            spreadsheetId=self.spreadsheet_id,
            ranges=range_table,
        ).execute()
        ranges = resp.get("valueRanges", [])
        r = [SheetsValuesOut.model_validate(r) for r in ranges]
        print(r)



    async def update_format(self, request: BatchUpdateFormatBody):
        format_data = request.model_dump()
        # Форматирование через batchUpdate
        self._service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body = format_data
        ).execute()
