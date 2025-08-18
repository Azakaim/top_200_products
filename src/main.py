import asyncio

from settings import proj_settings
from src.clients.google_sheets.schemas import SheetsValuesInTo, Request, RepeatCellRequest, GridRange, CellData, \
    TextFormat, CellFormat
from src.clients.google_sheets.sheets_cli import SheetsCli

async def main() -> None:
    scopes = [proj_settings.SERVICE_SCOPES.strip(',')]
    path_to_credentials = proj_settings.PATH_TO_CREDENTIALS
    spreadsheet_id = proj_settings.GOOGLE_SPREADSHEET_ID

    sheets_cli = SheetsCli(spreadsheet_id=spreadsheet_id,
                           scopes=scopes,
                           path_to_credentials=path_to_credentials)
    await sheets_cli.read_table(range_table=["Лист1!D3:E5"])
    val = [["хихот","вихот"] for _ in range(5)]
    print(val)
    print(len(val))
    sh_value = SheetsValuesInTo(range="Лист1!B4:C8", values=val)
    await sheets_cli.update_table(sh_value)
    # body = {
    #     "requests": [
    #         {"repeatCell": {
    #             "range": {"sheetId": 990682029, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0,
    #                       "endColumnIndex": 3},
    #             "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
    #             "fields": "userEnteredFormat.textFormat.bold"
    #         }}
    #     ]
    # }
    req_format = Request()
    gr_range = GridRange(sheet_id=990682029,
                         start_row_index=1,
                         end_row_index=2,
                         start_column_index=0,
                         end_column_index=3)
    text_format = TextFormat(bold=True)
    gr_cell_format = CellFormat(text_format=text_format)
    gr_cell = CellData(user_entered_format=gr_cell_format)
    gr_fields = ["userEnteredFormat.textFormat.bold","userEnteredFormat.backgroundColor"]
                                               cell=gr_cell,
                                               fields=gr_fields)
    print(req_format.model_dump())

if __name__ == '__main__':
    asyncio.run(main())


