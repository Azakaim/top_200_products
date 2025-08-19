import asyncio

from settings import proj_settings
from src.clients.google_sheets.schemas import SheetsValuesInTo, Request, RepeatCellRequest, GridRange, CellData, \
    TextFormat, CellFormat, FieldPath, BatchUpdateFormatBody, Color
from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_cli import OzonClient
from src.clients.ozon.schemas import extract_sellers, OzonAPIError


async def main() -> None:
    # Инициализация клиента Google Sheets
    # scopes = [proj_settings.SERVICE_SCOPES.strip(',')]
    # path_to_credentials = proj_settings.PATH_TO_CREDENTIALS
    # spreadsheet_id = proj_settings.GOOGLE_SPREADSHEET_ID
    #
    # sheets_cli = SheetsCli(spreadsheet_id=spreadsheet_id,
    #                        scopes=scopes,
    #                        path_to_credentials=path_to_credentials)
    # # Получаем ID листа по названию
    # sheet_id = await sheets_cli.get_sheet_id_by_title(spreadsheet_id=spreadsheet_id, title="Лист1")
    # print(f"ID листа 'Лист1': {sheet_id}")
    # # Читаем таблицу
    # await sheets_cli.read_table(range_table=["Лист1!D3:E5"])
    # val = [["--ИНФО--","--ДАТА--"] for _ in range(5)]
    # sh_value = SheetsValuesInTo(range="Лист1!B4:C8", values=val)
    # # Записываем данные в таблицу
    # await sheets_cli.update_table(sh_value)
    # # Форматируем ячейки
    # req_format = Request()
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
    # update_format_data = BatchUpdateFormatBody(requests=[req_format])
    # # Обновляем формат ячеек
    # await sheets_cli.update_format(update_format_data)
    ozon_client = OzonClient()
    try:
        for acc in extract_sellers():
            print(f"Обработка аккаунта: {acc.name} (ID: {acc.client_id})")
            ozon_client.headers = {"client_id":acc.client_id, "api_key": acc.api_key}
            # Получаем отчет FBS
            async for postings in ozon_client.get_fbs_report(since="2025-07-19T00:00:00Z", to="2025-08-19T23:59:59Z"):
                print(f"Получено {len(postings)} записей FBS для аккаунта {acc.name}")
                # Здесь можно обработать postings, например, сохранить в Google Sheets

            # Получаем отчет FBO
            # async for postings in ozon_client.get_fbo_report(since="2025-10-01T00:00:00Z", to="2025-10-31T23:59:59Z"):
            #     print(f"Получено {len(postings)} записей FBO для аккаунта {acc.name}")
            #     # Здесь можно обработать postings, например, сохранить в Google Sheets
    except OzonAPIError as e:
        print(f"Ошибка при обращении к Ozon API: {e.status} {e.endpoint} - {e.body}")
    finally:
        await ozon_client.aclose()  # Закрываем соединение с Ozon API


if __name__ == '__main__':
    asyncio.run(main())


