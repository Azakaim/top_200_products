from src.clients.google_sheets.sheets_cli import SheetsCli
from src.clients.ozon.ozon_client import OzonClient
from src.clients.ozon.schemas import SellerAccount

BASE_SHEETS_TITLES: list[str] = ["Модель", "SKU", "Наименование",
                                     "Цена", "Статус", "В заявке",
                                     "Дата от", "Дата до", "Дата обновления"]

async def run_pipeline(*, ozon_cli: OzonClient,
                       sheets_cli: SheetsCli,
                       accounts: list[SellerAccount]):
    # получаем данные из Google Sheets
    existed_sheets = await sheets_cli.get_sheets_info()
    sheets_names = list(existed_sheets.keys())
    extracted_dates = await sheets_cli.read_table(range_table=sheets_names)

