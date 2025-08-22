from enum import StrEnum
from typing import List, Optional, Literal, Final, Tuple, Any, Union
from pydantic import BaseModel, Field, AliasChoices, field_validator, PrivateAttr


class SheetsValues(BaseModel):
    range: str = Field(..., description="The A1 notation of the values to update.")
    values: List[List[str]] = Field(default_factory=list)

class SheetsValuesInTo(SheetsValues):
    ...

class SheetsValuesOut(SheetsValues):
    major_dimension: Literal["ROWS", "COLUMNS"] = Field(default="ROWS",
                                                        alias="majorDimension",
                                                        validation_alias=AliasChoices("majorDimension",
                                                                                      "major_dimension"))

    model_config = {
        "populate_by_name": True
    }
# ===== БАЗОВЫЕ ТИПЫ =====

class Color(BaseModel):
    # Google ждёт 0..1
    red: Optional[float] = None
    green: Optional[float] = None
    blue: Optional[float] = None
    alpha: Optional[float] = None

    model_config = {
        "populate_by_name": True
    }

class TextFormat(BaseModel):
    bold: Optional[bool] = None
    italic: Optional[bool] = None
    underline: Optional[bool] = None
    strikethrough: Optional[bool] = None
    font_family: Optional[str] = Field(default="Arial",
                                       alias="fontFamily",
                                       validation_alias=AliasChoices("font_family",
                                                                     "fontFamily"))
    font_size: Optional[int] = Field(default=12,
                                     alias="fontSize",
                                     validation_alias=AliasChoices("font_size",
                                                                   "fontSize"))
    foreground_color: Optional[Color] = Field(default=None,
                                              alias="foregroundColor",
                                              validation_alias=AliasChoices("foregroundColor",
                                                                            "foregroundColor"))

    model_config = {
        "populate_by_name": True
    }

class HorizontalAlign(str):
    pass
class VerticalAlign(str):
    pass

HorizontalAlignment = Literal["LEFT", "CENTER", "RIGHT"]
VerticalAlignment = Literal["TOP", "MIDDLE", "BOTTOM"]

class CellFormat(BaseModel):
    # см. https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#CellFormat
    text_format: Optional[TextFormat] = Field(default=None,
                                              alias="textFormat",
                                              validation_alias=AliasChoices("text_format",
                                                                            "textFormat"))
    background_color: Optional[Color] = Field(default=None,
                                              alias="backgroundColor",
                                              validation_alias=AliasChoices("backgroundColor",
                                                                            "backgroundColor"))
    horizontal_alignment: Optional[HorizontalAlignment] = Field(default=None,
                                                                alias="horizontalAlignment",
                                                                validation_alias=AliasChoices("horizontalAlignment",
                                                                                                "horizontalAlignment"))
    vertical_alignment: Optional[VerticalAlignment] = Field(default=None,
                                                            alias="verticalAlignment",
                                                            validation_alias=AliasChoices("verticalAlignment",
                                                                                          "verticalAlignment"))
    wrap_strategy: Optional[Literal["WRAP",
                                    "OVERFLOW_CELL",
                                    "CLIP"]] = Field(default="WRAP",
                                                     alias="wrapStrategy",
                                                     validation_alias=AliasChoices("wrapStrategy",
                                                                                    "wrapStrategy"))
    number_format: Optional[dict] = Field(default=None,
                                          alias="numberFormat",# {"type": "NUMBER", "pattern": "0.00"}
                                          validation_alias=AliasChoices("numberFormat",
                                                                        "numberFormat"))

    model_config = {
        "populate_by_name": True
    }

class GridRange(BaseModel):
    # Индексы нулевые, end* не включительно
    sheet_id: int = Field(alias="sheetId",
                          validation_alias=AliasChoices("sheet_id", "sheetId"))
    start_row_index: Optional[int] = Field(default=None,
                                           alias="startRowIndex",
                                           validation_alias=AliasChoices("start_row_index",
                                                                         "startRowIndex"))
    end_row_index: Optional[int] = Field(default=None,
                                         alias="endRowIndex",
                                         validation_alias=AliasChoices("end_row_index",
                                                                       "endRowIndex"))
    start_column_index: Optional[int] = Field(default=None,
                                              alias="startColumnIndex",
                                              validation_alias=AliasChoices("start_column_index",
                                                                            "startColumnIndex"))
    end_column_index: Optional[int] = Field(default=None,
                                            alias="endColumnIndex",
                                            validation_alias=AliasChoices("end_column_index",
                                                                            "endColumnIndex"))

    model_config = {
        "populate_by_name": True
    }
# ===== REPEAT CELL =====

class CellData(BaseModel):
    user_entered_value: Optional[dict] = Field(default=None,
                                               alias="userEnteredValue",
                                               validation_alias=AliasChoices("user_entered_value",
                                                                             "userEnteredValue"))  # как правило для форматирования не нужен
    user_entered_format: Optional[CellFormat] = Field(default=None,
                                                      alias="userEnteredFormat",
                                                      validation_alias=AliasChoices("user_entered_format",
                                                                                    "userEnteredFormat"))  # форматирование ячейки

    model_config = {
        "populate_by_name": True
    }

class FieldPath(StrEnum):
    """
    Тип для полей, которые нужно обновить в RepeatCellRequest.
    Используется для указания путей к полям в формате Google Sheets API.
    """
    BOLD = "userEnteredFormat.textFormat.bold"
    ITALIC = "userEnteredFormat.textFormat.italic"
    UNDERLINE = "userEnteredFormat.textFormat.underline"
    STRIKETHROUGH = "userEnteredFormat.textFormat.strikethrough"
    FONT_FAMILY = "userEnteredFormat.textFormat.fontFamily"
    FONT_SIZE = "userEnteredFormat.textFormat.fontSize"
    FOREGROUND_COLOR = "userEnteredFormat.textFormat.foregroundColor"
    BACKGROUND_COLOR = "userEnteredFormat.backgroundColor"
    HORIZONTAL_ALIGNMENT = "userEnteredFormat.horizontalAlignment"
    VERTICAL_ALIGNMENT = "userEnteredFormat.verticalAlignment"
    WRAP_STRATEGY = "userEnteredFormat.wrapStrategy"

# TODO: сделать так, чтобы можно было передавать список полей в RepeatCellRequest
# FIELD_PATHS: Final[Tuple[str, ...]] = (
#     "userEnteredFormat.textFormat.bold",
#     "userEnteredFormat.textFormat.italic",
#     "userEnteredFormat.textFormat.underline",
#     "userEnteredFormat.textFormat.strikethrough",
#     "userEnteredFormat.textFormat.fontFamily",
#     "userEnteredFormat.textFormat.fontSize",
#     "userEnteredFormat.textFormat.foregroundColor",
#     "userEnteredFormat.backgroundColor",
#     "userEnteredFormat.horizontalAlignment",
#     "userEnteredFormat.verticalAlignment",
#     "userEnteredFormat.wrapStrategy",
# )
#
# F= [*FIELD_PATHS]

class RepeatCellRequest(BaseModel):
    range: GridRange
    cell: CellData
    fields: Any

    def model_post_init(self, __context):
        if not self.fields:
            raise ValueError("Fields must not be empty. Use FIELD_PATHS to specify the fields to update.")
        # Преобразуем строки в FieldPath
        if isinstance(self.fields, list):
            self.fields = ",".join(self.fields)

# ===== MERGE CELLS =====

class MergeCellsRequest(BaseModel):
    range: GridRange
    merge_type: Literal["MERGE_ALL",
                        "MERGE_COLUMNS",
                        "MERGE_ROWS"] = Field(alias="mergeType",
                                              validation_alias=AliasChoices("merge_type",
                                                                            "mergeType"))

# ===== BORDERS =====

class Border(BaseModel):
    style: Optional[Literal["DOTTED", "DASHED", "SOLID", "SOLID_MEDIUM", "SOLID_THICK", "DOUBLE"]] = None
    width: Optional[int] = None
    color: Optional[Color] = None

class UpdateBordersRequest(BaseModel):
    range: GridRange
    top: Optional[Border] = None
    bottom: Optional[Border] = None
    left: Optional[Border] = None
    right: Optional[Border] = None
    inner_horizontal: Optional[Border] = Field(default=None,
                                               alias="innerHorizontal",
                                               validation_alias=AliasChoices("inner_horizontal",
                                                                             "innerHorizontal"))
    inner_vertical: Optional[Border] = Field(default=None,
                                             alias="innerVertical",
                                             validation_alias=AliasChoices("inner_vertical",
                                                                           "innerVertical"))

    model_config = {
        "populate_by_name": True
    }

# ===== AUTO-RESIZE =====

class DimensionRange(BaseModel):
    sheet_id: int = Field(alias="sheetId",
                          validation_alias=AliasChoices("sheet_id", "sheetId"))
    dimension: Literal["ROWS", "COLUMNS"]
    start_index: int = Field(alias="startIndex",
                             validation_alias=AliasChoices("start_index", "startIndex"))
    end_index: int = Field(alias="endIndex",
                           validation_alias=AliasChoices("end_index", "endIndex"))
    model_config = {
        "populate_by_name": True
    }

class AutoResizeDimensionsRequest(BaseModel):
    dimensions: DimensionRange

# ===== ОБЪЕДИНИТЕЛЬНЫЙ ТИП REQUEST =====
class Properties(BaseModel):
    title: Optional[str] = Field(default=None)

class AddSheet(BaseModel):
    properties: Optional[Properties] = Field(default=None)

class RequestToTable:
    ...

class BatchUpdateValues(BaseModel):
    value_input_option: Literal["RAW", "USER_ENTERED"] = Field(default="USER_ENTERED", alias="valueInputOption",
                                                               validation_alias=AliasChoices("value_input_option",
                                                                                             "valueInputOption"))
    data: List[SheetsValuesOut] = Field(default_factory=list)

class BatchUpdateFormat(BaseModel, RequestToTable):
    repeat_cell: Optional[RepeatCellRequest] = Field(default=None,
                                                     alias="repeatCell",
                                                     validation_alias=AliasChoices("repeat_cell",
                                                                                   "repeatCell"))
    merge_cells: Optional[MergeCellsRequest] = Field(default=None,
                                                     alias="mergeCells",
                                                     validation_alias=AliasChoices("merge_cells",
                                                                                   "mergeCells"))
    update_borders: Optional[UpdateBordersRequest] = Field(default=None,
                                                           alias="updateBorders")
    auto_resize_dimensions: Optional[AutoResizeDimensionsRequest] = Field(default=None,
                                                                          alias="autoResizeDimensions",
                                                                          validation_alias=AliasChoices("auto_resize_dimensions",
                                                                                                        "autoResizeDimensions"))
    add_sheet: Optional[AddSheet] = Field(default=None,
                                          alias="addSheet",
                                          validation_alias=AliasChoices("add_sheet",
                                                                        "addSheet"))

    model_config = {
        "populate_by_name": True,
        "extra": "forbid"  # чтоб не проскочило лишнего forbid --> строго следить за полями
    }

class Body(BaseModel):
    requests: List[Union[BatchUpdateFormat, BatchUpdateValues]] = Field(default_factory=list)
