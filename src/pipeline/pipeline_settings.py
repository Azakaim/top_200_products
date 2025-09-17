from typing import Optional

from pydantic import BaseModel

from src.clients.ozon.ozon_bound_client import OzonCliBound

class PipelineSettings(BaseModel):
    """
    PipelineSettings model
    :param since str
    :param to str
    """
    model_config = {
        "arbitrary_types_allowed": True ,# что бы пайдентик понимал что за модель OzonCliBound у него
                                        # иначе ругается тк обычный класс, без pydantic‑схемы
                                        # и он не генерил схему валидации
    }

    sheet_titles: Optional[list[str]] = None
    clusters_names: Optional[list[str]] = None
    values_range: Optional[list[list[str]]] = None
    account_id: str
    account_name: str
    account_api_key: str
    since: str
    to: str
    clear_scope_range: Optional[str] = ""

class PipelineCxt(BaseModel):
    cxt_config: PipelineSettings
    ozon: OzonCliBound

    model_config = {
        "arbitrary_types_allowed": True # что бы пайдентик понимал что за модель OzonCliBound у него
                                        # иначе ругается тк обычный класс, без pydantic‑схемы
                                        # и он не генерил схему валидации
    }
