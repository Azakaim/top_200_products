from dotenv import load_dotenv
from environs import Env
from pydantic_settings import BaseSettings
from pydantic import Field, PostgresDsn

class Settings(BaseSettings):

    MODE: str = Field("DEV", env="MODE")

    POSTGRES_USER: str = Field("postgres", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field("postgres", env="POSTGRES_PASSWORD")
    POSTGRES_DB: str = Field("postgres", env="POSTGRES_DB")
    POSTGRES_HOST: str = Field("localhost", env="POSTGRES_HOST")
    POSTGRES_PORT: int | str = Field("5432", env="POSTGRES_PORT")

    REDIS_HOST: str = Field("redis://redis:6379", env="REDIS_HOST")

    GOOGLE_SPREADSHEET_ID: str = Field("", env="GOOGLE_SPREADSHEET_ID")
    GOOGLE_CLIENT_SECRET: str = Field("", env="GOOGLE_CLIENT_SECRET")
    GOOGLE_SHEETS_URI: str = Field("", env="GOOGLE_SHEETS_URI")
    GOOGLE_SHEETS_DATE_UPDATING_RANGE: str = Field("", env="GOOGLE_SHEETS_DATE_UPDATE_RANGE")
    SERVICE_SCOPES: str = Field("", env="SCOPES")
    PATH_TO_CREDENTIALS: str = Field("", env="PATH_TO_CREDENTIALS")

    OZON_BASE_URL: str = Field("", env="OZON_BASE_URL")
    OZON_NAME_LK: str = Field("", env="OZON_NAME_LK")
    OZON_CLIENT_IDS: str = Field("", env="OZON_CLIENT_IDS")
    OZON_API_KEYS: str = Field("", env="OZON_API_KEYS")
    OZON_REMAIN_URL: str = Field("", env="OZON_REMAIN_URL")
    FBS_POSTINGS_REPORT_URL: str = Field("", env="FBS_POSTINGS_REPORT_URL")
    FBO_POSTINGS_REPORT_URL: str = Field("", env="FBO_POSTINGS_REPORT_URL")
    DATE_SINCE: str = Field("", env="DATE_SINCE")
    DATE_TO: str = Field("", env="DATE_TO")

    class Config:
        env = Env()
        load_dotenv()
        env.read_env()

    def get_postgres_uri(self) -> PostgresDsn:
        uri = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}".format(
            user=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            host=self.POSTGRES_HOST,
            port=self.POSTGRES_PORT,
            name=self.POSTGRES_DB,
        )
        return uri

proj_settings = Settings()
