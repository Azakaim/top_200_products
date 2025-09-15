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

    REDIS_HOST: str = Field("", env="REDIS_HOST")
    REDIS_PORT: str = Field("", env="REDIS_PORT")

    GOOGLE_SPREADSHEET_ID: str = Field("", env="GOOGLE_SPREADSHEET_ID")
    GOOGLE_CLIENT_SECRET: str = Field("", env="GOOGLE_CLIENT_SECRET")
    GOOGLE_SHEETS_URI: str = Field("", env="GOOGLE_SHEETS_URI")
    SERVICE_SCOPES: str = Field("", env="SCOPES")
    PATH_TO_CREDENTIALS: str = Field("", env="PATH_TO_CREDENTIALS")
    GOOGLE_BASE_TOP_SHEET_TITLES: str = Field("", env="GOOGLE_SHEET_BASE_TITLES")
    GOOGLE_BASE_SHEETS_TITLES_BY_ACC: str = Field("", env="GOOGLE_BASE_SHEETS_TITLES_BY_ACC")

    OZON_BASE_URL: str = Field("", env="OZON_BASE_URL")
    OZON_NAME_LK: str = Field("", env="OZON_NAME_LK")
    OZON_CLIENT_IDS: str = Field("", env="OZON_CLIENT_IDS")
    OZON_API_KEYS: str = Field("", env="OZON_API_KEYS")
    OZON_REMAINS_URL: str = Field("", env="OZON_REMAINS_URL")
    OZON_PRODUCTS_URL: str = Field("", env="OZON_PRODUCTS_URL")
    OZON_PRODUCTS_INFO_URL: str = Field("", env="OZON_PRODUCTS_INFO")
    OZON_FBS_POSTINGS_REPORT_URL: str = Field("", env="OZON_FBS_POSTINGS_REPORT_URL")
    OZON_FBO_POSTINGS_REPORT_URL: str = Field("", env="OZON_FBO_POSTINGS_REPORT_URL")
    OZON_ANALYTICS_URL: str = Field("", env="OZON_ANALYTICS_URL")
    ANALYTICS_MONTHS: str = Field("", env="ANALYTICS_MONTHS")
    DATE_SINCE: str = Field("", env="DATE_SINCE")
    DATE_TO: str = Field("", env="DATE_TO")

    ONEC_HOST: str = Field("", env="ONEC_HOST")
    ONEC_ENDPOINTS: str = Field("", env="ONEC_ENDPOINTS")
    ONEC_AUTH_LOGIN: str = Field("", env="ONEC_AUTH_LOGIN")
    ONEC_AUTH_PASS: str = Field("", env="ONEC_AUTH_PASS")

    BUCKET_NAME: str = Field("", env="BUCKET_NAME")
    S3_ACCESS_KEY: str = Field("", env="S3_ACCESS_KEY")
    S3_SECRET_KEY: str = Field("", env="S3_SECRET_KEY")
    S3_ENDPOINT: str = Field("", env="S3_ENDPOINT")

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
