import logging
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from io import BytesIO

from botocore.client import BaseClient
from pandas import DataFrame
from pyasn1_modules.rfc5990 import aes256_Wrap
from pydantic import BaseModel


log = logging.getLogger("backup service")

class BackupService(BaseModel):
    bucket_name: str
    cli: BaseClient

    model_config = {
        "arbitrary_types_allowed": True
    }

    async def save_parquet(self, data):
        df = pd.DataFrame(data)
        buffer = BytesIO()
        pq.write_table(pa.Table.from_pandas(df=df), buffer)
        buffer.seek(0)  # возвращаем курсор в начало буффера
        try:
            re = self.cli.put_object(Bucket=self.bucket_name, Key=f"{date.today()}.parquet", Body=buffer.getvalue())
            if re:
                return re['ETag']
        except Exception as e:
           if await self.__save_parquet_local(df) != 0:
               raise e
        return None

    async def __save_parquet_local(self, df: DataFrame):
        try:
            df.to_parquet(f"./local_storage/{date.today()}.parquet",
                          "pyarrow",
                          index=False)
            return 0
        except Exception as e:
            raise e

    async def load_parquet(self, req_date: date):
        buffer = BytesIO()
        try:
            self.cli.download_fileobj(self.bucket_name, f"{req_date}.parquet", buffer)
            buffer.seek(0)
            return pd.read_parquet(buffer).to_dict(orient="records")
        except Exception as e:
            return await self.__load_parquet_local(req_date)

    async def __load_parquet_local(self, req_date: date):
        try:
            df = pd.read_parquet(f"./local_storage/{req_date}.parquet","pyarrow").to_dict(orient="records")
        except Exception as e:
            raise e
        return df