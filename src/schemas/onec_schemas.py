from pydantic import BaseModel


class Product1C(BaseModel):
    uid: str
    article: str
    name: str
    stock: int
    summ: float
