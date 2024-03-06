from pydantic import BaseModel


class Property(BaseModel):
    id: str
    listing_title: str = ""
    description: str = ""
    listed_date: str = ""
    address: str = ""
    price: int = 0
    bedrooms: int = 0
    bathrooms: int = 0
    livingrooms: int = 0
    terms: list[str] = []
