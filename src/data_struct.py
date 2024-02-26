from pydantic import BaseModel


class Property(BaseModel):
    id: str
    listing_title: str
    description: str
    listed_date: str
    address: str
    price: int
    bedrooms: int
    bathrooms: int
    livingrooms: int
    terms: list[str] | None = None
