from pydantic import BaseModel, model_validator


class Property(BaseModel):
    id: str
    listing_title: str | None = None
    description: str | None = None
    listed_date: str | None = None
    address: str | None = None
    price: int = 0
    bedrooms: int = 0
    bathrooms: int = 0
    livingrooms: int = 0
    terms: str | None = None
    finger_print: str | None = None

    @model_validator(mode="after")
    def set_finger_print(self):
        self.finger_print = self.id + str(self.price)
        return self
