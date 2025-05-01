from pydantic import BaseModel, Field
from enum import StrEnum


class GenderCategory(StrEnum):
    MEN = "Men"
    WOMEN = "Women"
    KIDS = "Kids"
    BOYS = "Boys"
    GIRLS = "Girls"
    TODDLER = "Toddler"
    INFANT = "Infant"


class GenderTag(BaseModel):
    gender: list[GenderCategory] = Field(title="Gender", description="Gender Category of the fashion item")
