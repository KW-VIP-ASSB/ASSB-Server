from enum import StrEnum
from pydantic import BaseModel, Field


class Category(StrEnum):
    # Clothing Categories
    UNIFORM = "유니폼/단체복"
    VEST = "조끼"
    TSHIRT = "티셔츠"
    PANTS = "바지"
    SKIRT = "스커트"
    KNIT_SWEATER = "니트/스웨터"
    SUIT_SETUP = "정장/수트/셋업"
    JUMPSUIT = "점프슈트"
    JEANS = "청바지"
    LEGGINGS = "레깅스"
    JACKET = "재킷"
    BLOUSE_SHIRT = "블라우스/셔츠"
    COAT = "코트"
    CARDIGAN = "카디건"
    JUMPER = "점퍼"
    SHIRT = "셔츠/남방"
    TRAINING_WEAR = "트레이닝복"
    COORD_SET = "코디세트"
    PARTY_DRESS = "파티복"
    RAINCOAT = "레인코트"
    DRESS = "원피스"
    SHOES = "신발"
    BAG = "가방"
    WALLET = "지갑"
    BELT = "벨트"
    HAT = "모자"
    GLOVES = "장갑"
    UNDERWEAR_SLEEPWEAR = "언더웨어/잠옷"
    SPORTSWEAR = "스포츠웨어"
    SWIMWEAR = "수영복/스윔웨어"
    JEWELRY = "주얼리"
    SOCKS = "양말"
    GLASSES = "선글라스/안경"
    WATCH = "시계"
    HAIR_ACCESSORIES = "헤어악세서리"
    MUFFLER_WARMER = "머플러/워머"


class CategoryTag(BaseModel):
    category: list[Category] = Field(title="Category", description="Category of the fashion item")
