import json
from core.infra.utils.format_html import HTMLFormatter
from typing import List
from pydantic import BaseModel
from datetime import datetime


class MusinsaSchema(BaseModel):
    """무신사 상품 기본 정보를 위한 모델"""

    style_id: str
    site_id: str = "iylQhcSbkgVxi0Ye"
    brand: str
    title: str
    gender: List[str]
    category: str
    sub_categories: List[str]
    price: int
    original_price: int
    material: str  # JSON string 형태로 저장
    image_urls: List[str]
    url: str
    description: str
    date: datetime
    rank: int | None
    rank_category: str | None
    rank_metadata: list[dict] = []

    @classmethod
    def build(cls, style: dict, date: datetime) -> "MusinsaSchema":
        print(style)
        data = style["araas_style_info"]

        style_id = style["goodsNo"]
        title = style["goodsName"]
        url = f"https://www.musinsa.com/products/{style_id}"
        gender = data.get("sex", [])
        category_info = data.get("category", {})
        main_category = category_info.get("categoryDepth1Name", "")
        sub_categories = []
        for depth in range(2, 5):  # 2, 3, 4 depth 처리
            category_name = category_info.get(f"categoryDepth{depth}Name")
            if category_name:
                sub_categories.append(category_name)
        material = json.dumps(data.get("goodsMaterial", {}), ensure_ascii=False)

        images = [
            f"https://image.musinsa.com{data['thumbnailImageUrl']}",
        ]

        for image in data.get("goodsImages", []):
            if image.get("imageUrl") is None:
                continue
            images.append(f"https://image.musinsa.com{image['imageUrl']}")
        assert len(images) > 0, style

        raw_description = data.get("goodsContents", "")
        description = HTMLFormatter.format_description(raw_description)
        add_description = data.get("mdOpinion", "")

        return cls(
            style_id=str(style_id),
            brand=data["brand"],
            title=title,
            gender=gender,
            category=main_category,
            sub_categories=sub_categories,
            price=data.get("goodsPrice", {}).get("normalPrice", 0),
            original_price=data.get("goodsPrice", {}).get("originPrice", 0),
            material=material,
            image_urls=images,
            url=url,
            description=description + add_description,
            rank=style.get("araas_rank", None),
            rank_category=style.get("araas_category", None),
            rank_metadata=style.get("araas_rank_inputs", {}).get("data", []),
            date=date.replace(hour=0, minute=0, second=0, microsecond=0),
        )

    @property
    def style(self) -> dict | None:
        if len(self.image_urls) == 0:
            return None
        return dict(
            site_id=self.site_id,
            style_id=self.style_id,
            code=self.style_id,
            name=self.title,
            url=self.url,
        )

    @property
    def images(self) -> list[dict] | None:
        if len(self.image_urls) == 0:
            return None
        images = []

        for position, image_url in enumerate(self.image_urls):
            images.append(
                dict(
                    site_id=self.site_id,
                    style_idx=self.style_id,
                    variant_id=None,
                    position=position,
                    origin=image_url,
                    image=None,
                ),
            )
        return images

    @property
    def style_price(self) -> dict | None:
        if len(self.image_urls) == 0:
            return None
        return dict(
            site_id=self.site_id,
            style_idx=self.style_id,
            variant_id=None,
            price=self.price,
            currency="KRW",
            original_price=self.original_price,
            date=self.date,
        )

    @property
    def style_metadata(self) -> dict | None:
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, style_idx=self.style_id, description=self.description)

    @property
    def style_crawled(self) -> dict | None:
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, style_idx=self.style_id, date=self.date)

    @property
    def brand_facet(self) -> dict | None:
        if len(self.image_urls) == 0:
            return None
        return dict(name=self.brand, type="brand", version="original")

    @property
    def brand_style_facet(self) -> dict | None:
        if len(self.image_urls) == 0:
            return None
        return dict(
            site_id=self.site_id,
            facet_idx=self.brand,
            style_idx=self.style_id,
        )

    @property
    def style_category_rank(self) -> dict | None:
        if self.rank is None:
            return None
        return dict(
            site_id=self.site_id,
            style_idx=self.style_id,
            ranking=self.rank,
            version="category",
            code=self.rank_category,
            metadata=self.rank_metadata,
            date=self.date,
        )
