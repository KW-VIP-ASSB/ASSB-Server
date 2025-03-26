import json
from urllib.parse import parse_qs, urlparse
from core.infra.utils.format_html import HTMLFormatter
from typing import List
from pydantic import BaseModel
from datetime import datetime


class TwentyninecmSchema(BaseModel):
    """29cm 상품 기본 정보를 위한 모델"""

    style_id: str
    site_id: str = "qSh_hnGm49bnPArh"
    brand: str
    title: str
    gender: List[str]
    category: str
    sub_categories: List[str]
    price: int
    material: str
    description: str
    original_price: int
    image_urls: List[str]
    url: str
    date: datetime
    rank: int | None
    rank_category: str | None
    rank_metadata: list[dict] = []

    @classmethod
    def build(cls, style: dict, date: datetime) -> List["TwentyninecmSchema"]:
        def extract_ctg_code_and_period(url: str):
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)

            ctg_code = query_params.get("categoryList", [""])[0]
            period = query_params.get("periodSort", [""])[0]

            return ctg_code, period

        parsed_styles = []
        item_list = style.get("data", {}).get("content", [])
        url = style.get("araas_url", "")
        ctg_code, period = extract_ctg_code_and_period(url)
        date = style.get("araas_date", "")

        for index, item in enumerate(item_list):
            product_id = str(item.get("itemNo", ""))
            brand = item.get("frontBrandNameKor", "")
            name = item.get("itemName", "")
            rank = index
            product_url = f"https://product.29cm.co.kr/catalog/{product_id}"
            genders = set()
            categories = set()

            for category_info in item.get("frontCategoryInfo", []):
                category = {
                    category_info.get("category1Code", ""): category_info.get("category1Name", ""),
                    category_info.get("category2Code", ""): category_info.get("category2Name", ""),
                    category_info.get("category3Code", ""): category_info.get("category3Name", "")
                }
    
                categories.update(category.values())  

                if int(ctg_code) in category:
                    genders.add(category_info.get("category1Name", ""))  

            genders = list(genders)
            categories = list(categories)

            parsed_styles.append(cls(
                style_id=str(product_id),
                brand=brand,
                title=name,
                gender=genders,
                category=ctg_code,
                sub_categories=categories,
                price=item.get("lastSalePrice", 0),
                original_price=item.get("consumerPrice", 0),
                image_urls=[f"https://img.29cm.co.kr{item.get('imageUrl', '')}"],
                url=product_url,
                rank=rank,
                material =  product_url,
                description = product_url,
                rank_category=ctg_code,
                rank_metadata=[{"period": period, "url":url }],
                date=date.replace(hour=0, minute=0, second=0, microsecond=0),
            ))

        return parsed_styles 

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
        return dict(name=self.brand, type="brand", version="")

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
