import re
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import logging
from urllib.parse import urlparse, urlunparse
log = logging.getLogger()

from urllib.parse import urlparse, parse_qs

def remove_query_params(url) -> str:
    parsed_url = urlparse(url)
    cleaned_url = urlunparse(
        (parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, "", parsed_url.fragment)
    )
    return cleaned_url


class ZigzagSchema(BaseModel):
    style_id: str
    brand: str
    site_id: str = "vPu2SsvYkCYXDCiz"
    name: str
    price: float
    original_price: float
    image_urls: List[str]
    url: str
    categories: str
    gender: str
    description: str = ""
    sizes: Optional[List[str]] = None
    features: Optional[dict] = None
    date: datetime

    @property
    def style(self) -> dict:
        if len(self.image_urls) == 0:
            return None
        return dict(
            site_id=self.site_id,
            style_id=self.style_id,
            code=self.style_id,
            name=self.name,
            url=self.url,
        )

    @property
    def images(self) -> list[dict]:
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
                    origin=remove_query_params(image_url),
                    image=None,
                ),
            )
        assert len(images) > 0, self

        return images

    @property
    def style_price(self) -> dict:
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
            facet_type="brand",
        )

    @property
    def category_facets(self) -> list[dict] | None:
        if len(self.image_urls) == 0:
            return None
        return [dict(name=self.categories, type="category", version="original")]
        

    @property
    def category_style_facets(self) -> list[dict] | None:
        if len(self.image_urls) == 0:
            return None
        return [dict(
                site_id=self.site_id,
                facet_idx=self.categories,
                style_idx=self.style_id,
                facet_type="category",
            )
        ]

    @property
    def style_metadata(self) -> dict:
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, style_idx=self.style_id, description=self.description)

    @property
    def style_crawled(self) -> dict:
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, style_idx=self.style_id, date=self.date)


    def extract_goods_no(self, text: str):
        match = re.search(r"goods_no:\s*'(?P<goods_no>\d+)'", text)
        return match.group("goods_no") if match else None

    @classmethod

    def build(cls, item_list: list, date: datetime) -> List["ZigzagSchema"]:
        results = []

        for item in item_list:
            if item.get("__typename") != "UxGoodsCardItem":
                continue

            goods_id = item.get("goods_id")
            image_url = item.get("image_url")
            final_price = item.get("final_price", 0)
            price = item.get("price", 0)
            product_url = item.get("product_url")
            title = item.get("title")
            shop_name = item.get("shop_name")
            managed_categories = item.get("managed_category_list", [])

            # gender 추출 (depth == 2)
            gender = ""
            category_parts = []
            for cat in managed_categories:
                depth = cat.get("depth")
                value = cat.get("value", "")
                if depth == 2:
                    if "여성" in value:
                        gender = "여성"
                    elif "남성" in value:
                        gender = "남성"
                    else:
                        gender = "유니섹스"
                elif depth in [3, 4]:
                    category_parts.append(value)

            category_str = "->".join(category_parts)
            image_urls = [image_url] if image_url else []

            # ZigzagSchema 인스턴스로 변환
            style_info = ZigzagSchema(
                style_id=goods_id,
                brand=shop_name,
                name=title,
                price=float(final_price),
                original_price=float(price),
                image_urls=image_urls,
                url=product_url,
                categories=category_str,
                gender=gender,
                date = date
            )

            results.append(style_info)

        return results
