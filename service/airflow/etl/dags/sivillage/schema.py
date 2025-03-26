import re
from typing import List
from pydantic import BaseModel
from datetime import datetime
import logging
from urllib.parse import urlparse, urlunparse

log = logging.getLogger()


def remove_query_params(url) -> str:
    parsed_url = urlparse(url)
    cleaned_url = urlunparse(
        (parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, "", parsed_url.fragment)
    )
    return cleaned_url


class SivillageSchema(BaseModel):
    style_id: str
    brand: str
    site_id: str = "1PdU2BdfW5R2LXRM"
    name: str
    price: float
    original_price: float
    image_urls: List[str]
    url: str
    categories: List[str]
    gender: List[str]
    description: str
    date: datetime

    @property
    def style(self) -> dict:
        return dict(
            site_id=self.site_id,
            style_id=self.style_id,
            code=self.style_id,
            name=self.name,
            url=self.url,
        )

    @property
    def images(self) -> list[dict]:
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
    def style_metadata(self) -> dict:
        return dict(site_id=self.site_id, style_idx=self.style_id, description=self.description)

    @property
    def style_crawled(self) -> dict:
        return dict(site_id=self.site_id, style_idx=self.style_id, date=self.date)


    def extract_goods_no(self, text: str):
        match = re.search(r"goods_no:\s*'(?P<goods_no>\d+)'", text)
        return match.group("goods_no") if match else None

    @classmethod
    def build(cls, ctg_gender_map: dict, product_info: dict, date: datetime) -> "SivillageSchema":
        log.info(product_info)
        meta = product_info.get("meta", {})
        images = product_info.get("images", "")
        categories = [
            meta.get("eg:category1", ""),
            meta.get("eg:category2", ""),
            meta.get("eg:category3", ""),
            meta.get("eg:category4", ""),
        ]
        genders = []
        for category in categories:
            if not ctg_gender_map.get(category):
                continue
            genders.append(ctg_gender_map.get(category))
        product_id = meta.get("eg:itemId", "unknown")
        images = list(set(images))
        assert len(images) > 0, product_info

        style_info = SivillageSchema(
            style_id=product_id,
            brand=meta.get("eg:brandName", "unknown"),
            name=meta.get("eg:itemName", "No Name"),
            price=meta.get("eg:salePrice", "0"),
            original_price=meta.get("eg:originalPrice", "0"),
            image_urls=images,
            url=f"https://www.sivillage.com/goods/initDetailGoods.siv?goods_no={product_id}",
            categories=categories,
            description=meta.get("description", ""),
            gender=genders,
            date=date,
        )
        
        return style_info
