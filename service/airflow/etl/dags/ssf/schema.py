import json
import re
from core.infra.utils.format_html import HTMLFormatter
from typing import List
from pydantic import BaseModel
from core.entity.style import Style
from datetime import datetime
from bs4 import BeautifulSoup
import logging
from urllib.parse import urlparse, urlunparse

log = logging.getLogger()


def remove_query_params(url) -> str:
    parsed_url = urlparse(url)
    cleaned_url = urlunparse(
        (parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, "", parsed_url.fragment)
    )
    return cleaned_url


class SsfSchema(BaseModel):
    style_id: str
    brand: str
    site_id: str = "Komw3BAkvYVSx_bJ"
    name: str
    price: float
    original_price: float
    image_urls: List[str]
    url: str
    gender: str
    description : str =""
    date: datetime

    @property
    def style(self) -> dict | None:
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
    def images(self) -> list[dict]| None:
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
    def style_price(self) -> dict| None:
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
    def style_metadata(self) -> dict| None:
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, style_idx=self.style_id, description=self.description)

    @property
    def style_crawled(self) -> dict| None:
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, style_idx=self.style_id, variant_id=None, date=self.date)

    

    @classmethod
    def build(cls , product_data :dict, category_mapping: dict, date:datetime):
        
        ctg_code = product_data.pop("ctg_code", "")
        
        product_data["url"] = f"https://www.ssfshop.com/{product_data["brand"]}/{product_data["style_id"]}/good?dspCtgryNo={ctg_code}"

        product_data["gender"] = category_mapping.get(ctg_code, "")
        product_data["date"] = date
        price_info = product_data.get("price_info", "")
        prices = re.findall(r"\d{1,3}(?:,\d{3})*", price_info)
        
        def parse_price(price_str: str) -> float:
            """문자열 가격을 쉼표 제거 후 숫자로 변환"""
            return float(price_str.replace(",", ""))

        if len(prices) >= 2:
            product_data["original_price"] = parse_price(prices[0])
            product_data["price"] = parse_price(prices[-1])
        elif len(prices) == 1:
            product_data["original_price"] = parse_price(prices[0])
            product_data["price"] = parse_price(prices[0])
        else:
            product_data["original_price"] = 0.0
            product_data["price"] = 0.0

        product_data.pop("price_info")
        
        return SsfSchema(**product_data)
    

