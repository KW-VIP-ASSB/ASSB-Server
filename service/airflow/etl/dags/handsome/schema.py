import re
from typing import List
from pydantic import BaseModel
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class HandsomeStyleInfo(BaseModel):
    product_id: str
    site_id: str = "iK9Z0qwLb-snTbnE"
    brand: str
    name: str
    price: float
    original_price: float
    image_urls: List[str]
    product_url: str
    categories: List[dict]
    main_ctg: str
    gender: str
    date: datetime

    @property
    def style(self) -> dict:
        return dict(
            site_id=self.site_id,
            style_id=self.product_id,
            code=self.product_id,
            name=self.name,
            url=self.product_url,
        )

    @property
    def images(self) -> list[dict]:
        images = []
        for position, image_url in enumerate(self.image_urls):
            images.append(
                dict(
                    site_id=self.site_id,
                    style_idx=self.product_id,
                    variant_id=None,
                    position=position,
                    origin=image_url,
                    image=None,
                ),
            )
        return images

    @property
    def style_price(self) -> dict:
        return dict(
            site_id=self.site_id,
            style_idx=self.product_id,
            variant_id=None,
            price=self.price,
            currency="KRW",
            original_price=self.original_price,
            date=self.date,
        )

    @property
    def style_metadata(self) -> dict:
        return dict(site_id=self.site_id, style_idx=self.product_id, description="")

    @property
    def style_crawled(self) -> dict:
        return dict(site_id=self.site_id, style_idx=self.product_id, date=self.date)


class HandsomeInfoParser:
    def get_image_urls_regex(data):
        image_urls = []
        if not data or not data.get("colorInfo"):
            return image_urls

        pattern = re.compile(r"^W\d{2}$")  # 'W'로 시작하고 뒤에 두 자리 숫자가 오는 패턴

        for color_info in data["colorInfo"]:
            if not color_info.get("colorContInfo"):
                continue

            for cont_info in color_info["colorContInfo"]:
                img_gb_cd = cont_info.get("imgGbCd")
                if img_gb_cd and pattern.match(img_gb_cd):
                    image_urls.append(cont_info.get("dispGoodsContUrl"))

        assert len(image_urls) > 0, data
        return ["https://cdn-img.thehandsome.com/studio/goods" + path for path in image_urls]

    @classmethod
    def parse(cls, product_info: dict, date: datetime) -> List[HandsomeStyleInfo]:
        parsed_styles = []

        product_id = str(product_info.get("goodsNo", ""))
        brand = product_info.get("brandNm", "")
        name = product_info.get("goodsNm", "")
        original_price = product_info.get("norPrc", 0.0)
        price = product_info.get("salePrc", 0.0)

        image_urls = cls.get_image_urls_regex(product_info)
        if len(image_urls) == 0:
            image_urls = ["https://cdn-img.thehandsome.com/studio/goods" + product_info.get("dispGoodsContUrl", "")]
        product_url = f"https://www.thehandsome.com/ko/PM/productDetail/{product_id}"
        gender = "unmapped"  # Assuming gender data needs to be handled properly
        categories = [{"category": product_info.get("dispCtgNo", "")}]
        main_ctg = product_info["main_ctg"]
        style_info = HandsomeStyleInfo(
            product_id=product_id,
            brand=brand,
            name=name,
            price=price,
            original_price=original_price,
            image_urls=image_urls,
            product_url=product_url,
            categories=categories,
            main_ctg=main_ctg,
            gender=gender,
            date=date,
        )
        parsed_styles.append(style_info)

        return parsed_styles
