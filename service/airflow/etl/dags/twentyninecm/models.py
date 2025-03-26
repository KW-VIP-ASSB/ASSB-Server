from typing import List
from pydantic import BaseModel
from datetime import datetime


class TwentynineStyleInfo(BaseModel):
    product_id: str
    brand: str
    site_id: str = "qSh_hnGm49bnPArh"
    name: str
    price: float
    original_price: float
    image_urls: List[str]
    product_url: str
    categories: List[dict]
    review_count: int
    gender: List[str]
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


class TwentyNineInfoParser:
    @classmethod
    def parse(cls, product_info, date: datetime) -> List[TwentynineStyleInfo]:
        parsed_styles = []
        item_list = product_info.get("data", {}).get("content", [])
        for item in item_list:
            product_id = str(item.get("itemNo", ""))
            brand = item.get("frontBrandNameKor", "")
            name = item.get("itemName", "")
            original_price = item.get("consumerPrice", 0.0)
            price = item.get("lastSalePrice", 0.0)
            review_count = item.get("reviewCount", 0)
            image_urls = [f"https://img.29cm.co.kr{item.get('imageUrl', '')}"]
            product_url = f"https://product.29cm.co.kr/catalog/{product_id}"
            genders = []
            categories = []
            for category_info in item.get("frontCategoryInfo", []):
                category = {
                    "category1_code": category_info.get("category1Code", ""),
                    "category1_name": category_info.get("category1Name", ""),
                    "category2_code": category_info.get("category2Code", ""),
                    "category2_name": category_info.get("category2Name", ""),
                    "category3_code": category_info.get("category3Code", ""),
                    "category3_name": category_info.get("category3Name", ""),
                }
                categories.append(category)
                genders.append(category.get("category1_name"))

            assert len(image_urls) > 0, product_info

            style_info = TwentynineStyleInfo(
                product_id=product_id,
                brand=brand,
                name=name,
                price=price,
                original_price=original_price,
                image_urls=image_urls,
                product_url=product_url,
                categories=categories,
                review_count=review_count,
                gender=genders,
                date=date,
            )

            parsed_styles.append(style_info)

        return parsed_styles

    @classmethod
    def rank_parse(cls, product_info, ctg_code, date: datetime) -> List[TwentynineStyleInfo]:
        parsed_styles = []
        item_list = product_info.get("data", {}).get("content", [])

        for item in item_list:
            product_id = str(item.get("itemNo", ""))
            brand = item.get("frontBrandNameKor", "")
            name = item.get("itemName", "")
            original_price = item.get("consumerPrice", 0.0)
            price = item.get("lastSalePrice", 0.0)
            review_count = item.get("reviewCount", 0)
            image_urls = [f"https://image.29cm.co.kr{item.get('imageUrl', '')}"]
            product_url = f"https://www.29cm.co.kr/shop/item/{product_id}"
            genders = []
            categories = []
            for category_info in item.get("frontCategoryInfo", []):
                category = {
                    "category1_code": category_info.get("category1Code", ""),
                    "category1_name": category_info.get("category1Name", ""),
                    "category2_code": category_info.get("category2Code", ""),
                    "category2_name": category_info.get("category2Name", ""),
                    "category3_code": category_info.get("category3Code", ""),
                    "category3_name": category_info.get("category3Name", ""),
                }
                categories.append(category)
                genders.append(category.get("category1_name"))

            style_info = TwentynineStyleInfo(
                product_id=product_id,
                brand=brand,
                name=name,
                price=price,
                original_price=original_price,
                image_urls=image_urls,
                product_url=product_url,
                categories=categories,
                review_count=review_count,
                gender=genders,
                date=date,
            )

            parsed_styles.append(style_info)

        return parsed_styles
