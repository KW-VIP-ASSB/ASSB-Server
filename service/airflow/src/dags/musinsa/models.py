import json
from core.infra.utils.format_html import HTMLFormatter
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
from urllib.parse import urlparse, urlunparse

def remove_query_params(url) -> str:
    parsed_url = urlparse(url)
    cleaned_url = urlunparse(
        (parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, "", parsed_url.fragment)
    )
    return cleaned_url

class MusinsaStyleInfo(BaseModel):
    """무신사 상품 기본 정보를 위한 모델"""
    style_id: str
    brand: str
    site_id: str = "iylQhcSbkgVxi0Ye"
    name: str
    price: float
    original_price: float
    image_urls: List[str]
    url: str
    categories: str
    gender: str
    description: str = ""
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
        )]

    @property
    def style_metadata(self) -> dict:
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, style_idx=self.style_id,data = self.features,  description=self.description)

    @property
    def style_crawled(self) -> dict:
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, style_idx=self.style_id, date=self.date)


class MusinsaParser:
    @classmethod
    def parse(cls, product_info, date: datetime) -> MusinsaStyleInfo:
        data = product_info.get("data", {})
        
        # 이미지 URL 처리
        image_urls = [f"https://image.musinsa.com{data['thumbnailImageUrl']}"]
        for img in data.get("goodsImages", []):
            if img.get("imageUrl"):
                image_urls.append("https://image.musinsa.com" + img["imageUrl"])

        # 성별 처리
        gender_list = data.get("sex", [])
        gender = ",".join(gender_list) if gender_list else "기타"

        # 카테고리 처리
        category_str = data.get("baseCategoryFullPath", {})

        # 상품 설명 처리
        raw_description = data.get("goodsContents", "")
        description = HTMLFormatter.format_description(raw_description)
        add_description = data.get("mdOpinion", "")

        # 사이즈 정보 처리
        sizes = []
        for size_info in data.get("sizeInfo", []):
            if size_info.get("sizeName"):
                sizes.append(size_info["sizeName"])

        # 특징 정보 처리
        material_data = data.get("goodsMaterial", {})
        material_features = []
        
        if material_data and "materials" in material_data:
            for material in material_data["materials"]:
                selected_items = [
                    item["name"] for item in material["items"]
                    if item.get("isSelected", False)
                ]
                if selected_items:
                    material_features.append({
                        "name": material["name"],
                        "value": selected_items
                    })

        features = {
            "material": material_features,
        }

        assert len(image_urls) > 0, data

        return MusinsaStyleInfo(
            style_id=str(data.get("goodsNo")),
            brand=data["brand"],
            name=data.get("goodsNm", ""),
            price=float(data.get("goodsPrice", {}).get("normalPrice", 0)),
            original_price=float(data.get("goodsPrice", {}).get("originPrice", 0)),
            image_urls=image_urls,
            url=f"https://store.musinsa.com/app/goods/{data.get('goodsNo')}",
            categories=category_str,
            gender=gender,
            description=description + add_description,
            sizes=sizes if sizes else None,
            features=features,
            date=date.replace(hour=0, minute=0, second=0, microsecond=0),
        )
