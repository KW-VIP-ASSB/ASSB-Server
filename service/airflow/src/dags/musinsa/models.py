import json
from core.infra.utils.format_html import HTMLFormatter
from typing import List
from pydantic import BaseModel
from datetime import datetime


class MusinsaStyleInfo(BaseModel):
    """무신사 상품 기본 정보를 위한 모델"""

    product_id: str
    site_id: str = "iylQhcSbkgVxi0Ye"
    brand: str
    name: str
    gender: List[str]
    category: str
    sub_categories: List[str]
    price: int
    original_price: int
    material: str  # JSON string 형태로 저장
    image_urls: List[str]
    product_url: str
    description: str
    date: datetime

    @property
    def style(self) -> dict:
        if len(self.image_urls) == 0:
            return None
        return dict(
            site_id=self.site_id,
            style_id=self.product_id,
            code=self.product_id,
            name=self.name,
            url=self.product_url,
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
        if len(self.image_urls) == 0:
            return None
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
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, style_idx=self.product_id, description=self.description)

    @property
    def style_crawled(self) -> dict:
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, style_idx=self.product_id, date=self.date)

    @property
    def brand_facet(self) -> dict | None:
        if len(self.image_urls) == 0:
            return None
        return dict(name=self.brand, type="brand", version="original")

    @property
    def brand_style_facet(self) -> dict | None:
        if len(self.image_urls) == 0:
            return None
        return dict(site_id=self.site_id, facet_idx=self.brand, style_idx=self.product_id)


class MusinsaParser:
    @classmethod
    def parse(cls, product_info, date: datetime) -> MusinsaStyleInfo:
        data = product_info.get("data", {})
        # 이미지 URL만 리스트로 추출
        image_urls = [f"https://image.musinsa.com{data['thumbnailImageUrl']}"]
        for img in data.get("goodsImages", []):
            if img.get("imageUrl"):
                image_urls.append("https://image.musinsa.com" + img["imageUrl"])

        # 성별 처리
        gender = data.get("sex", [])

        # 카테고리 처리
        category_info = data.get("category", {})
        main_category = category_info.get("categoryDepth1Name", "")

        # 서브카테고리 처리
        sub_categories = []
        for depth in range(2, 5):  # 2, 3, 4 depth 처리
            category_name = category_info.get(f"categoryDepth{depth}Name")
            if category_name:  # 값이 존재하고 빈 문자열이 아닌 경우만 추가
                sub_categories.append(category_name)

        # 소재 정보 처리
        material = json.dumps(data.get("goodsMaterial", {}), ensure_ascii=False)

        # 상품 설명 처리
        raw_description = data.get("goodsContents", "")
        description = HTMLFormatter.format_description(raw_description)
        add_description = data.get("mdOpinion", "")

        assert len(image_urls) > 0, data
        # Pydantic 모델을 사용하여 데이터 검증 및 반환
        return MusinsaStyleInfo(
            product_id=str(data.get("goodsNo")),  # product_id를 문자열로 변환
            brand=data["brand"],
            name=data.get("goodsNm", ""),
            gender=gender,
            category=main_category,
            sub_categories=sub_categories,
            price=data.get("goodsPrice", {}).get("normalPrice", 0),
            original_price=data.get("goodsPrice", {}).get("originPrice", 0),
            material=material,
            image_urls=image_urls,  # 필드명을 모델과 일치하도록 수정
            product_url=f"https://store.musinsa.com/app/goods/{data.get('goodsNo')}",
            description=description + add_description,  # 새로 추가된 필드
            date=date.replace(hour=0, minute=0, second=0, microsecond=0),
        )
