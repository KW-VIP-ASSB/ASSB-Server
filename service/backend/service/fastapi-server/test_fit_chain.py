import asyncio
from llm_process import fit_item

# 테스트용
user_info = {
    "gender": "여성",
    "height": 163,
    "weight": 50,
    "body_shape": "마른 체형",
    "preferred_style": "미니멀"
}

style_data = {
    "style_id": 100003,
    "product_name": "린넨 슬림핏 셔츠",
    "description": "가볍고 통기성 좋은 린넨 소재의 슬림핏 셔츠입니다. 여름철 데일리룩으로 적합하며, 단정한 카라와 기본 기장으로 다양한 스타일에 매치 가능합니다.",
    "site_id": "musinsa",
    "url": "https://store.musinsa.com/app/goods/100003",
    "price": 39000,
    "original_price": 49000,
    "currency": "KRW",
    "season": ["봄", "여름"],
    "material": ["린넨", "코튼"],
    "fit": "슬림핏",
    "category": "셔츠",
    "length": "기본",
    "sleeve_length": "긴팔",
    "neck_type": "카라넥",
    "color": "화이트",
    "style_tags": ["미니멀", "베이직"],
    "image_urls": [
        "https://image.musinsa.com/images/goods_img/202405010001.jpg",
        "https://image.musinsa.com/images/goods_img/202405010002.jpg"
    ]
}

async def main():
    result = await fit_item(user_info=user_info, style_data=style_data)
    print("분석 결과:\n")
    for k, v in result.items():
        print(f"{k}: {v}")

if __name__ == "__main__":
    asyncio.run(main())