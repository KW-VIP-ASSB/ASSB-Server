from typing import List
from pydantic import BaseModel
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class HandsomeStyleRank(BaseModel):
    product_id: str
    site_id: str = "iK9Z0qwLb-snTbnE"
    rank: int
    rank_category: int
    sort_code: str
    date: datetime

    @property
    def style_rank(self) -> dict:
        return dict(
            site_id=self.site_id,
            style_idx=self.product_id,
            value=self.rank,
            category=self.rank_category,
            filter=self.sort_code,
            rank_updated_at=self.date,
            rank_type="sales",
        )

    @property
    def style_crawled(self) -> dict:
        return dict(site_id=self.site_id, style_idx=self.product_id, variant_id=None, date=self.date)


class HandsomeRankParser:
    @classmethod
    def parse(cls, product_info: dict, date: datetime) -> List[HandsomeStyleRank]:
        parsed_styles_rank = []

        product_id = str(product_info.get("goodsNo", ""))
        ctg_code = product_info.get("main_ctg", "")
        filter = product_info.get("sort_code", "")
        rank_value = int(product_info.get("rank", ""))
        style_info = HandsomeStyleRank(
            product_id=product_id,
            rank=rank_value,
            rank_category=ctg_code,
            sort_code=filter,
            date=date,
        )
        parsed_styles_rank.append(dict(style_info))

        return parsed_styles_rank
