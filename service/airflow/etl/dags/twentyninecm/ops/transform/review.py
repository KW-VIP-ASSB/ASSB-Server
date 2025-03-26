from functools import reduce
import json
from typing import Any, List
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
import logging
from typing import List

from bs4 import BeautifulSoup


__SITE_ID__ = "qSh_hnGm49bnPArh"


class TransformReviewResponseDataOperator(BaseOperator):
    reviews: List[dict]
    def __init__(self, reviews: List[Any], **kwargs) -> None:
        super().__init__(**kwargs)
        self.reviews = reviews

    def execute(self, context:Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        reviews = [self.transform(review, date=today) for review in self.reviews]
        reviews = list(reduce(lambda x, y: x + y, reviews, []))
        self.log.info(f"reviews count: {len(reviews)}")
        self.log.info(f"review {reviews[0]}")
        return reviews

    def transform(self, response:dict, date:datetime)->list[dict]:
        reviews = []
        review_id = response.get("itemReviewNo")
        author_name = response.get("userId", "Unknown")
        user_size = response.get("userSize", [])
        rating = response.get("point", 0)
        text = response.get("contents", "")
        written_at = response.get("insertTimestamp", "")
        style_id = response['araas_style_id']

        #TODO recommended 로직 구현
        recommended = False
        response = dict(
            style_review=dict(style_id=style_id, review_idx= review_id, site_id=__SITE_ID__, rating=rating, writed_date=written_at),
            review_id=review_id,
            site_id=__SITE_ID__,
            writed_at=written_at,
            writed_date=written_at,
            author_name=author_name,
            author_id=None,
            rating=int(rating),
            recommended=recommended,
            verifed_purchaser=True,
            title=None,
            text=text,
        )
        reviews.append(response)
        return reviews
