import json
from typing import Any, List
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from handsome.schema import HandsomeStyleInfo, HandsomeInfoParser


class TransformReviewResponseDataOperator(BaseOperator):
    reviews: List[dict]

    def __init__(self, reviews: List[Any], **kwargs) -> None:
        super().__init__(**kwargs)
        self.reviews = reviews

    def execute(self, context: Context):
        today = context["execution_date"].in_timezone("UTC").today()  # type: ignore
        reviews = [self.transform(review, date=today) for review in self.reviews]
        self.log.info(f"reviews count: {len(reviews)}")
        return reviews

    def transform(self, review: dict, date: datetime) -> dict:
        raw_date = review["revWrtDtm"]
        formatted_date = raw_date.replace(".", "-")
        writed_at = datetime.strptime(formatted_date, "%Y-%m-%d")
        text = review["revCont"].encode().decode().replace("\x00", "\n") if review["revCont"] is not None else None
        return dict(
            style_review_map=dict(
                style_id=review["style_id"],
                review_idx=review["revNo"],
                site_id="iK9Z0qwLb-snTbnE",
                rating=review["revScrVal"],
                writed_date=writed_at,
            ),
            review_id=review["revNo"],
            site_id="iK9Z0qwLb-snTbnE",
            rating=review["revScrVal"],
            recommended=(not review["hlpfulCnt"] == 0),
            verifed_purchaser=review["onlOrdYn"] == "Y",
            title=None,
            text=text,
            author_id=review["loginId"],
            author_name=review["loginId"],
            writed_at=writed_at,
            writed_date=writed_at,
        )
