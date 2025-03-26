import json
from typing import Any, List
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from musinsa.models import MusinsaParser,MusinsaStyleInfo



class TransformReviewResponseDataOperator(BaseOperator):
    reviews: List[dict]
    def __init__(self, reviews: List[Any], **kwargs) -> None:
        super().__init__(**kwargs)
        self.reviews = reviews

    def execute(self, context:Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        reviews = [self.transform(review, date=today) for review in self.reviews]
        self.log.info(f"reviews count: {len(reviews)}")
        return reviews

    def transform(self, review:dict, date:datetime)->dict:
        writed_at = datetime.fromisoformat(review["createDate"])
        writed_date = writed_at.replace(hour=0, minute=0, second=0, microsecond=0)
        text=review["content"].encode().decode().replace("\x00", "\n") if review["content"] is not None else None
        return dict(
            style_review_map=dict(style_id=review["style_id"], review_idx=review["no"], site_id="iylQhcSbkgVxi0Ye", rating=review["grade"], writed_date=writed_date),
            review_id=review["no"],
            site_id="iylQhcSbkgVxi0Ye",
            rating=review["grade"],
            recommended=False,
            verifed_purchaser=False,
            title=None,
            text=text,
            author_id=review['encryptedUserId'],
            author_name=review["userProfileInfo"]["userNickName"],
            writed_at=writed_at,
            writed_date=writed_date,

        )
