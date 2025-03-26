from functools import reduce
import json
import re
from typing import Any, List
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
import logging
from typing import List

from bs4 import BeautifulSoup


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

    def transform(self, review:dict, date:datetime)->list[dict]:
        content = review['content']
        style_id = review['style_id']
        soup = BeautifulSoup(content, 'html.parser')
        review_items = soup.find_all('li')

        reviews = []
        for item in review_items:
            review_id = item.get('id')
            rating_tag = item.find("div", class_ = "list-status")
            rating = len(rating_tag.select('i'))
            user_tag = item.find('span', class_='list-id')
            author_name = user_tag.text.strip() if user_tag else ""
            review_content_tag = item.find('p', class_='review-txts')
            text = review_content_tag.text.strip() if review_content_tag else ""
            review_date_tag = item.find('span', class_='list-date')
            review_date = review_date_tag.text.strip() if review_date_tag else ""
            review_date = str(datetime.strptime(review_date, "%Y.%m.%d")) 
            review = dict(
                style_review=dict(
                    style_id=style_id,
                    review_idx= review_id,
                    brand_id="ssf",
                ),

                review_id=review_id,
                brand_id="ssf",

                writed_at=review_date,
                writed_date=review_date,

                author_name=author_name,
                author_id=author_name,

                rating=rating,
                recommended=False, # recommend 정보 알수없음
                verifed_purchaser=True,
                title=None,
                text=text,
            )

            reviews.append(review)

        return reviews
