from functools import reduce
from typing import Any, List
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
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
        review_items = soup.find_all('li', class_='detail__bottom-review-item')

        reviews = []
        for item in review_items:
            star_rating_tag = item.find('p', class_='siv-starpoint__bar')
            rating = int(star_rating_tag.text.strip()) if star_rating_tag else 0
            user_tag = item.find('span', class_='review-user')
            author_name = user_tag.text.strip() if user_tag else ""
            review_content_tag = item.find('p', class_='review-contents')
            text = review_content_tag.text.strip() if review_content_tag else ""
            review_date_tag = item.find('span', class_='review-writing-date')
            review_date = review_date_tag.text.strip() if review_date_tag else ""
            review_id_tag = item.find('button', class_='like-review-btn')
            review_id = review_id_tag.get('data-goods_eval_no') if review_id_tag else ""
            recommended_tag = review_id_tag.find('span', class_='text') if review_id_tag else ""
            recommended_count = int(recommended_tag.text.strip()) if recommended_tag else 0
            recommended = True if recommended_count > 0 else False
            review = dict(
                style_review=dict(
                    style_id=style_id,
                    review_idx= review_id,
                    site_id="1PdU2BdfW5R2LXRM",
                    rating=int(rating),
                    writed_date=review_date,
                ),

                review_id=review_id,
                site_id="1PdU2BdfW5R2LXRM",

                writed_at=review_date,
                writed_date=review_date,

                author_name=author_name,
                author_id=None,

                rating=int(rating),
                recommended=recommended,
                verifed_purchaser=True,
                title=None,
                text=text,
            )

            reviews.append(review)

        return reviews
