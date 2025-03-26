import asyncio
import logging

import httpx
from airflow import XComArg
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from core.infra.httpx_cache.mongo import AsyncMongoDBTransport
from airflow.providers.mongo.hooks.mongo import MongoHook
from musinsa.models import MusinsaStyleInfo
from core.entity.reviews import Review, StyleReview
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.infra.database.models.connections import Database
from sqlalchemy.dialects.postgresql import insert as pg_insert
import sqlalchemy as sa
from sqlalchemy.orm import Session


class MusinsaReviewLoadDataOperator(BaseOperator):
    def __init__(self, reviews: list[dict], db_conn_id="ncp-pg-db", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.reviews = reviews

    def execute(self, context: Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        ti = context['task_instance'] # type: ignore
        postgres_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        db = Database(db_hook=postgres_hook.connection, echo=False)
        self.log.info(self.reviews[0])

        reviews = []
        style_review_maps = []

        for review in self.reviews:
            style_review_maps.append(review.pop("style_review_map"))
            reviews.append(review)
        review_ids = list(set([review["review_id"] for review in reviews]))
        review_ids = list(map(str, review_ids))

        with db.session() as session:
            stmt = sa.select(Review.review_id,Review.id).where(Review.review_id.in_(review_ids)).where(Review.site_id == "iylQhcSbkgVxi0Ye")
            results = session.execute(stmt).all()
            exist_review_idxs = { review_id:1 for review_id,_ in results }


        def review_exist(review:dict) ->bool:
            if str(review["review_id"]) in exist_review_idxs:
                return True
            review["text"] = "" if review["text"] is None else review["text"].encode().decode().replace("\x00", "\n")
            return False
        reviews = list(filter(lambda review: not review_exist(review), reviews))

        with db.session() as session:
            if len(reviews) > 0:
                stmt = pg_insert(Review.__table__).values(reviews).on_conflict_do_nothing()
                self.log.info(f"reviews insert ({len(reviews)})")
                session.execute(stmt)
                session.commit()
                self.log.info("reviews inserted")

            stmt = sa.select(Review.id, Review.review_id).where(Review.review_id.in_(review_ids)).where(Review.site_id == "iylQhcSbkgVxi0Ye")
            results = session.execute(stmt).all()
            self.log.info("reviews id fetched")
            ids = {  review_idx : review_id for review_id,review_idx in results }


        for style_review in style_review_maps:
            style_review["review_id"] = ids[str(style_review.pop("review_idx"))]

        with db.session() as session:
            stmt = pg_insert(StyleReview.__table__).values(style_review_maps).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()



        self.log.info(f"reviews count: {len(reviews)}")
        self.log.info(f"style_review_maps count: {len(style_review_maps)}")
