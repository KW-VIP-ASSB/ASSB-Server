import asyncio
import logging

import httpx
from airflow import XComArg
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from handsome.rank_schema import HandsomeStyleRank
from core.entity.style import Style, StyleRank, StyleCrawled
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.infra.database.models.connections import Database
from sqlalchemy.dialects.postgresql import insert as pg_insert
import sqlalchemy as sa
from sqlalchemy.orm import Session

logger = logging.getLogger()
class HandsomeLoadRankOperator(BaseOperator):
    def __init__(self, transform_data: XComArg, db_conn_id="ncp-pg-db", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.transform_data = transform_data

    def execute(self, context: Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        postgres_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        db = Database(db_hook=postgres_hook.get_connection(self.db_conn_id), echo=False)
        self.log.info(self.transform_data)
        ti = context['task_instance'] # type: ignore
        results = ti.xcom_pull(task_ids="transform.data.flatten", key="return_value")
        results = [ HandsomeStyleRank.model_validate(result) for result in results ]

        with db.session() as session:
            stmt = sa.select(Style.id, Style.style_id,Style.site_id).where(Style.style_id.in_([ result.product_id for result in results ]))
            style_id_maps_results = session.execute(stmt).all()
            style_id_maps = { style_idx : id for id,style_idx,_ in style_id_maps_results }

        style_ranks = [result.style_rank for result in results]

        def is_valid_style(style_rank):
            if not style_rank.get("style_idx") in style_id_maps :
                logger.info(f"doesn't exist {style_rank.get("style_idx")}")
                return False
            else :
                return True

        valid_style_ranks = list(filter(is_valid_style, style_ranks))

        for style_rank in valid_style_ranks:
            style_rank["style_id"] = style_id_maps[style_rank.pop("style_idx")]

        with db.session() as session:
            stmt = pg_insert(StyleRank).values(valid_style_ranks).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()

        style_crawled = [ result.style_crawled for result in results ]
        for crawled in style_crawled:
            crawled['style_id'] = style_id_maps[crawled.pop('style_idx')]
        with db.session() as session:
            stmt = pg_insert(StyleCrawled).values(style_crawled).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()

        self.log.info(f"Styles Count : {len(style_ranks)}")
