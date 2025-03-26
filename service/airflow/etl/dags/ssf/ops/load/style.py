from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from core.entity.style import Style,StyleImage,StyleMetadata,StylePrice,StyleCrawled
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.infra.database.models.connections import Database
from sqlalchemy.dialects.postgresql import insert as pg_insert
import sqlalchemy as sa
from ssf.schema import SsfSchema

class StyleLoadDataOperator(BaseOperator):
    def __init__(self, styles: list[dict], db_conn_id="ncp-pg-db", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.styles = styles

    def execute(self, context: Context):
        postgres_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        db = Database(db_hook=postgres_hook.connection, echo=False)
        results = [ SsfSchema.model_validate(result) for result in self.styles]
        styles = [ result.style for result in results  if result.style is not None]

        with db.session() as session:
            stmt = pg_insert(Style).values(styles).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()
            stmt = sa.select(Style.id, Style.style_id,Style.site_id).where(Style.style_id.in_([ result.style_id for result in results ]))
            style_id_maps_results = session.execute(stmt).all()
            style_id_maps = { style_idx : id for id,style_idx,_ in style_id_maps_results }

        images = [ image for image_list in results for image in image_list.images ]
        for image in images:
            image['style_id'] = style_id_maps[image.pop('style_idx')]
        with db.session() as session:
            stmt = pg_insert(StyleImage).values(images).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()


        style_prices = [ result.style_price for result in results  if result.style_price is not None]
        for price in style_prices:
            price['style_id'] = style_id_maps[price.pop('style_idx')]
        with db.session() as session:
            stmt = pg_insert(StylePrice).values(style_prices).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()


        style_metadata = [ result.style_metadata for result in results  if result.style_metadata is not None]
        if len(style_metadata) > 0:
            for meta in style_metadata:
                meta['style_id'] = style_id_maps[meta.pop('style_idx')]
            with db.session() as session:
                stmt = pg_insert(StyleMetadata).values(style_metadata).on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()

        style_crawled = [ result.style_crawled for result in results if result.style_crawled is not None ]
        for crawled in style_crawled:
            crawled['style_id'] = style_id_maps[crawled.pop('style_idx')]
        with db.session() as session:
            stmt = pg_insert(StyleCrawled).values(style_crawled).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()

        self.log.info(f"Styles Count : {len(styles)}")
        self.log.info(f"Images Count : {len(images)}")
        self.log.info(f"Prices Count : {len(style_prices)}")
        self.log.info(f"Metadata Count : {len(style_metadata)}")
        self.log.info(f"Crawled Count : {len(style_crawled)}")
