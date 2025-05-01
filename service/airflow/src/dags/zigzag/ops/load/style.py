from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from core.entity.style import Style,StyleImage,StyleMetadata,StylePrice,StyleCrawled
from core.entity.facets import Facet, StyleFacet
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.infra.database.models.connections import Database
from sqlalchemy.dialects.postgresql import insert as pg_insert
import sqlalchemy as sa
from zigzag.schema import ZigzagSchema

class StyleLoadDataOperator(BaseOperator):
    def __init__(self, styles: list[dict], db_conn_id="ncp-pg-db", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.styles = styles

    def execute(self, context: Context):
        postgres_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        db = Database(db_hook=postgres_hook.connection, echo=False)
        results = [ ZigzagSchema.model_validate(result) for result in self.styles ]
        styles = [ result.style for result in results  if result.style is not None]

        with db.session() as session:
            stmt = pg_insert(Style).values(styles).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()
            stmt = sa.select(Style.id, Style.style_id,Style.site_id).where(Style.style_id.in_([ result.style_id for result in results ])).where(
                Style.site_id == "vPu2SsvYkCYXDCiz"
            )
            style_id_maps_results = session.execute(stmt).all()
            style_id_maps = { style_idx : id for id,style_idx,_ in style_id_maps_results }
            self.log.info(list(style_id_maps.keys()))
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
                meta['id'] = style_id_maps[meta.pop('style_idx')]
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

        brands_facets = [result.brand_facet for result in results if result.brand_facet is not None]
        brand_style_facets = [result.brand_style_facet for result in results if result.brand_style_facet is not None]

        category_facets = [facet for result in results for facet in (result.category_facets or [])]
        category_style_facets = [facet for result in results for facet in (result.category_style_facets or [])]

        facet_name_id_map = {
            "brand": {},
            "category": {}
        }

        for facet_type, facets in [
            ("brand", brands_facets),
            ("category", category_facets),
        ]:
            with db.session() as session:
                stmt = pg_insert(Facet).values(facets).on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()

            with db.session() as session:
                stmt = sa.select(
                    Facet.id, Facet.name
                ).where(
                    Facet.name.in_([facet['name'] for facet in facets])
                ).where(Facet.type == facet_type)
                results = session.execute(stmt).all()
                facet_name_id_map[facet_type].update({facet_name: facet_id for facet_id, facet_name in results})

        for style_facet in (brand_style_facets   + category_style_facets):
            facet_type = style_facet.pop('facet_type')
            style_facet['facet_id'] = facet_name_id_map[facet_type][style_facet.pop('facet_idx')]
            style_facet['style_id'] = style_id_maps[style_facet.pop('style_idx')]
            for k, v in style_facet.items():
                assert v is not None, f"{k} is None"

        with db.session() as session:
            all_style_facets = (brand_style_facets  + category_style_facets)
            stmt = pg_insert(StyleFacet).values(all_style_facets).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()
            

        self.log.info(f"Crawled Count : {len(style_crawled)}")
        self.log.info(f"Styles Count : {len(styles)}")
        self.log.info(f"Images Count : {len(images)}")
        self.log.info(f"Prices Count : {len(style_prices)}")
        self.log.info(f"Metadata Count : {len(style_metadata)}")
        self.log.info(f"Crawled Count : {len(style_crawled)}")
        self.log.info(f"Brand Facets Count: {len(brands_facets)}")
        # self.log.info(f"Color Facets Count: {len(color_facets)}")
        # self.log.info(f"Size Facets Count: {len(size_facets)}")
        self.log.info(f"Category Facets Count: {len(category_facets)}")
        self.log.info(f"Total Style Facets Count: {len(all_style_facets)}")
