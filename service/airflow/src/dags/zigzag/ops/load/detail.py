from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from core.entity.style import Style, StyleMetadata
from core.entity.facets import Facet, StyleFacet
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.infra.database.models.connections import Database
from sqlalchemy.dialects.postgresql import insert as pg_insert
import sqlalchemy as sa


class DetailLoadDataOperator(BaseOperator):
    def __init__(self, details: list[dict], site_id: str = "vPu2SsvYkCYXDCiz", db_conn_id="ncp-pg-db", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.details = details
        self.site_id = site_id

    def execute(self, context: Context):
        postgres_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        db = Database(db_hook=postgres_hook.connection, echo=False)

        style_ids = [detail["style_id"] for detail in self.details]
        with db.session() as session:
            stmt = sa.select(Style.id, Style.style_id, Style.site_id).where(
                Style.style_id.in_(style_ids), Style.site_id == self.site_id
            )
            style_id_maps_results = session.execute(stmt).all()
            style_id_maps = {style_idx: id for id, style_idx, _ in style_id_maps_results}

        style_metadata_list = []
        for detail in self.details:
            meta = detail.get("style_metadata")
            if meta:
                meta["id"] = style_id_maps.get(meta.pop("style_idx"))
                style_metadata_list.append(meta)

        if style_metadata_list:
            with db.session() as session:
                stmt = pg_insert(StyleMetadata).values(style_metadata_list).on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()

        facet_type_map = {}
        for detail in self.details:
            for facet in detail.get("facets", []):
                facet_type = facet["type"]
                if facet_type not in facet_type_map:
                    facet_type_map[facet_type] = []
                facet_type_map[facet_type].append(facet)

        facet_name_id_map = {}

        for facet_type, facet_list in facet_type_map.items():
            if not facet_list:
                continue


            with db.session() as session:
                stmt = pg_insert(Facet).values(facet_list).on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()

            with db.session() as session:
                stmt = sa.select(Facet.id, Facet.name).where(
                    Facet.name.in_([f["name"] for f in facet_list]),
                    Facet.type == facet_type
                )
                results = session.execute(stmt).all()
                facet_name_id_map[facet_type] = {name: facet_id for facet_id, name in results}

        style_facets_all = []
        for detail in self.details:
            for style_facet in detail.get("style_facets", []):
                facet_type = style_facet.pop("facet_type")
                style_idx = style_facet.pop("style_idx")
                facet_idx = style_facet.pop("facet_idx")

                style_facet["facet_id"] = facet_name_id_map[facet_type][facet_idx]
                style_facet["style_id"] = style_id_maps[style_idx]

                # 검증
                for k, v in style_facet.items():
                    assert v is not None, f"{k} is None in StyleFacet"

                style_facets_all.append(style_facet)

        if style_facets_all:
            with db.session() as session:
                stmt = pg_insert(StyleFacet).values(style_facets_all).on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()

        self.log.info(f"✅ Metadata inserted: {len(style_metadata_list)} rows")
        self.log.info(f"✅ StyleFacet inserted: {len(style_facets_all)} rows")
        self.log.info(f"✅ Facet types: {list(facet_type_map.keys())}")