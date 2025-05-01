from datetime import datetime
from enum import Enum
import sqlalchemy as sa
from src.db.schema.base import Base
from sqlalchemy.dialects import postgresql as pg


class FacetType(str, Enum):
    CATEGORY = "category"
    BRAND = "brand"
    SUBCATEGORY = "subcategory"
    GENDER = "gender"
    COLOR = "colors"


class Facet(Base):
    __tablename__ = "facet"

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String(100), nullable=False)
    type = sa.Column(sa.String(50), nullable=False)
    version = sa.Column(sa.String(10), nullable=True, default="original")
    data = sa.Column(pg.JSONB, default={}, nullable=True)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now, onupdate=datetime.now)


class StyleFacet(Base):
    __tablename__ = "style_facet"

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    site_id = sa.Column(sa.String(16), sa.ForeignKey("site.id", ondelete="CASCADE"), nullable=False)
    style_id = sa.Column(sa.BigInteger, sa.ForeignKey("style.id", ondelete="CASCADE"), nullable=False)
    facet_id = sa.Column(sa.BigInteger, sa.ForeignKey("facets.id", ondelete="CASCADE"), nullable=False)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now, onupdate=datetime.now)
