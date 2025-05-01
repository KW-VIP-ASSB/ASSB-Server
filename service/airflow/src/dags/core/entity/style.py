from datetime import datetime


from core.entity.base import Base
from .brand import Site
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg
from pgvector.sqlalchemy import Vector


class Style(Base):
    __tablename__ = "style"

    id = sa.Column(sa.BigInteger, autoincrement=True, primary_key=True)
    site_id = sa.Column(sa.String(16), sa.ForeignKey("site.id", ondelete="CASCADE"), nullable=False)
    style_id = sa.Column(sa.String(50), nullable=False)
    code = sa.Column(sa.String(50))
    name = sa.Column(sa.Text, nullable=True)
    url = sa.Column(sa.String(500), nullable=True)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now, onupdate=datetime.now)


class StyleVariant(Base):
    __tablename__ = "style_variant"

    id = sa.Column(sa.BigInteger, autoincrement=True, primary_key=True)
    site_id = sa.Column(sa.String(16), sa.ForeignKey("site.id"), nullable=False)
    style_id = sa.Column(sa.BigInteger, sa.ForeignKey("Style.id"), nullable=False)
    variant_id = sa.Column(sa.String(50), nullable=False)
    url = sa.Column(sa.String(500), nullable=True)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now, onupdate=datetime.now)


class StyleImage(Base):
    __tablename__ = "style_image"

    id = sa.Column(sa.BigInteger, autoincrement=True, primary_key=True)
    site_id = sa.Column(sa.String(16), sa.ForeignKey(Site.id), nullable=False)
    style_id = sa.Column(sa.BigInteger, sa.ForeignKey(Style.id), nullable=False)
    variant_id = sa.Column(sa.BigInteger, sa.ForeignKey(StyleVariant.id), nullable=True)
    position = sa.Column(sa.SmallInteger, nullable=False, default=0)
    origin = sa.Column(sa.String(300), nullable=True)
    image = sa.Column(sa.String(300), nullable=True)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now, onupdate=datetime.now)


class StyleImageVector(Base):
    __tablename__ = "style_image_vector"

    id = sa.Column(sa.BigInteger, sa.ForeignKey(StyleImage.id), primary_key=True)
    site_id = sa.Column(
        sa.String(16),
        sa.ForeignKey("site.id", ondelete="CASCADE"),
        nullable=False,
    )
    style_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey(Style.id, ondelete="CASCADE"),
        nullable=False,
    )
    variant_id = sa.Column(sa.BigInteger, nullable=True)
    position = sa.Column(sa.Integer, nullable=False, default=0)
    embedding = sa.Column("clip_embedding", Vector(512), nullable=False)


class StylePrice(Base):
    __tablename__ = "style_price"

    id = sa.Column(sa.BigInteger, autoincrement=True, primary_key=True)
    site_id = sa.Column(sa.String(16), sa.ForeignKey("site.id"), nullable=False)
    style_id = sa.Column(sa.BigInteger, sa.ForeignKey("Style.id"), nullable=False)
    variant_id = sa.Column(sa.BigInteger, sa.ForeignKey("StyleVariant.id"), nullable=True)
    original_price = sa.Column(sa.Float, nullable=False)
    price = sa.Column(sa.Float, nullable=False)
    currency = sa.Column(sa.String(5), nullable=False)
    date = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)


class StyleMetadata(Base):
    __tablename__ = "style_metadata"

    id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey(Style.id, ondelete="CASCADE", onupdate="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    site_id = sa.Column(sa.String(16), sa.ForeignKey("site.id", ondelete="CASCADE"), nullable=False)
    description = sa.Column(sa.Text, nullable=False)
    data = sa.Column(sa.JSON, default={}, nullable=False)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now, onupdate=datetime.now)


class StyleCrawled(Base):
    __tablename__ = "style_crawled"

    id = sa.Column(sa.BigInteger, autoincrement=True, primary_key=True)
    site_id = sa.Column(sa.String(16), sa.ForeignKey("site.id", ondelete="CASCADE"), nullable=False)
    style_id = sa.Column(sa.BigInteger, sa.ForeignKey("Style.id", ondelete="CASCADE"), nullable=False)
    date = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now)
    updated_at = sa.Column(
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        default=datetime.now,
        onupdate=datetime.now,
    )


class StyleRank(Base):
    __tablename__ = "style_rank"

    id = sa.Column(sa.BigInteger, autoincrement=True, primary_key=True)
    site_id = sa.Column(sa.String(16), sa.ForeignKey("site.id", ondelete="CASCADE"), nullable=False)
    style_id = sa.Column(sa.BigInteger, nullable=False)
    ranking = sa.Column(sa.Integer, nullable=False)

    version = sa.Column(sa.String(100), nullable=False)
    code = sa.Column(sa.String(100), nullable=False)
    m_data = sa.Column("metadata", pg.JSONB, default={})
    date = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now, onupdate=datetime.now)
