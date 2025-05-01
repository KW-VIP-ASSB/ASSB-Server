from core.entity.base import Base, TimestampMixin
import sqlalchemy as sa


class StyleCollection(Base, TimestampMixin):
    __tablename__ = "style_collection"

    id = sa.Column(sa.String(24), primary_key=True)
    name = sa.Column(sa.String(100), nullable=False)
    description = sa.Column(sa.Text, nullable=False, default="")


class StyleCollectionOrgMap(Base, TimestampMixin):
    __tablename__ = "style_collection_organization_map"
    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    collection_id = sa.Column(sa.String(24), sa.ForeignKey("style_collection.id", ondelete="CASCADE"), nullable=False)
    organization_id = sa.Column(sa.String(16), sa.ForeignKey("organization.id", ondelete="CASCADE"), nullable=False)
