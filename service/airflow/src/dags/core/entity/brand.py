from core.entity.base import Base
import sqlalchemy as sa


class Site(Base):
    __tablename__ = "site"

    id = sa.Column(sa.String(16), primary_key=True)
    name = sa.Column(sa.String(50), nullable=False)
