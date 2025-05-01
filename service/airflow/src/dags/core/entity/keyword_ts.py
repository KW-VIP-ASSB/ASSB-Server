from core.entity.base import Base
import sqlalchemy as sa

class KeywordTsClassification(Base):
    __tablename__ = "keyword_ts_classification"

    keyword =sa.Column(sa.String, primary_key=True)
    trend = sa.Column(sa.String)
    seasonal = sa.Column(sa.String)
    peak = sa.Column(sa.String)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True))
