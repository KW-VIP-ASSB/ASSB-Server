from sqlalchemy.orm import declarative_base
import sqlalchemy as sa
from datetime import datetime

Base = declarative_base()


class TimestampMixin:
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now, onupdate=datetime.now)
