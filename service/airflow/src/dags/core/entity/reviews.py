from datetime import datetime
import sqlalchemy as sa
from core.entity.base import Base


class Review(Base):
    __tablename__ = "review"

    id = sa.Column(sa.BigInteger, autoincrement=True, primary_key=True)
    review_id = sa.Column(sa.String(50), nullable=False)
    site_id = sa.Column(sa.String(16), sa.ForeignKey("site.id", ondelete="CASCADE"), nullable=False)
    writed_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)
    writed_date = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)

    author_id = sa.Column(sa.String(100))
    author_name = sa.Column(sa.String(100))

    rating = sa.Column(sa.SMALLINT())

    recommended = sa.Column(sa.Boolean())
    verified_purchaser = sa.Column(sa.Boolean())
    title = sa.Column(sa.Text())
    text = sa.Column(sa.Text())

    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, default=datetime.now, onupdate=datetime.now)


class StyleReview(Base):
    __tablename__ = "style_review"

    id = sa.Column(sa.BigInteger, autoincrement=True, primary_key=True)
    site_id = sa.Column(sa.String(16), sa.ForeignKey("site.id", ondelete="CASCADE"), nullable=False)
    style_id = sa.Column(sa.BigInteger, sa.ForeignKey("styles.id", ondelete="CASCADE"), nullable=False)
    review_id = sa.Column(sa.BigInteger, sa.ForeignKey("review.id", ondelete="CASCADE"), nullable=False)
    rating = sa.Column(sa.SMALLINT, nullable=False)
    writed_date = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, index=True)
