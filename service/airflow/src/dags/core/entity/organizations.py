from core.entity.base import Base, TimestampMixin
import sqlalchemy as sa


class Organization(Base, TimestampMixin):
    __tablename__ = "organization"

    id = sa.Column(sa.String(16), primary_key=True)
    name = sa.Column(sa.String(100))
    desc = sa.Column("description", sa.Text(), server_default="", nullable=False, default="")
    is_active = sa.Column(sa.Boolean, default=True)


class OrganizationMember(Base, TimestampMixin):
    __tablename__ = "organization_member"

    id = sa.Column(primary_key=True, autoincrement=True)
    organization_id = sa.Column(sa.String(16), sa.ForeignKey("organization.id", ondelete="CASCADE"))
    user_id = sa.Column(sa.String(16), sa.ForeignKey("user.id", ondelete="CASCADE"))
    is_active = sa.Column(sa.Boolean, default=True, server_default=sa.sql.expression.true())


class OrganizationResource(Base, TimestampMixin):
    __tablename__ = "organization_resource"

    id = sa.Column(sa.BigInteger, primary_key=True)
    organization_id = sa.Column(sa.String(16), sa.ForeignKey("organization.id", ondelete="CASCADE"))
    resource = sa.Column(sa.JSON(), nullable=False)
    date = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)
