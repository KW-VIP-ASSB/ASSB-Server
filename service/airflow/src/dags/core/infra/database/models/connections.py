from contextlib import contextmanager
from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session, sessionmaker

if TYPE_CHECKING:
    from airflow.models.connection import Connection  # Avoid circular imports.


class Database:
    def __init__(self, db_hook: "Connection", database: str | None = None, echo: bool = False) -> None:
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=db_hook.login,
            password=db_hook.password,
            host=db_hook.host,
            port=db_hook.port,  # Ensure port is included for PostgreSQL
            database=db_hook.schema if database is None else database,
        )
        self.engine = engine = create_engine(url, echo=echo)
        self._session_factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    @contextmanager
    def session(self):
        session: Session = self._session_factory()

        try:
            yield session
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
