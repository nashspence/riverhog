from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from .config import SQLITE_PATH


class Base(DeclarativeBase):
    pass


engine = create_engine(
    f"sqlite:///{SQLITE_PATH}",
    connect_args={"check_same_thread": False},
    future=True,
)


@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA foreign_keys=ON;")
    cursor.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def migrate_schema() -> None:
    inspector = inspect(engine)
    with engine.begin() as conn:
        job_columns = {column["name"] for column in inspector.get_columns("jobs")} if inspector.has_table("jobs") else set()
        if "keep_buffer_after_archive" not in job_columns:
            conn.execute(
                text(
                    "ALTER TABLE jobs ADD COLUMN "
                    "keep_buffer_after_archive BOOLEAN NOT NULL DEFAULT 0"
                )
            )

        disc_columns = {column["name"] for column in inspector.get_columns("discs")} if inspector.has_table("discs") else set()
        if "burn_confirmed_at" not in disc_columns:
            conn.execute(
                text(
                    "ALTER TABLE discs ADD COLUMN "
                    "burn_confirmed_at DATETIME"
                )
            )


@contextmanager
def session_scope() -> Session:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
