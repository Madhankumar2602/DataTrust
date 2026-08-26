"""Database connection helpers for MySQL and isolated SQLite tests."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config import settings


def create_database_engine(database_url: str | None = None) -> Engine:
    """Create an engine from DATABASE_URL, without opening a connection yet."""
    url = database_url or settings.DATABASE_URL
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, future=True, connect_args=connect_args)


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Build a session factory bound to the supplied engine."""
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
