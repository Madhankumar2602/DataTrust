"""FastAPI dependencies for database sessions and repository access."""

from __future__ import annotations

from collections.abc import Generator

from fastapi import Depends
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.database.connection import (
    create_database_engine,
    create_session_factory,
)
from src.database.repository import QualityRepository


_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def get_session_factory() -> sessionmaker[Session]:
    """Return the shared database session factory."""

    global _engine, _session_factory

    if _session_factory is None:
        _engine = create_database_engine()
        _session_factory = create_session_factory(_engine)

    return _session_factory


def get_db_session() -> Generator[Session, None, None]:
    """Create one database session per API request."""

    session = get_session_factory()()

    try:
        yield session
    finally:
        session.close()


def get_repository(
    session: Session = Depends(get_db_session),
) -> QualityRepository:
    """Provide a repository backed by the request database session."""

    return QualityRepository(session)
