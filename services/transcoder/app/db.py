"""SQLAlchemy engine, session and declarative base."""

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config import get_settings

settings = get_settings()

# SQLAlchemy 2.x requires the postgresql:// scheme (postgres:// is deprecated).
_db_url = settings.DATABASE_URL
if _db_url.startswith("postgres://"):
    _db_url = "postgresql://" + _db_url[len("postgres://"):]

engine = create_engine(
    _db_url,
    future=True,
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
