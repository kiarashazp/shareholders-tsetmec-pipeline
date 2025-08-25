"""
Database initialization script for the ETL pipeline.

This module creates all database tables defined in the SQLAlchemy models (Base).
It should be executed once when setting up a fresh PostgreSQL instance,
ensuring that the schema is created before running the ETL process.
"""

from database import engine
from models import Base


def init_db():
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    init_db()
