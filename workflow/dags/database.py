import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

POSTGRES_ETL_USER = os.getenv("POSTGRES_ETL_USER")
POSTGRES_ETL_PASSWORD = os.getenv("POSTGRES_ETL_PASSWORD")
POSTGRES_ETL_DB = os.getenv("POSTGRES_ETL_DB")

PG_DSN = os.getenv("PG_DSN", f"postgresql+psycopg2://{POSTGRES_ETL_USER}:{POSTGRES_ETL_PASSWORD}@postgres-etl:5432/{POSTGRES_ETL_DB}")
engine = create_engine(PG_DSN, future=True, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
