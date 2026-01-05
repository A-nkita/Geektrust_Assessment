from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://postgres:root@localhost:5432/geektrust_db"
)
