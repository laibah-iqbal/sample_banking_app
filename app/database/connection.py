from sqlmodel import create_engine
from sqlalchemy import URL
from app.database import schemas
from app.utils.config import settings

url_object = URL.create(
    "postgresql+psycopg2",
    username=f"{settings.database_username}",
    password=f"{settings.database_password}",
    host=f"{settings.database_hostname}",
    # Previously omitted, so every connection silently fell back to libpq's
    # default 5432. That went unnoticed while only the containerised app used
    # this engine, but the fraud consumer now settles transactions through it
    # and is run from the host, where Postgres is published on 5433.
    port=int(settings.database_port),
    database=f"{settings.database_name}",
)

engine = create_engine(url_object, echo=True)

def create_db_and_tables():
    schemas.SQLModel.metadata.create_all(engine)