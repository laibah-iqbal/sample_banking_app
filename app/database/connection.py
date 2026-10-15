from sqlmodel import create_engine
from sqlalchemy import URL
from app.database import schemas
from app.utils.config import settings

url_object = URL.create(
    "postgresql+psycopg2",
    username=f"{settings.database_username}",
    password=f"{settings.database_password}",
    host=f"{settings.database_hostname}",
    database=f"{settings.database_name}",
)

engine = create_engine(url_object, echo=True)

def create_db_and_tables():
    schemas.SQLModel.metadata.create_all(engine)