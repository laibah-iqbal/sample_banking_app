from fastapi import FastAPI
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database.connection import create_db_and_tables
from app.routers.transactions import router as transactions_router
from app.routers.users import router as users_router
from app.routers.authentication import router as authentication_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    '''This function replaces on_event("startup") legacy function
        by putting the create_metadalta function inside the create_db_and_tables function 
        and calling it inside the lifespan function, we ensure tables are not created 
        multiple times on app reload or importing the main file in other modules. 
        This is a more efficient and cleaner approach to creating tables in the database.'''
    create_db_and_tables()
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(users_router)
app.include_router(transactions_router)
app.include_router(authentication_router)
