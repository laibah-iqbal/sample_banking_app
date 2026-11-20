from fastapi import FastAPI

from app.routers.transactions import router as transactions_router
from app.routers.users import router as users_router
from app.routers.authentication import router as authentication_router


# No lifespan hook here on purpose. It used to call create_db_and_tables(),
# but Alembic owns the schema now: the `migrate` service in docker-compose.yml
# runs `alembic upgrade head` to completion before this container starts.
#
# Leaving create_all() in as well would make the app a second schema owner —
# on a fresh database whichever ran first would build the tables, and if that
# was create_all() there would be no alembic_version row, so Alembic would
# think it was at base and try to create everything again. create_all() also
# only ever CREATEs missing tables and never ALTERs an existing one, which is
# how the transactions txn_type CHECK constraint kept accepting 'fee' and
# 'reversal' long after they were dropped from TxnType.
app = FastAPI()
app.include_router(users_router)
app.include_router(transactions_router)
app.include_router(authentication_router)
