"""Alembic environment.

The connection URL comes from app.database.connection rather than from
alembic.ini — see the comment on `sqlalchemy.url` there for why.

Importing that module also imports app.database.schemas as a side effect,
and that import is what registers every table on SQLModel.metadata.
Autogenerate diffs metadata against the live database, so without the model
classes actually imported it sees an empty metadata and cheerfully generates
a migration that drops all six tables.

Port, when running from the host: .env carries DATABASE_PORT=5433, which is
where compose publishes Postgres. The app container overrides DATABASE_PORT
to 5432 in docker-compose.yml because container-to-container traffic hits the
real port, so the same code path works in both places.
"""

from logging.config import fileConfig

from sqlalchemy import create_engine, pool

from alembic import context

from app.database import schemas
from app.database.connection import url_object

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# SQLModel's metadata is a plain SQLAlchemy MetaData, so autogenerate works
# against it directly. schemas.py sets a naming_convention on it before any
# table is defined, which is what keeps index and constraint names stable
# across autogenerate runs instead of churning on every revision.
target_metadata = schemas.SQLModel.metadata

def include_name(name, type_, parent_names):
    if type_ == "table" and name in {"debezium_signal", "debezium_heartbeat"}:
        return False
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        # hide_password=False because this string is what actually gets
        # connected with; the default renders the password as "***".
        url=url_object.render_as_string(hide_password=False),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        include_name=include_name,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # create_engine rather than engine_from_config: there is no
    # sqlalchemy.url in the ini for engine_from_config to read.
    # NullPool because a migration run is one short-lived connection and
    # pooling it would just hold the connection open afterwards.
    connectable = create_engine(url_object, poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # Without this, autogenerate compares names only and never sees a
            # changed column type. Numeric(precision, scale) on the money
            # columns and String(n) lengths are exactly the drift that went
            # unnoticed while create_all() owned the schema — create_all only
            # ever creates missing tables, it never alters an existing one.
            compare_type=True,
            # compare_server_default is left off on purpose. Postgres
            # normalises defaults on the way in (func.now() comes back as
            # "now()", text("...") comes back parenthesised), so it reports a
            # phantom diff on nearly every ts_field() column on every run.
            include_name=include_name,
            )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
