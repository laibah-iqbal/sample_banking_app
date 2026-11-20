"""replica identity full for cdc before images

Revision ID: 07d9e6820274
Revises: 195721f396f8
Create Date: 2026-11-16 15:30:29.679731

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
# SQLModel maps a bare `str` field to sqlmodel.sql.sqltypes.AutoString, and
# autogenerate renders that fully-qualified name into the migration body.
# Without this import every generated revision dies with NameError: name
# 'sqlmodel' is not defined. Unused in revisions that touch no str column,
# which is harmless.
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = '07d9e6820274'
down_revision: Union[str, Sequence[str], None] = '195721f396f8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Only the mutable tables. REPLICA IDENTITY FULL writes the entire old row
# into WAL on every UPDATE and DELETE, so it is a real I/O cost and is only
# worth paying where the previous values are actually wanted:
#
#   accounts         balance is mutable; the before-image gives the prior
#                    balance for free, which is otherwise only derivable by
#                    replaying the whole ledger
#   users            attribute changes, for SCD-style history in silver
#   account_holders   low volume, and relationship changes matter for lineage
#
# Deliberately excluded: ledger_entries and user_accountactivity are
# append-only, so a before-image would never be populated and the WAL cost
# would buy nothing. transactions is excluded too — its mutations are status
# transitions (PENDING -> COMPLETED/FAILED) that are already recoverable from
# the ordered event stream, and it is the highest-write table here, so it is
# where FULL would cost the most. Add it if a silver model ever needs the
# prior status in the same record rather than from the preceding event.
TABLES = ("accounts", "users", "account_holders")


def upgrade() -> None:
    """Upgrade schema."""
    # op.execute because Alembic has no operation for REPLICA IDENTITY, and
    # autogenerate neither detects nor reverts it — which makes hand-written
    # DDL the correct answer here rather than a workaround.
    #
    # Not retroactive: WAL already written keeps the old shape, so only
    # changes made after this runs carry full before-images.
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} REPLICA IDENTITY FULL")


def downgrade() -> None:
    """Downgrade schema."""
    # DEFAULT means "primary key columns only", which is what every table
    # here started as — not NOTHING, which would stop deletes replicating
    # altogether.
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} REPLICA IDENTITY DEFAULT")
