"""create publication and grant rights to user debezium

Revision ID: 3e8fc76400cc
Revises: 07d9e6820274
Create Date: 2026-11-26 01:16:33.893770

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
revision: str = '3e8fc76400cc'
down_revision: Union[str, Sequence[str], None] = '07d9e6820274'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLES = ("transactions", "ledger_entries", "user_accountactivity", "account_holders", "users", "accounts")
def upgrade() -> None:
    """Upgrade schema."""
    for table in TABLES:
        op.execute(f"GRANT SELECT ON {table} TO debezium;")
    op.execute("CREATE PUBLICATION debezium_publication FOR TABLE transactions, ledger_entries, user_accountactivity, account_holders, users, accounts")


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP PUBLICATION IF EXISTS debezium_publication")
