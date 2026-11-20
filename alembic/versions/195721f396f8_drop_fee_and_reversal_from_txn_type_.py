"""drop fee and reversal from txn_type check

Revision ID: 195721f396f8
Revises: f8efcb7a2b73
Create Date: 2026-11-12 01:27:30.941457

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
revision: str = '195721f396f8'
down_revision: Union[str, Sequence[str], None] = 'f8efcb7a2b73'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# The bare token, not the full "ck_transactions_txn_type_valid". Alembic feeds
# this name through the metadata's naming_convention ("ck_%(table_name)s_
# %(constraint_name)s"), so passing the rendered name produces the doubled
# "ck_transactions_ck_transactions_txn_type_valid". Same string schemas.py
# hands enum_check().
CONSTRAINT = "txn_type_valid"


def upgrade() -> None:
    """Upgrade schema."""
    # TxnType dropped FEE and REVERSAL, but the live constraint was created by
    # SQLModel.metadata.create_all(), which only ever CREATEs missing tables —
    # it never alters one that already exists. So banking_app_v2 still accepts
    # txn_type='fee' while nothing in the application can produce or settle
    # one. Autogenerate won't catch this: Alembic does not diff CHECK
    # constraints, so it has to be written by hand.
    op.drop_constraint(CONSTRAINT, "transactions", type_="check")
    op.create_check_constraint(
        CONSTRAINT,
        "transactions",
        "txn_type IN ('transfer', 'deposit', 'withdrawal')",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(CONSTRAINT, "transactions", type_="check")
    op.create_check_constraint(
        CONSTRAINT,
        "transactions",
        "txn_type IN ('transfer', 'deposit', 'withdrawal', 'fee', 'reversal')",
    )
