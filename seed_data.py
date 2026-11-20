"""Seed reference data: users, accounts, and the account_holders link rows.

No transactions and no ledger entries. That is deliberate rather than just
scope — see the note on opening balances below.

Run from the host, where .env points DATABASE_PORT at 5433:

    .venv/bin/python seed_data.py

Idempotent. Rows are matched on their natural keys (users.email,
accounts.iban), so re-running adds nothing and changes nothing. Faker is
seeded with a fixed value, so the generated people and IBANs are identical on
every run and on every machine.

Does NOT create tables. Alembic owns the schema now; this script fails with a
pointer to `alembic upgrade head` if the tables aren't there.
"""

from __future__ import annotations

import sys
from decimal import Decimal

from faker import Faker
from sqlalchemy import inspect
from sqlmodel import Session, create_engine, select

from app.database.connection import url_object
from app.database.schemas import (
    Account,
    AccountHolder,
    AccountHoldersRelationship,
    AccountStatus,
    AccountType,
    CustomerStatus,
    User,
)
from app.utils.helpers import hash_password

# The shared engine in app.database.connection is created with echo=True, which
# turns a seed run into thousands of lines of SQL. Same URL, quiet.
engine = create_engine(url_object, echo=False)

# Fixed seed: deterministic output, so the same email addresses and IBANs exist
# after every reseed and anything you hardcode in a test keeps working.
fake = Faker("en_GB")
Faker.seed(20261117)

# Every seeded user shares this password so you can log in as any of them.
# Fine for a local fixture, obviously not for anything else.
SEED_PASSWORD = "Password123!"

NUM_CUSTOMERS = 8

# Opening balances are all zero, including the internal clearing account.
#
# accounts.balance is not independent data — it is the running total of that
# account's ledger_entries, maintained by _apply_entry under a row lock. Seeding
# a non-zero balance with no matching entries would put the two permanently out
# of agreement, and the double-entry invariant (every transaction books a
# balanced debit/credit pair) would be false from the first row.
#
# To fund an account, POST a deposit through the API. That runs process_deposit,
# which debits the clearing account and credits the customer in one committed
# transaction, so the balance and the ledger move together.
OPENING_BALANCE = Decimal("0")

# The IBAN the internal clearing account is created with. It is looked up by
# account_type rather than by IBAN (see _internal_account_id), so this value is
# cosmetic — but a recognisable one beats a random string when reading the
# ledger by hand.
INTERNAL_IBAN = "GB00BANK00000000000001"


def require_tables() -> None:
    """Fail early and legibly rather than on a confusing IntegrityError."""
    expected = {"users", "accounts", "account_holders"}
    present = set(inspect(engine).get_table_names())
    missing = expected - present
    if missing:
        print(
            f"missing tables: {', '.join(sorted(missing))}\n"
            "run `alembic upgrade head` first — this script does not create "
            "the schema.",
            file=sys.stderr,
        )
        raise SystemExit(1)


def make_customer() -> User:
    """One customer. Ids come from the model's default_factory (uuid7)."""
    first_name = fake.first_name()
    last_name = fake.last_name()
    return User(
        # fake.unique guarantees no collision with the unique index on email,
        # which would otherwise surface as an IntegrityError on commit.
        email=fake.unique.email(),
        password_hash=hash_password(SEED_PASSWORD),
        phone_number=fake.unique.msisdn(),
        first_name=first_name,
        last_name=last_name,
        date_of_birth=fake.date_of_birth(minimum_age=18, maximum_age=90),
        address=fake.address().replace("\n", ", "),
        status=CustomerStatus.ACTIVE,
    )


def make_account(account_type: AccountType, iban: str | None = None) -> Account:
    return Account(
        balance=OPENING_BALANCE,
        status=AccountStatus.ACTIVE,
        account_type=account_type,
        iban=iban if iban is not None else fake.unique.iban(),
    )


def seed(session: Session) -> dict[str, int]:
    counts = {"users": 0, "accounts": 0, "account_holders": 0}

    # ---------------------------------------------------------------
    # The internal clearing account.
    #
    # Created first and on its own because every deposit and withdrawal books
    # against it, and _internal_account_id resolves it with .one() — so zero
    # accounts of this type breaks those flows, and two breaks them just as
    # loudly. It gets no account_holders row: it belongs to the bank, not to a
    # customer, which is also why the balance_non_negative constraint exempts
    # it (a deposit debits clearing, so it legitimately runs negative).
    # ---------------------------------------------------------------
    internal = session.exec(
        select(Account).where(Account.account_type == AccountType.INTERNAL)
    ).first()
    if internal is None:
        internal = make_account(AccountType.INTERNAL, iban=INTERNAL_IBAN)
        session.add(internal)
        counts["accounts"] += 1
        print(f"internal clearing account: {internal.iban}")
    else:
        print(f"internal clearing account already exists: {internal.iban}")

    # ---------------------------------------------------------------
    # Customers, each with a checking account they hold as primary.
    # ---------------------------------------------------------------
    customers: list[User] = []
    checking: list[Account] = []

    for _ in range(NUM_CUSTOMERS):
        user = make_customer()
        existing = session.exec(
            select(User).where(User.email == user.email)
        ).first()
        if existing is not None:
            # Faker is seeded, so a re-run regenerates this same person. Reuse
            # the stored row and keep going, which is what makes the script
            # safe to run repeatedly.
            customers.append(existing)
            continue

        session.add(user)
        counts["users"] += 1
        customers.append(user)

    # flush so the users have database-visible ids before account_holders rows
    # reference them — without this the composite FK insert has nothing to
    # point at.
    session.flush()

    for user in customers:
        account = make_account(AccountType.CHECKING)
        existing = session.exec(
            select(Account).where(Account.iban == account.iban)
        ).first()
        if existing is not None:
            checking.append(existing)
            continue

        session.add(account)
        counts["accounts"] += 1
        session.flush()
        checking.append(account)

        session.add(
            AccountHolder(
                user_id=user.id,
                account_id=account.id,
                relationship=AccountHoldersRelationship.PRIMARY,
            )
        )
        counts["account_holders"] += 1

    # ---------------------------------------------------------------
    # A few extra shapes, so the link table isn't uniformly one-user-one-account
    # and all three relationship values actually appear.
    # ---------------------------------------------------------------

    # Savings accounts for the first two customers, held as primary.
    for user in customers[:2]:
        savings = make_account(AccountType.SAVINGS)
        if session.exec(
            select(Account).where(Account.iban == savings.iban)
        ).first() is None:
            session.add(savings)
            counts["accounts"] += 1
            session.flush()
            session.add(
                AccountHolder(
                    user_id=user.id,
                    account_id=savings.id,
                    relationship=AccountHoldersRelationship.PRIMARY,
                )
            )
            counts["account_holders"] += 1

    # A joint account: customers[2] primary, customers[3] joint. Two rows in
    # account_holders for one account, which is the case the composite primary
    # key (user_id, account_id) exists to allow.
    joint = make_account(AccountType.CHECKING)
    if session.exec(select(Account).where(Account.iban == joint.iban)).first() is None:
        session.add(joint)
        counts["accounts"] += 1
        session.flush()
        for user, rel in (
            (customers[2], AccountHoldersRelationship.PRIMARY),
            (customers[3], AccountHoldersRelationship.JOINT),
        ):
            session.add(
                AccountHolder(
                    user_id=user.id, account_id=joint.id, relationship=rel
                )
            )
            counts["account_holders"] += 1

    # customers[4] named as a beneficiary on customers[0]'s checking account —
    # the third relationship value, and a second holder who is not an owner.
    beneficiary_exists = session.exec(
        select(AccountHolder).where(
            AccountHolder.user_id == customers[4].id,
            AccountHolder.account_id == checking[0].id,
        )
    ).first()
    if beneficiary_exists is None:
        session.add(
            AccountHolder(
                user_id=customers[4].id,
                account_id=checking[0].id,
                relationship=AccountHoldersRelationship.BENEFICIARY,
            )
        )
        counts["account_holders"] += 1

    session.commit()
    return counts


def main() -> int:
    require_tables()

    with Session(engine) as session:
        counts = seed(session)

        print(
            "\ninserted: "
            f"{counts['users']} users, "
            f"{counts['accounts']} accounts, "
            f"{counts['account_holders']} account_holders"
        )

        totals = {
            "users": len(session.exec(select(User)).all()),
            "accounts": len(session.exec(select(Account)).all()),
            "account_holders": len(session.exec(select(AccountHolder)).all()),
        }
        print(
            "totals now: "
            f"{totals['users']} users, "
            f"{totals['accounts']} accounts, "
            f"{totals['account_holders']} account_holders"
        )

        print(f"\nlog in as any seeded user with password: {SEED_PASSWORD}")
        for user in session.exec(select(User).order_by(User.email)).all():
            print(f"  {user.email}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
