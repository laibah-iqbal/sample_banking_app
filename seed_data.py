import argparse

from faker import Faker
from sqlmodel import Session

from app.database.connection import create_db_and_tables, engine
from app.database.schemas import Account, AccountHolder, User


def seed_data(user_count: int) -> None:
    fake = Faker()
    create_db_and_tables()

    with Session(engine) as session:
        for _ in range(user_count):
            user = User(
                phone_number=fake.unique.phone_number(),
                first_name=fake.first_name(),
                last_name=fake.last_name(),
                password=fake.password(length=24),
                date_of_birth=fake.date_of_birth(minimum_age=18, maximum_age=90),
                email=fake.unique.email(),
                address=fake.address(),
            )
            session.add(user)
            session.flush()

            account = Account(
                balance=round(fake.pyfloat(min_value=0, max_value=10000), 2),
                status="ACTIVE",
                account_type=fake.random_element(elements=("CHECKING", "SAVINGS")),
                iban=fake.unique.iban(),
            )
            session.add(account)
            session.flush()

            session.add(
                AccountHolder(
                    user_id=user.id,
                    account_id=account.id,
                    relationship="PRIMARY",
                )
            )

        session.commit()

    print(f"Inserted {user_count} users, accounts, and account holders.")


if __name__ == "__main__":
    
    seed_data(10)