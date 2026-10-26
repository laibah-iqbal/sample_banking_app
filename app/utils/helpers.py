from fastapi import Depends
from fastapi.security import OAuth2PasswordRequestForm

from passlib.context import CryptContext
from pydantic import EmailStr
import faker as fake

from app.routers import data_models
from app.database import schemas, session
from app.utils import oauth2

pwd_context = CryptContext(
    schemes=["pbkdf2_sha256", "bcrypt"],
    deprecated=["bcrypt"],
    pbkdf2_sha256__rounds=600_000,
)


def hash_password(password: str) -> str:
    """Hash a password using PBKDF2-SHA256."""
    return pwd_context.hash(password)

def verify(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


# -----------------------------------------------------------------
# ACCOUNT HELPERS
# -----------------------------------------------------------------
    

# -----------------------------------------------------------------
# TRANSACTION HELPERS
# -----------------------------------------------------------------


def insert_transaction(*, 
                       transaction: data_models.TransactionBase, idempotency_key: str,
                       session: session.SessionDep,
                       current_user_id: str):
    
    transaction = schemas.Transaction(idempotency_key=idempotency_key,
                                      initiated_by=current_user_id,
                                      account_id=transaction.account_id,
                                      txn_type=transaction.transaction_type,
                                      amount=transaction.amount,
                                      currency=transaction.currency,
                                      description=transaction.description
                                      )
    session.add(transaction)
    session.commit()
    session.refresh(transaction)

def log_account_activity(activity: str, activity_outcome: str,
                         outcome_reason: str,
                         session: session.SessionDep,
                         user_id: str):
    accountactivity = schemas.UserAccountActivity(
        initiated_by=user_id,
        activity=activity,
        activity_outcome=activity_outcome, 
        outcome_reason=outcome_reason
    )    
    session.add(accountactivity)
    session.commit()
    session.refresh(accountactivity)
        
def credit_account() -> None:
    """Credit the specified amount to the account."""
    pass

def debit_account() -> None:
    pass






