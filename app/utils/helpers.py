

from passlib.context import CryptContext
from pydantic import EmailStr
import faker as fake

from app.routers import data_models
from app.database import schemas, session

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

def credit_account() -> None:
    """Credit the specified amount to the account."""
    pass

def debit_account() -> None:
    pass






