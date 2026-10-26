from typing import List

from fastapi import APIRouter, status, Depends
from sqlmodel import select 
from faker import Faker

from app.database import schemas
from app.routers import data_models
from app.utils.helpers import hash_password, log_account_activity
from app.database.session import SessionDep
from app.utils.oauth2 import get_current_user

router = APIRouter(prefix="/users", tags=["users"])
fake = Faker()

@router.post("", response_model=data_models.UserOut, status_code=status.HTTP_201_CREATED)
async def register_user(payload: data_models.UserBase, session: SessionDep) -> schemas.User:
    password_hash = hash_password(payload.password)
    user = schemas.User(
        email=payload.email,
        password_hash=password_hash,
        phone_number=payload.phone_number,
        first_name=payload.first_name,
        last_name=payload.last_name,
        date_of_birth=payload.date_of_birth,
        address=payload.address,
    )
    user_id = user.id
    session.add(user)
    session.commit()
    session.refresh(user)
    
    account = schemas.Account(
        balance=0,
        status="active",
        account_type=fake.random_element(elements=("checking", "savings")),
        iban=fake.unique.iban(),
    )
    account_id = account.id
    session.add(account)
    session.commit()
    session.refresh(account)
    
    accountholder = schemas.AccountHolder(
        user_id=user_id,
        account_id=account_id,
        relationship="primary"
    )
    session.add(accountholder)
    session.commit()
    session.refresh(accountholder)
    
    log_account_activity(activity="registered", activity_outcome="success",
                         session=session, user_id=user_id)
    
    return user

@router.get("/accounts", response_model=list[data_models.AccountBase])
async def get_user_accounts(session: SessionDep, current_user: data_models.UserBase = Depends(get_current_user)):
    return current_user.accounts

@router.post("/account_history/{account_id}", response_model=list[data_models.LedgerEntry])
async def select_account(account_id: str, session: SessionDep, current_user: data_models.UserBase = Depends(get_current_user)):
    ledger_entries = session.exec(
            select(schemas.LedgerEntry).where(schemas.LedgerEntry.account_id == account_id)
        ).all()
    # join to ledger and transactions to get transaction type and receiver account
    return ledger_entries



# @router.patch("", status_code=HTTP_501_NOT_IMPLEMENTED)
# async def update_account_info(new_info: dict, session: SessionDep, current_user: data_models.UserBase = Depends(get_current_user)):
#     # if delete user make user inactive
#     # make any priamry single accounts inactive
#     # pass off any joint accounts to joint as primary
#     # only other fields that can be updated are password and address
#     pass

# Users
# view accounts [DONE]
# make user inactive 

# Accounts
# Create account (checking, savings)
# add beneificiary or joint holder on account (separate process flow handled through kafka)
# make account inactive
# view account ledger

# Transactions ( needs Redis, + Kafka implementation)
# deposit
# withdraw
# fee
# reversal
# transfer
# create transaction record
# create ledger entry