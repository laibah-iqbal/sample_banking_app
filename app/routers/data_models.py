from fastapi import FastAPI, Body, Response, HTTPException, status
from pydantic import BaseModel, EmailStr
from sqlmodel import Field
from typing import Optional
from datetime import date, timezone, datetime

from app.database.schemas import TxnType


class UserBase(BaseModel):
    email : EmailStr 
    password : str
    phone_number : str 
    first_name : str
    last_name : str
    date_of_birth : date
    address : str
    
class UserRegisterOutput(BaseModel):
    email : EmailStr 
    phone_number : str 
    first_name : str
    last_name : str 

class UserID(UserBase):
    id : str

class AccountBase(BaseModel):
    status: str
    account_type: str
    iban: str
    balance: float
    currency: str = "USD"
    status: str = "ACTIVE"

class AccountCreate(AccountBase):
    user_id: str
    relationship: str

class UserOut(BaseModel):
    email : EmailStr
    accounts : list[AccountBase]
    
class TransactionBase(BaseModel):
    initiated_by: str
    account_id: str
    transaction_type: TxnType
    amount: float
    currency: str = "USD"
    description: Optional[str] = None
    target_account_iban: Optional[str] = None

class TransactionFailed(TransactionBase):
    reason: str

class TransactionSuccess(TransactionBase):
    transaction_id: str
    completed_at: datetime
    
class LedgerEntry(BaseModel):
    transaction_id : str
    transaction_type: str
    receiver_account: str
    direction: str
    amount : float
    balance_after : float
    created_at : datetime
    
    
