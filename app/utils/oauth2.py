from datetime import datetime, timedelta
from typing import Optional
import uuid

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr
from sqlmodel import select

from app.utils.config import settings
from app.database.session import SessionDep
from app.database.schemas import User



class UserLogin(BaseModel):
    email: EmailStr
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    id: Optional[str] = None
    
# -----------------------------------------------------------------
# JWT HELPERS       
# -----------------------------------------------------------------

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

def create_access_token(data: dict):
    """Creates a JWT access token with an expiration time."""
    to_encode = data.copy() 
    expire = datetime.now() + timedelta(minutes=settings.access_token_expire_minutes)
    to_encode.update({"exp": expire})  
    encoded_jwt = jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)
    return encoded_jwt 

def verify_access_token(token: str, credentials_exception):
    """Verifies the access token and retrieves the token data."""
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        id_: str = payload.get("user_id")
        if id_ is None:
            raise credentials_exception
        token_data = TokenData(id=id_)
    except JWTError:
        raise credentials_exception
    return token_data

def get_current_user(*, token: str = Depends(oauth2_scheme), session: SessionDep): # * allows non-default session argument to follow default token argument
    """
    Returns the current user based on the access token.
    This function first verifies the token, then queries the database
    to retrieve the user object.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, 
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"}
    )
    token_data = verify_access_token(token, credentials_exception)
    
    user = session.exec(
            select(User).where(User.id == uuid.UUID(token_data.id))
        ).first() # I convert id to a string when encoding it for token generation so UUID parses it back as a uuid 
    if not user:
        raise credentials_exception
    return user