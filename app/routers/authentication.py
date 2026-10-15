from fastapi import APIRouter, status, HTTPException, Depends
from fastapi.security import OAuth2PasswordRequestForm

from app.database.schemas import User
from app.utils.oauth2 import UserLogin
from app.utils.helpers import hash_password, verify
from app.utils.oauth2 import create_access_token
create_access_token
from app.database.session import SessionDep
from sqlmodel import select


router = APIRouter(tags=["authentication"])

@router.post('/login', status_code=status.HTTP_200_OK)
def login(*, user_credentials: OAuth2PasswordRequestForm = Depends(), session: SessionDep): # * allows non-default session argument to follow default user_credentials argument
    user = session.exec(
        select(User).where(User.email == user_credentials.username)
    ).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password.",
        )
        
    if not verify(user_credentials.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid password.",
        )

    access_token = create_access_token(data={"user_id": str(user.id)})
    return {"access_token": access_token, "token_type": "bearer"}