from fastapi import APIRouter, HTTPException, status, Depends, Header
from fastapi.responses import JSONResponse
from sqlmodel import select 
from redis.asyncio import Redis
from app.routers import data_models
from app.database.session import SessionDep
from app.utils import oauth2, idempotency, config, helpers
from app.database.schemas import User, Account


router = APIRouter(prefix="/transactions", tags=["transactions"])
redis_client = Redis(host=f"{config.settings.redis_host}", port=f"{config.settings.redis_port}", decode_responses=True)
idempotency_manager = idempotency.IdompotencyManager(redis_client=redis_client)

@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def process_transaction(payload: data_models.TransactionBase, 
                              session: SessionDep, current_user: User = Depends(oauth2.get_current_user), 
                              idempotency_key: str = Header(..., alias="Idempotency-Key")):
    # check cache
    redis_key, is_already_cached, cached_response = await idempotency_manager.check_and_cache(idempotency_key=idempotency_key,
                                                                                          transaction=payload)
    
    # if already exists in cache, return cached response
    if is_already_cached:
        return JSONResponse(
            status_code=cached_response["status_code"],
            content=cached_response["body"],
            headers={"X-Cache": "HIT"}
        )
    
    account = session.exec(select(Account).where(Account.id == payload.account_id)).first()
    account_balance = account.get("balance")
    if account_balance < payload.amount:
        # insert failed transaction
        raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Transaction failed. Insufficent funds in account."
                )
        
    try:
        helpers.insert_transaction(transaction=payload,idempotency_key=idempotency_key,
                                   session=session,current_user_id=current_user.id)
        # process transaction
            # 1. insert into transaction table with status pending
            # 2. insert into WAL for Kafka processing
            # 3. if transaction failed: return 500
            # 4. if transaction success: cache response, return 200
        # await Kafka consumer response
            # insert ledger entries
            # change status to COMPLETED
    except Exception as Exc:
        print(f"Transaction processing failed: {Exc!r}")
        # If DB fails, release lock so the user can retry the transfer
        await idempotency_manager.release_lock(redis_key)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Transaction failed. Idempotency lock cleared for retry."
        )
        

