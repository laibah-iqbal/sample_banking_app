import uuid
from fastapi import APIRouter, HTTPException, status, Depends, Header
from fastapi.responses import JSONResponse
from sqlmodel import select
from redis.asyncio import Redis
from app.routers import data_models
from app.database.session import SessionDep
from app.utils import oauth2, idempotency, config, helpers
from app.database.schemas import User, Account, Transaction, TxnStatus, TxnType

router = APIRouter(prefix="/transactions", tags=["transactions"])
redis_client = Redis(
    host=f"{config.settings.redis_host}",
    port=f"{config.settings.redis_port}",
    decode_responses=True,
)
idempotency_manager = idempotency.IdompotencyManager(redis_client=redis_client)


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def process_transaction(
    payload: data_models.TransactionBase,
    session: SessionDep,
    current_user: User = Depends(oauth2.get_current_user),
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    # check cache
    redis_key, is_already_cached, cached_response = (
        await idempotency_manager.check_and_cache(
            idempotency_key=idempotency_key, transaction=payload
        )
    )

    # if already exists in cache, return cached response
    if is_already_cached:
        return JSONResponse(
            status_code=cached_response["status_code"],
            content=cached_response["body"],
            headers={"X-Cache": "HIT"},
        )

    # Pre-flight validation. Each failure releases the idempotency lock first —
    # otherwise the key stays PROCESSING and every retry gets a 409 until the
    # TTL expires, even once the client has corrected the request.
    if payload.transaction_type not in (
        TxnType.DEPOSIT,
        TxnType.WITHDRAWAL,
        TxnType.TRANSFER,
    ):
        await idempotency_manager.release_lock(redis_key)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported transaction type: {payload.transaction_type}",
        )

    if payload.transaction_type == TxnType.TRANSFER and not payload.target_account_iban:
        await idempotency_manager.release_lock(redis_key)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="target_account_iban is required for a transfer.",
        )

    account = session.exec(
        select(Account).where(Account.id == payload.account_id)
    ).first()
    if account is None:
        await idempotency_manager.release_lock(redis_key)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Account not found.",
        )

    # Advisory only: the authoritative check happens under row lock inside the
    # ledger helpers, which is what catches a concurrent debit racing this one.
    if account.balance < payload.amount and payload.transaction_type in (
        TxnType.WITHDRAWAL,
        TxnType.TRANSFER,
    ):
        await idempotency_manager.release_lock(redis_key)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Transaction failed. Insufficent funds in account.",
        )

    try:
        transaction_id = helpers.insert_transaction(
            transaction=payload,
            idempotency_key=idempotency_key,
            session=session,
            current_user_id=current_user.id,
        )
    except Exception as Exc:
        print(f"Transaction processing failed: {Exc!r}")
        # If DB fails, release lock so the user can retry the transfer
        await idempotency_manager.release_lock(redis_key)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Transaction failed. Idempotency lock cleared for retry.",
        )

    fraud_flag = await helpers.wait_for_fraud_result(
        redis_client, transaction_id=transaction_id
    )

    if fraud_flag is None:
        # Scoring is still in flight. This is not an error: the fraud consumer
        # settles the transaction whenever its verdict lands, so the work is
        # still on its way. Hand back the id and let the client poll
        # GET /transactions/{id}.
        #
        # The idempotency lock is deliberately NOT released. The key really is
        # still PROCESSING, so a retried POST correctly gets 409 rather than
        # inserting a second pending transaction for the same intent, and the
        # consumer overwrites the cached response once it settles.
        print(f"Fraud scoring still pending for transaction {transaction_id}")
        status_code, body = helpers.transaction_response(
            TxnStatus.PENDING, transaction_id=transaction_id
        )
        return JSONResponse(
            status_code=status_code, content=body, headers={"X-Cache": "MISS"}
        )

    # A verdict arrived, which means the consumer has already committed the
    # settlement — it only publishes after the commit. So report the row's real
    # state rather than re-deriving the outcome here; the consumer is the single
    # writer, and two settlement paths racing on the same row is exactly the
    # thing worth avoiding.
    #
    # expire_all() first: this session loaded the transaction as PENDING during
    # insert_transaction, and session.get() would serve that copy straight back
    # out of the identity map without ever querying.
    session.expire_all()
    txn = session.get(Transaction, transaction_id)

    status_code, body = helpers.transaction_response(
        txn.status, txn.failure_reason, transaction_id=transaction_id
    )
    return JSONResponse(
        status_code=status_code, content=body, headers={"X-Cache": "MISS"}
    )


@router.get("/{transaction_id}")
async def get_transaction_status(
    transaction_id: uuid.UUID,
    session: SessionDep,
    current_user: User = Depends(oauth2.get_current_user),
):
    """
    Poll target for a transaction whose fraud verdict hadn't landed by the time
    the POST returned.

    Postgres is the source of truth here, not the fraud_result Redis key. That
    key is only a short-lived (300s) handoff signal between the consumer and a
    request that happens to be waiting; the transaction row is the durable
    record, and a client may well poll long after the key has expired.
    """
    txn = session.get(Transaction, transaction_id)

    # Same 404 for "doesn't exist" and "isn't yours", so transaction ids can't
    # be enumerated by watching which ones come back 403 instead.
    if txn is None or str(txn.initiated_by) != str(current_user.id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Transaction not found.",
        )

    return {
        "transaction_id": str(txn.id),
        "status": txn.status,
        "transaction_type": txn.txn_type,
        # str, not float: the response shouldn't be where NUMERIC(18,2) money
        # quietly becomes a binary float.
        "amount": str(txn.amount),
        "currency": txn.currency,
        "failure_reason": txn.failure_reason,
        "created_at": txn.created_at,
        "completed_at": txn.completed_at,
    }
