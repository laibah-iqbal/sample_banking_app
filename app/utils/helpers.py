import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import json
from typing import NamedTuple
import uuid

from fastapi import Depends, status
from fastapi.security import OAuth2PasswordRequestForm

from passlib.context import CryptContext
from pydantic import EmailStr
import faker as fake
from sqlmodel import Session, select, update

from app.routers import data_models
from app.database import schemas, session
from app.utils import oauth2

from app.database.schemas import (
    AccountType,
    Direction,
    User,
    Account,
    AccountHolder,
    Transaction,
    TxnStatus,
    TxnType,
    UserAccountActivity,
)

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


def insert_transaction(
    *,
    transaction: data_models.TransactionBase,
    idempotency_key: str,
    session: session.SessionDep,
    current_user_id: str,
):

    transaction = schemas.Transaction(
        idempotency_key=idempotency_key,
        initiated_by=current_user_id,
        account_id=transaction.account_id,
        txn_type=transaction.transaction_type,
        amount=transaction.amount,
        currency=transaction.currency,
        description=transaction.description,
        destination_account_iban=transaction.target_account_iban,
    )
    session.add(transaction)
    session.commit()
    session.refresh(transaction)

    return transaction.id


def log_account_activity(
    activity: str,
    activity_outcome: str,
    session: session.SessionDep,
    user_id: str,
    outcome_reason: str = "",
):
    accountactivity = schemas.UserAccountActivity(
        initiated_by=user_id,
        activity=activity,
        activity_outcome=activity_outcome,
        outcome_reason=outcome_reason,
    )
    session.add(accountactivity)
    session.commit()
    session.refresh(accountactivity)


async def wait_for_fraud_result(
    redis_client, transaction_id: str, timeout: float = 2.0
):
    key = f"fraud_result:{transaction_id}"
    channel = key

    # 1. Maybe the consumer already finished before we even started waiting
    existing = await redis_client.get(key)
    if existing is not None:
        return json.loads(existing)

    # 2. Not there yet — subscribe BEFORE doing anything else, so we can't
    #    miss a publish that happens while we're setting up
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)
    try:
        # 3. Re-check the key now that we're subscribed. This closes the
        #    gap between step 1's GET and step 2's SUBSCRIBE actually
        #    registering — without this, a publish landing in exactly
        #    that gap would be missed by both the GET and the listen().
        existing = await redis_client.get(key)
        if existing is not None:
            return json.loads(existing)

        # 4. Now actually wait, bounded by a timeout
        async with asyncio.timeout(timeout):
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                return json.loads(message["data"])
    except TimeoutError:
        return None  # caller falls back to 202 Accepted + client polls GET /transactions/{id}
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()


class InsufficientFunds(Exception):
    """Raised when a debit would take an account below zero."""


def _internal_account_id(session: Session) -> uuid.UUID:
    """
    Resolve the internal clearing account. Only the id is read here — the
    balance is left until _lock_accounts so it is never read outside a lock.
    .one() means a missing or ambiguous internal account fails loudly rather
    than silently booking against whichever one happens to be picked.
    """
    return session.exec(
        select(Account.id).where(Account.account_type == AccountType.INTERNAL)
    ).one()


def _lock_accounts(
    session: Session, *account_ids: uuid.UUID
) -> dict[uuid.UUID, Account]:
    """
    Row-lock every account a transaction touches, returned keyed by id.

    Two things matter here. The lock has to be taken before the balance is
    read, or two concurrent writers both compute from the same stale value
    and one update is silently lost. And the locks must be acquired in a
    consistent order across all operations — otherwise two opposing
    transfers (A->B and B->A) each hold one row, wait on the other, and
    deadlock.
    """
    ordered = sorted({uuid.UUID(str(account_id)) for account_id in account_ids})
    return {
        account_id: session.exec(
            select(Account).where(Account.id == account_id).with_for_update()
        ).one()
        for account_id in ordered
    }


def _apply_entry(
    session: Session,
    account: Account,
    direction: Direction,
    amount: Decimal,
    transaction_id: uuid.UUID,
) -> schemas.LedgerEntry:
    """
    Move `amount` on an already-locked account and record the matching entry.
    balance_after is derived from the same locked read that produces the new
    balance, so the ledger cannot drift from accounts.balance.
    """
    if direction is Direction.DEBIT:
        new_balance = account.balance - amount
    else:
        new_balance = account.balance + amount

    account.balance = new_balance

    entry = schemas.LedgerEntry(
        transaction_id=transaction_id,
        account_iban=account.iban,
        direction=direction,
        amount=amount,
        balance_after=new_balance,
    )
    session.add(entry)
    return entry


def _account_id_by_iban(session: Session, iban: str) -> uuid.UUID | None:
    """
    Resolve an IBAN to an account id, or None if this system doesn't hold that
    account. Only the id is read, so the balance is still only ever read under
    lock in _lock_accounts.
    """
    return session.exec(
        select(Account.id).where(Account.iban == iban)
    ).one_or_none()


def _record_untracked_entry(
    session: Session,
    iban: str,
    direction: Direction,
    amount: Decimal,
    transaction_id: uuid.UUID,
) -> schemas.LedgerEntry:
    """
    Record an entry against an account this system doesn't hold — an external
    IBAN, say. There's no row to lock and no balance to maintain, so
    balance_after stays NULL; the entry exists so the transaction still
    satisfies debits == credits.
    """
    entry = schemas.LedgerEntry(
        transaction_id=transaction_id,
        account_iban=iban,
        direction=direction,
        amount=amount,
        balance_after=None,
    )
    session.add(entry)
    return entry


def _require_funds(account: Account, amount: Decimal) -> None:
    if account.balance < amount:
        raise InsufficientFunds(
            f"Account {account.iban} holds {account.balance}, needs {amount}"
        )


def process_deposit(
    *,
    session: Session,
    account_id: uuid.UUID,
    amount: Decimal,
    transaction_id: uuid.UUID,
) -> list[schemas.LedgerEntry]:
    """
    Money entering the system: customer credited, internal clearing account
    debited. Does not commit — the caller commits once so the entries, the
    balances and the transaction status all land in one database transaction.
    """
    internal_id = _internal_account_id(session)
    accounts = _lock_accounts(session, internal_id, account_id)

    return [
        _apply_entry(
            session, accounts[internal_id], Direction.DEBIT, amount, transaction_id
        ),
        _apply_entry(
            session,
            accounts[uuid.UUID(str(account_id))],
            Direction.CREDIT,
            amount,
            transaction_id,
        ),
    ]


def process_withdrawal(
    *,
    session: Session,
    account_id: uuid.UUID,
    amount: Decimal,
    transaction_id: uuid.UUID,
) -> list[schemas.LedgerEntry]:
    """Money leaving the system: customer debited, clearing account credited."""
    internal_id = _internal_account_id(session)
    accounts = _lock_accounts(session, internal_id, account_id)
    customer = accounts[uuid.UUID(str(account_id))]

    _require_funds(customer, amount)

    return [
        _apply_entry(session, customer, Direction.DEBIT, amount, transaction_id),
        _apply_entry(
            session, accounts[internal_id], Direction.CREDIT, amount, transaction_id
        ),
    ]


def process_transfer(
    *,
    session: Session,
    source_account_id: uuid.UUID,
    target_account_iban: str,
    amount: Decimal,
    transaction_id: uuid.UUID,
) -> list[schemas.LedgerEntry]:
    """
    Money moving out of the source account to `target_account_iban`.

    The target is addressed by IBAN rather than id because it need not be an
    account this system holds. If it is, it gets locked and its balance
    maintained like any other. If it isn't, the credit is still recorded
    against the raw IBAN with a NULL balance_after — so the transaction
    balances either way, and we never write a one-sided entry.
    """
    target_id = _account_id_by_iban(session, target_account_iban)

    to_lock = [source_account_id] if target_id is None else [source_account_id, target_id]
    accounts = _lock_accounts(session, *to_lock)

    source = accounts[uuid.UUID(str(source_account_id))]
    _require_funds(source, amount)

    debit = _apply_entry(session, source, Direction.DEBIT, amount, transaction_id)

    if target_id is None:
        credit = _record_untracked_entry(
            session, target_account_iban, Direction.CREDIT, amount, transaction_id
        )
    else:
        credit = _apply_entry(
            session, accounts[target_id], Direction.CREDIT, amount, transaction_id
        )

    return [debit, credit]


# -----------------------------------------------------------------
# SETTLEMENT
# -----------------------------------------------------------------


def post_ledger_entries(
    session: Session, txn: Transaction
) -> list[schemas.LedgerEntry]:
    """
    Dispatch to the double-entry handler for this transaction's type.

    Everything is read off the persisted transaction row rather than the API
    request body, because the fraud consumer settles transactions it never saw
    a request for — all it receives over Kafka is a transaction id. That is
    also why the transfer destination has to be persisted on the row rather
    than only living in the payload.

    Does not commit. The caller commits once, so the ledger entries, the
    account balances and the transaction status all land together or not at all.
    """
    if txn.txn_type == TxnType.DEPOSIT:
        return process_deposit(
            session=session,
            account_id=txn.account_id,
            amount=txn.amount,
            transaction_id=txn.id,
        )

    if txn.txn_type == TxnType.WITHDRAWAL:
        return process_withdrawal(
            session=session,
            account_id=txn.account_id,
            amount=txn.amount,
            transaction_id=txn.id,
        )

    if txn.txn_type == TxnType.TRANSFER:
        if not txn.destination_account_iban:
            raise ValueError(
                f"Transaction {txn.id} is a transfer with no destination IBAN."
            )
        return process_transfer(
            session=session,
            source_account_id=txn.account_id,
            target_account_iban=txn.destination_account_iban,
            amount=txn.amount,
            transaction_id=txn.id,
        )

    raise ValueError(f"No ledger handler for {txn.txn_type}")


class Settlement(NamedTuple):
    """
    The outcome of settling one transaction, plus what the caller needs in
    order to report it: the idempotency key, so the cached response can be
    rewritten under the key the next retry will look under, and the failure
    reason, so the client is told why rather than just that it failed.
    """

    status: TxnStatus
    idempotency_key: str
    failure_reason: str | None


def settle_transaction(
    session: Session, transaction_id: uuid.UUID | str, fraud_flag: int
) -> Settlement | None:
    """
    Apply the fraud verdict to a pending transaction: post the ledger and mark
    it completed, or mark it failed. Commits. Returns None when there was
    nothing to do — the transaction doesn't exist, or someone already settled it.

    The row is locked and re-checked for PENDING before anything is written,
    because this runs from a Kafka consumer. Delivery is at-least-once, and the
    upsert-kafka sink re-emits a row every time a transaction's 30-minute login
    aggregate changes, so the same transaction id genuinely does arrive more
    than once. Without this guard each arrival would post another balanced pair
    of ledger entries and move the balance again — the ledger would still
    balance, which is exactly what makes the bug hard to spot.
    """
    transaction_id = uuid.UUID(str(transaction_id))

    txn = session.exec(
        select(Transaction).where(Transaction.id == transaction_id).with_for_update()
    ).one_or_none()

    if txn is None or txn.status != TxnStatus.PENDING:
        return None

    # Captured now: after a rollback below the instance is expired, and
    # re-reading it would mean another query.
    idempotency_key = txn.idempotency_key

    if fraud_flag == 1:
        reason = "Fraudulent transaction."
        txn.status = TxnStatus.FAILED
        txn.failure_reason = reason
        session.commit()
        return Settlement(TxnStatus.FAILED, idempotency_key, reason)

    try:
        post_ledger_entries(session, txn)
    except InsufficientFunds as exc:
        # Nothing has actually been written — the funds check runs before the
        # first entry — but roll back anyway so the FAILED flip can't inherit a
        # half-applied unit of work, then re-apply it on a clean transaction.
        session.rollback()
        reason = str(exc)
        session.exec(
            update(Transaction)
            .where(Transaction.id == transaction_id)
            .values(status=TxnStatus.FAILED, failure_reason=reason)
        )
        session.commit()
        return Settlement(TxnStatus.FAILED, idempotency_key, reason)

    txn.status = TxnStatus.COMPLETED
    txn.completed_at = datetime.now(timezone.utc)
    session.commit()
    return Settlement(TxnStatus.COMPLETED, idempotency_key, None)


def transaction_response(
    txn_status: TxnStatus | str,
    failure_reason: str | None = None,
    transaction_id: uuid.UUID | str | None = None,
) -> tuple[int, dict]:
    """
    Map a transaction status onto the HTTP status and body used to report it.

    Kept in one place because three callers have to agree: the POST when its
    wait resolves, the GET a poller hits, and the fraud consumer when it writes
    the cached idempotent response. If they drift, a client gets one answer
    live and a different one when it retries the same idempotency key.
    """
    if txn_status == TxnStatus.COMPLETED:
        return status.HTTP_200_OK, {"message": "Transaction completed successfully."}

    if txn_status == TxnStatus.FAILED:
        return status.HTTP_400_BAD_REQUEST, {
            "message": "Transaction failed.",
            "reason": failure_reason,
        }

    # Still PENDING: scoring hasn't landed yet. Not an error — the client is
    # given the handle and polls.
    body = {"message": "Transaction is still processing.", "status": TxnStatus.PENDING}
    if transaction_id is not None:
        body["transaction_id"] = str(transaction_id)
        body["poll_url"] = f"/transactions/{transaction_id}"
    return status.HTTP_202_ACCEPTED, body