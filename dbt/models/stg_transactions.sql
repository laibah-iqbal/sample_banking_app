select after.id as id
    , after.idempotency_key as idempotency_key
    , after.initiated_by as initiated_by
    , after.account_id as account_id
    , after.txn_type as txn_type
    , after.status as status
    , after.amount as amount
    , after.currency as currency
    , after.destination_account_iban as destination_account_iban
    , after.description as description
    , after.failure_reason as failure_reason
    , after.reverse_transaction_id as reverse_transaction_id
    , after.created_at as created_at
    , after.updated_at as updated_at
    , after.completed_at as completed_at
from {{ source('minio_raw', 'transactions') }}

