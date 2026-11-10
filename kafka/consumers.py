import asyncio
import json
import os
import pandas as pd
from aiokafka import AIOKafkaConsumer
from sqlmodel import Session
from ml.train_fraud_model import featurize
import xgboost as xgb
from app.database.connection import engine
from app.utils import helpers
from app.routers.transactions import redis_client, idempotency_manager

# defaults to the host-facing listener for running this locally;
# override with KAFKA_BOOTSTRAP_SERVERS=kafka:19092 when run inside a container
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

class CustomConsumer:
    def __init__(self, topic: str, group_id: str):
        self.consumer = AIOKafkaConsumer(
        topic,  # Debezium topic format: <topic.prefix>.<schema>.<table>
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,  # Consumer Group ID
        auto_offset_reset="earliest",          # Read from beginning if new group
        enable_auto_commit=True)
    
    async def _consume_messages(self):
        await self.consumer.start()
        try:
            print("Listening for CDC events from PostgreSQL...")
                
            async for msg in self.consumer:
                await self._process_message(msg)
                    
        finally:
            await self.consumer.stop()
            print("Stopped consumer.")
    
    async def _process_message(self, payload):
        pass

class FraudDetectionConsumer(CustomConsumer):
    def __init__(self, topic, group_id, model):
        super().__init__(topic, group_id)
        self.model = model
        
    def _settle(self, transaction_id: str, fraud_flag: int):
        """
        The blocking database half of settlement, run off the event loop.

        psycopg2 is synchronous and this holds a row lock while it posts the
        ledger, so calling it inline would stall the event loop — and with it
        every other consumer sharing this process — for the whole round trip.
        """
        with Session(engine) as session:
            return helpers.settle_transaction(session, transaction_id, fraud_flag)

    async def _process_message(self, msg):
        enriched = json.loads(msg.value.decode("utf-8"))
        print("enriched: ",enriched)
        record = [{"transaction_type": enriched["transaction_type"],
                  "amount": enriched["amount"],
                  "login_attempts_last_30min": enriched["login_attempts_last_30min"]}]
        features = featurize(pd.DataFrame(record))
        fraud_flag = int(self.model.predict(features)[0])
        print("result: ", fraud_flag)

        transaction_id = enriched["transaction_id"]

        # Settlement lives here rather than in the POST handler. The handler
        # only waits a couple of seconds before handing the client a
        # transaction id to poll, so if it has already given up, nothing else
        # would ever post the ledger — the transaction would sit PENDING
        # forever and the client would poll forever.
        try:
            settlement = await asyncio.to_thread(
                self._settle, transaction_id, fraud_flag
            )
        except Exception as exc:
            # Swallowed on purpose: one unsettleable transaction shouldn't tear
            # down the consumer and stall every transaction behind it. The row
            # stays PENDING, so nothing is half-written — but auto-commit means
            # the offset moves on regardless, so this message will not be
            # redelivered and that transaction needs operator attention.
            print(f"SETTLEMENT FAILED for transaction {transaction_id}: {exc!r}")
            return

        if settlement is None:
            # Already settled, which is the expected case for a repeat. Kafka
            # delivery is at-least-once and the Flink upsert sink re-emits a
            # row every time a transaction's login aggregate changes, so the
            # same id arrives here several times. The first delivery wins.
            print(f"Transaction {transaction_id} already settled; skipping.")
            return

        print(f"Transaction {transaction_id} settled as {settlement.status}")

        # The cached idempotent response is written before the signal, so a
        # client that retries the POST the instant it sees the result gets the
        # settled answer rather than a stale 409 "currently processing".
        status_code, body = helpers.transaction_response(
            settlement.status, settlement.failure_reason, transaction_id=transaction_id
        )
        await idempotency_manager.cache_response(
            redis_key=idempotency_manager.redis_key(settlement.idempotency_key),
            status=settlement.status.upper(),
            status_code=status_code,
            response_body=body,
        )

        # SET then PUBLISH, both strictly after the commit. A waiting request
        # re-reads the transaction row when it wakes, so the row must already
        # be settled before anyone can be woken. The key is the fallback for a
        # request that starts waiting after the publish has gone out, since
        # pub/sub has no replay.
        key = f"fraud_result:{transaction_id}"
        await redis_client.set(key, json.dumps(fraud_flag), ex=300)
        await redis_client.publish(key, json.dumps(fraud_flag))

class CDC_Ledger_Consumer(CustomConsumer):
    async def _process_message(self, msg):
        pass

class S3_Sink_Consumer(CustomConsumer):
    async def _process_message(self, msg):
        pass
        
# async def consume_ledger_cdc():
#     # 1. Initialize Consumer to read from the Debezium-generated topic
#     consumer = AIOKafkaConsumer(
#         "transactions.public.transactions",  # Debezium topic format: <topic.prefix>.<schema>.<table>
#         bootstrap_servers=BOOTSTRAP_SERVERS,
#         group_id="analytics_ingestion_group",  # Consumer Group ID
#         auto_offset_reset="earliest",          # Read from beginning if new group
#         enable_auto_commit=True
#     )

#     await consumer.start()
#     try:
#         print("Listening for CDC events from PostgreSQL...")
        
#         async for msg in consumer:
#             event = json.loads(msg.value.decode("utf-8"))
            
#             # Debezium CDC payload structure
#             payload = event.get("payload", {})
#             print(payload)
            
#     finally:
#         await consumer.stop()
#         print("Stopped consumer.")


# async def consume_enriched_transactions():
#     # Reads the Flink job's output topic (flink/create_enriched_stream.py).
#     # Unlike consume_ledger_cdc above, this is NOT a Debezium-wrapped payload —
#     # the Flink sink table writes plain JSON rows ('format' = 'json'), so the
#     # message body is already {"transaction_id": ..., "user_id": ..., "login_attempts": ...}
#     # with no "payload" envelope to unwrap.
    
#     model = xgb.XGBClassifier()
#     model.load_model("model.json")
    
#     consumer = AIOKafkaConsumer(
#         "transaction_login_attempts",
#         bootstrap_servers=BOOTSTRAP_SERVERS,
#         group_id="fraud_scoring_group",
#         auto_offset_reset="earliest",
#         enable_auto_commit=True
#     )

#     await consumer.start()
#     try:
#         print("Listening for enriched transactions from Flink...")

#         async for msg in consumer:
#             enriched = json.loads(msg.value.decode("utf-8"))
#             print(enriched)
#             # TODO: featurize(enriched) + model.predict(...), then write the
#             # fraud outcome back onto transactions.status

#     finally:
#         await consumer.stop()
#         print("Stopped consumer.")


async def main():
    model = xgb.XGBClassifier()
    model.load_model("ml/models/fraud_model.json")
    fraud_consumer = FraudDetectionConsumer("transaction_login_attempts", group_id="fraud_scoring_group", model=model)
    
    ledger_consumer = CDC_Ledger_Consumer("transactions.public.transactions", group_id="analytics_ingestion_group")
    
    await asyncio.gather(
        fraud_consumer._consume_messages(),
        ledger_consumer._consume_messages()
    )


if __name__ == "__main__":
    asyncio.run(main())