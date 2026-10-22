import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer

# defaults to the host-facing listener for running this locally;
# override with KAFKA_BOOTSTRAP_SERVERS=kafka:19092 when run inside a container
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

async def consume_ledger_cdc():
    # 1. Initialize Consumer to read from the Debezium-generated topic
    consumer = AIOKafkaConsumer(
        "transactions.public.transactions",  # Debezium topic format: <topic.prefix>.<schema>.<table>
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="analytics_ingestion_group",  # Consumer Group ID
        auto_offset_reset="earliest",          # Read from beginning if new group
        enable_auto_commit=True
    )

    await consumer.start()
    try:
        print("Listening for CDC events from PostgreSQL...")
        
        async for msg in consumer:
            event = json.loads(msg.value.decode("utf-8"))
            
            # Debezium CDC payload structure
            payload = event.get("payload", {})
            print(payload)
            
    finally:
        await consumer.stop()
        print("Stopped consumer.")
        
        
if __name__ == "__main__":
    asyncio.run(consume_ledger_cdc())