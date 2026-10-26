# Banking App Architecture 

The primary aim of this project is to understand and implement the kind of data architecture behind modern applications dealing with large volumes of data and transactions such as banking, ecommerce and delivery apps. A secondary aim is to learn how to leverage AI in data engineering workflows. 

## Architecture

As such, I have built a simple architecture outlined in the diagram below.

<img width="702" height="642" alt="architecture drawio" src="https://github.com/user-attachments/assets/f8baf9b2-9a32-4d01-ab64-d70b53c71450" />

- **FastAPI app:** An API with endpoints for user registration, login, and initiating banking transactions (deposits, withdrawals, and transfers). A Locust file will be used to send simulated traffic to this app to test the latency and load handling ability of the architecture. JWT tokens and OAuth2 authorisation are implemented and used to protect the endpoints.
- **PostgreSQL:** Functions as the transactional database for the app.
- **Redis cache:** Used to implement idempotency in transactions. When a transaction is initiated, the app first checks Redis cache for a transaction with the same idempotency key and payload. If the transaction does not exist in cache, it is inserted into both cache and Postgres database with the status pending. If a transactio with the same payload and idempotency key arrives, it will not be processed. Once the transaction has been processed, the status is returned to the app, the completed transaction is updated in the cache and stored for 24 hours for quick retrieval.
- **Apache Kafka + Debezium connector:** This allows transaction processing to not slow down the app. Postgres' wirte-ahead-log (WAL) is read by Debezium and streamed to Kafka consumers as an event stream. The Kafka consumers then use the fraud detection ML model to classify the transaction as fraud/not fraud (fraudulent transactions are cancelled), processed by Apache Flink for real-time analytics, and store the transactions in minio S3 storage.
- **Apache Flink:** Used to keep daily metric aggregates for real-time analytics. Also used to aggregate succesful/failed login attempts by user. This is a feature required by the fraud detection model.
- **S3 bucket:** For cold storage of data and to allow the batch pipeline run to not slow down the transactional database.
- **Airflow:** For orchestrating batch pipelines that load data from S3 bucket into the analytical warehouse.
- **dbt:** Used for data modelling and creating the schema for the analytical warehouse.

## Deployment
- Docker:
- Terraform:
- Github Actions:
- Cloud hosting
  
## Best practices
1. Typed schemas using SQLModel and Alembic for data migrations to allow tracking schema changes.
2. Idempotent transactions through Redis caching layer.
3. 
