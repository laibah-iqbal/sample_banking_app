│   └── utils/               # config, JWT/OAuth2, password hashing, idempotency manager
├── init-postgres/           # creates the Debezium replication user and publication
├── kafka/
│   ├── connect/             # Debezium connector config and registration script
│   └── consumers.py         # CDC event consumer
├── flink/                   # Flink jobs (in progress)
├── ml/
│   ├── generate_dataset.py  # synthetic fraud dataset
│   ├── train_fraud_model.py # XGBoost training
│   └── models/              # saved model and feature schema
├── seed_data.py             # fake users and accounts with Faker
├── docker-compose.yml
└── Dockerfile
```

## Security

- Passwords are hashed with **PBKDF2-SHA256 (600,000 rounds)** and never stored in plain text.
- Endpoints are protected with **OAuth2 and JWT** access tokens that expire.
- Every login attempt is logged with its outcome, which also feeds the fraud model.
- Secrets live in a `.env` file that is gitignored, and Debezium reads its credentials from environment variables rather than the connector config.
- Debezium uses a **least-privilege replication user** instead of the database owner.
- The app container runs as a **non-root user**.

## Testing, observability and load testing

These are planned:

- **Tests:** unit tests for the idempotency manager and helpers, and integration tests that run a transaction through the whole pipeline.
- **Observability:** pipeline and consumer metrics with Grafana / OpenTelemetry.
- **Load testing:** a Locust file sending simulated traffic to measure latency and throughput. I will add the results here.

## Using AI in the pipeline

This is the secondary aim of the project and is planned for once the core pipeline is finished:

- **Natural-language questions over metrics:** ask questions in plain English and have them answered from the gold tables in the analytical warehouse.
- **Pipeline failure diagnosis:** an agent that reads pipeline errors, works out the likely cause and suggests a fix as a pull request.

## Deployment

- **Docker:** Every service runs in its own container, orchestrated locally with Docker Compose. Services start in dependency order using health checks.
- **Terraform:** Planned, to provision the cloud infrastructure as code.
- **GitHub Actions:** Planned, for running tests and deploying on every push.
- **Cloud hosting:** Planned, on AWS.

## Things I learned along the way

- **Kafka listeners in Docker.** Containers and the host machine need different listeners. Other containers reach Kafka at `kafka:19092`, while scripts on my laptop use `localhost:9092`. Mixing these up is why Kafka can work from one place and not another, so I kept them separate on purpose.
- **Waiting for Kafka Connect properly.** Connect's port opens before its REST API is ready and returns a `404` in the meantime, which `curl`'s own retry treats as success. The registration script retries until it gets a real `201` (or `409` if the connector already exists).
- **Docker volumes vs bind mounts.** A named volume persists data, but on Docker's Linux VM where I can't easily get to it. Bind-mounting a local folder keeps the Postgres data on my own filesystem.
- **Installing PyFlink.** Its dependency chain still expects `pkg_resources`, which recent `setuptools` versions removed, so `setuptools` has to be pinned below 81 and installed with `--no-build-isolation`.
