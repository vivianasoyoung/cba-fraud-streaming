# CBA Fraud Streaming Pipeline

A real-time fraud detection pipeline that processes banking transactions as they happen using Apache Kafka. Modelled on how Australian banks detect fraudulent activity within seconds of a transaction occurring.

## How this fits with the rest of the project

This is one of four repos that together form an end-to-end banking data platform:

| Repo | Stack | Role |
| --- | --- | --- |
| [`cba-banking-pipeline`](https://github.com/vivianasoyoung/cba-banking-pipeline) | Airflow, Postgres, Docker | Foundation: synthetic data generation + batch ingestion |
| [`cba-dbt-analytics`](https://github.com/vivianasoyoung/cba-dbt-analytics) | dbt-postgres, dbt_utils | Staging → intermediate → marts transformations |
| **[`cba-fraud-streaming`](https://github.com/vivianasoyoung/cba-fraud-streaming)** *(You are here)* | Kafka, Python, Postgres | Real-time rule-based fraud detection. Produces the **labels** consumed by `cba-feature-store`. |
| [`cba-feature-store`](https://github.com/vivianasoyoung/cba-feature-store) | Feast, MLflow, FastAPI | ML feature store + model serving |

The `fraud.flagged_transactions` table this repo populates is exported and used as **ML labels** in `cba-feature-store`. This decouples feature definition from label definition.

---

## Architecture

```
Transaction Producer (Python, containerised)
        │
        │  keyed by account_id (one partition per account)
        ▼
Kafka Topic: "transactions"
        │
        ▼
Fraud Detection Consumer (Python, containerised)
   ├── Schema validation via Pydantic
   ├── Rules engine (5 rules, scored)
   ├── Dead-letter queue for malformed messages
   ├── Manual offset commits after successful DB write
   └── Connection pool
        │
        ▼
PostgreSQL — fraud.flagged_transactions
   (UNIQUE on transaction_id, ON CONFLICT DO NOTHING)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Message broker | Apache Kafka |
| Orchestration | Docker + Docker Compose |
| Stream processing | Python (kafka-python, Pydantic) |
| Storage | PostgreSQL 15 |
| Monitoring | Kafka UI |

## Quick Start

### Prerequisites
- Docker Desktop

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env and set PG_PASSWORD
```

### 2. Start everything

```bash
docker compose up -d
```

This boots Kafka, Postgres, Kafka UI, the producer, and the consumer — all containerised.

Services:
| Service | URL |
|---|---|
| Kafka UI | http://localhost:8090 |
| PostgreSQL | localhost:5433 |

### 3. Watch fraud detection in real time

```bash
docker compose logs -f consumer
```

### 4. Query flagged transactions

```bash
docker compose exec postgres psql -U fraud -d fraud_detection \
  -c "SELECT account_id, amount, risk_score, fraud_reasons, event_time \
      FROM fraud.flagged_transactions \
      ORDER BY processed_at DESC LIMIT 10;"
```

## Fraud Detection Rules

| Rule | Condition | Risk Score |
|---|---|---|
| Large amount | Transaction > $9,000 | +50 |
| Elevated amount | Transaction > $5,000 | +20 |
| Overseas transaction | `merchant_state = OVS` | +30 |
| Late-night online | `ONLINE` channel + amount > $2,000 + hour < 6 or > 22 | +20 |
| High velocity | 5+ transactions in 60 seconds | +30 |

Maximum risk score is capped at 100. Rules are defined as data (`RULES = [...]`) — one place to read or edit them.

## Data Model

### fraud.flagged_transactions
| Column | Type | Description |
|---|---|---|
| transaction_id | VARCHAR | Unique transaction identifier (UNIQUE constraint) |
| account_id | VARCHAR | Account that made the transaction |
| amount | NUMERIC | Transaction amount (CHECK >= 0) |
| merchant_category | VARCHAR | Category of merchant |
| channel | VARCHAR | EFTPOS / ONLINE / ATM / BPAY |
| fraud_reasons | TEXT[] | Array of triggered rule names |
| risk_score | INTEGER | Composite risk score 0–100 |
| event_time | TIMESTAMPTZ | When the transaction occurred (from producer) |
| processed_at | TIMESTAMPTZ | When the consumer recorded the flag |

Indexed by `(account_id, event_time DESC)` and by `processed_at DESC`.

## Robustness

- **Idempotent inserts**: `UNIQUE(transaction_id)` + `ON CONFLICT DO NOTHING`. Replays are safe.
- **At-least-once delivery**: `enable_auto_commit=False`; offsets commit only after the DB write succeeds.
- **Dead-letter queue**: malformed messages route to `transactions.dlq` instead of crashing the loop.
- **Schema validation**: Pydantic model rejects messages with missing or invalid fields.
- **Connection pooling**: `psycopg2.pool.SimpleConnectionPool` instead of connect-per-write.

See [`REAL_WORLD_NOTES.md`](./REAL_WORLD_NOTES.md) for what would change at production scale (Redis-backed velocity tracking, schema registry, exactly-once, observability).

## Project Structure

```
cba-fraud-streaming/
├── producer/
│   └── transaction_producer.py   # Simulates live transaction feed
├── consumer/
│   └── fraud_consumer.py         # Real-time fraud detection engine
├── docker/
│   └── init.sql                  # PostgreSQL schema setup
├── Dockerfile.producer
├── Dockerfile.consumer
├── requirements.txt
├── docker-compose.yml            # Kafka + Postgres + Kafka UI + producer + consumer
├── .env.example
└── README.md
```
