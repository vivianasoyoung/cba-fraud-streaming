# CBA Fraud Streaming Pipeline

A real-time fraud detection pipeline that processes banking transactions as they happen using Apache Kafka. Simulates a production-grade streaming system modelled on how Australian banks detect fraudulent activity within seconds of a transaction occurring.

## Architecture

```
Transaction Producer (Python)
        │
        ▼
Kafka Topic: "transactions"
        │
        ▼
Fraud Detection Consumer (Python)
   ├── Rule 1: Large amount > $9,000
   ├── Rule 2: 5+ transactions in 60 seconds (velocity)
   ├── Rule 3: Overseas transaction detected
   └── Rule 4: Large online transaction at night
        │
        ▼
PostgreSQL — fraud.flagged_transactions
```

## Tech Stack

| Layer | Tool |
|---|---|
| Message broker | Apache Kafka |
| Orchestration | Docker + Docker Compose |
| Stream processing | Python (kafka-python) |
| Storage | PostgreSQL 15 |
| Monitoring | Kafka UI |

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.10+

### 1. Start the stack

```bash
docker-compose up -d
```

Services:
| Service | URL |
|---|---|
| Kafka UI | http://localhost:8090 |
| PostgreSQL | localhost:5433 |

### 2. Install dependencies

```bash
pip install kafka-python psycopg2-binary
```

### 3. Run the consumer (Terminal 1)

```bash
python consumer/fraud_consumer.py
```

### 4. Run the producer (Terminal 2)

```bash
python producer/transaction_producer.py
```

Watch fraud being detected in real time in Terminal 1.

### 5. Query flagged transactions

```bash
docker exec -it cba-fraud-streaming-postgres-1 psql -U fraud -d fraud_detection \
  -c "SELECT account_id, amount, risk_score, fraud_reason FROM fraud.flagged_transactions ORDER BY flagged_at DESC LIMIT 10;"
```

## Fraud Detection Rules

| Rule | Condition | Risk Score |
|---|---|---|
| Large amount | Transaction > $9,000 | +50 |
| High velocity | 5+ transactions in 60 seconds | +30 |
| Overseas transaction | merchant_state = OVS | +30 |
| Late night online | ONLINE channel + amount > $2,000 + hour < 6 or > 22 | +20 |

Maximum risk score is capped at 100. Transactions with any triggered rule are flagged and stored in PostgreSQL.

## Data Model

### fraud.flagged_transactions
| Column | Type | Description |
|---|---|---|
| transaction_id | VARCHAR | Unique transaction identifier |
| account_id | VARCHAR | Account that made the transaction |
| amount | NUMERIC | Transaction amount |
| merchant_category | VARCHAR | Category of merchant |
| channel | VARCHAR | EFTPOS / ONLINE / ATM / BPAY |
| fraud_reason | TEXT | Pipe-separated list of triggered rules |
| risk_score | INTEGER | Composite risk score 0-100 |
| flagged_at | TIMESTAMP | When the transaction was flagged |

## Project Structure

```
cba-fraud-streaming/
├── producer/
│   └── transaction_producer.py   # Simulates live transaction feed
├── consumer/
│   └── fraud_consumer.py         # Real-time fraud detection engine
├── docker/
│   └── init.sql                  # PostgreSQL schema setup
├── docker-compose.yml            # Kafka + Postgres + Kafka UI
└── README.md
```
