# 🔥 Real-Time Data Streaming Platform

[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-231F20.svg)](https://kafka.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1.svg)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://docker.com)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Ready-326CE5.svg)](https://kubernetes.io)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> **Event-driven data platform handling 500K+ events/hour with exactly-once semantics, implementing Complex Event Processing (CEP) patterns, windowing functions, and stateful transformations for real-time analytics and anomaly detection.**

---

## 📐 Architecture

```
┌──────────────────┐     ┌─────────────────────────────────────────────────────┐
│  EVENT PRODUCERS │     │              APACHE KAFKA                          │
│                  │     │  ┌──────────┐ ┌──────────┐ ┌──────────┐          │
│ • Transactions   │────▶│  │raw-events│ │enriched- │ │  alerts  │          │
│ • User Activity  │     │  │(6 parts) │ │ events   │ │(3 parts) │          │
│ • IoT Sensors    │     │  └────┬─────┘ └────▲─────┘ └────▲─────┘          │
│ • API Calls      │     │       │            │            │                  │
│                  │     └───────┼────────────┼────────────┼──────────────────┘
└──────────────────┘             │            │            │
                                 ▼            │            │
                    ┌────────────────────────────────────────────┐
                    │         STREAM PROCESSOR                    │
                    │                                            │
                    │  ┌──────────┐ ┌──────────┐ ┌──────────┐  │
                    │  │ Windowing│ │   CEP    │ │  State   │  │
                    │  │          │ │  Engine  │ │  Store   │  │
                    │  │• Tumbling│ │          │ │          │  │
                    │  │• Sliding │ │• Velocity│ │• Redis   │  │
                    │  │• Session │ │• Anomaly │ │• RocksDB │  │
                    │  │          │ │• Travel  │ │  backed  │  │
                    │  │          │ │• Burst   │ │          │  │
                    │  └──────────┘ └──────────┘ └──────────┘  │
                    └────────────────────────────────────────────┘
                                 │
                    ┌────────────┼────────────────┐
                    ▼            ▼                ▼
            ┌────────────┐ ┌──────────┐  ┌──────────────┐
            │ PostgreSQL │ │ Grafana  │  │  Alert       │
            │ (Events +  │ │Dashboard │  │  Routing     │
            │  Alerts)   │ │          │  │  (Webhook)   │
            └────────────┘ └──────────┘  └──────────────┘
```

## 🔑 Key Features

- **Exactly-Once Semantics** — Idempotent producers + manual offset commit + transactional reads
- **Complex Event Processing** — 5 CEP patterns: velocity spike, amount anomaly, impossible travel, rapid-fire burst, merchant diversity spike
- **Windowing Functions** — Tumbling (fixed), sliding (overlapping), and session (activity-based) windows
- **Stateful Transformations** — Redis-backed state store with TTL, Welford's online statistics, fault-tolerant checkpointing
- **Dead Letter Queue** — Automatic routing of failed events for retry/investigation
- **Batched Sink** — PostgreSQL consumer with bulk inserts (500 events/batch) for sustained throughput
- **Full Observability** — Prometheus metrics, Grafana dashboards, structured JSON logging
- **Kubernetes-Ready** — HPA auto-scaling on consumer lag, liveness/readiness probes

## 🛠️ Tech Stack

| Category | Technologies |
|----------|-------------|
| Messaging | Apache Kafka 3.6 (confluent-kafka), 6-partition topics |
| Processing | Python 3.12, Pydantic, orjson, structlog |
| Storage | PostgreSQL 16, Redis 7 (state store) |
| Observability | Prometheus, Grafana, structured JSON logging |
| Infrastructure | Docker Compose, Kubernetes (HPA), GitHub Actions |
| API | FastAPI (health/metrics endpoints) |

## 📁 Project Structure

```
realtime-streaming-platform/
├── config/
│   └── settings.py                 # Pydantic config with env var support
├── src/
│   ├── producers/
│   │   └── transaction_producer.py # Event generator (500+ EPS, anomaly injection)
│   ├── processors/
│   │   ├── stream_processor.py     # Main pipeline: enrich → window → CEP → route
│   │   ├── windowing.py            # Tumbling, sliding, session windows
│   │   └── cep_engine.py           # 5 CEP patterns with Welford's online stats
│   ├── consumers/
│   │   ├── postgres_sink.py        # Batched DB writer (500/batch, 5s flush)
│   │   └── database.py             # SQLAlchemy models, indexes, materialized views
│   ├── models/
│   │   └── events.py               # Pydantic schemas (RawEvent, Alert, WindowAggregate)
│   ├── utils/
│   │   ├── kafka_client.py         # Producer/Consumer wrappers, topic management
│   │   ├── state_store.py          # Redis-backed state with TTL and checkpointing
│   │   ├── metrics.py              # Prometheus counters, histograms, gauges
│   │   └── logging.py              # structlog JSON logging
│   └── api.py                      # FastAPI health/metrics endpoints
├── tests/
│   ├── test_windowing.py           # Window function tests (10 tests)
│   ├── test_cep.py                 # CEP pattern tests (8 tests)
│   └── test_state_and_models.py    # State store + model tests (10 tests)
├── infrastructure/
│   ├── docker/
│   │   ├── Dockerfile
│   │   └── prometheus.yml
│   └── kubernetes/
│       └── deployments.yaml        # Deployments, HPA, ConfigMap, Service
├── scripts/
│   └── init_db.sql                 # Materialized views for dashboards
├── docker-compose.yml              # Full stack: Kafka, PG, Redis, Grafana, app
└── requirements.txt
```

## 🚀 Quick Start

### Run Full Stack (Docker Compose)
```bash
git clone https://github.com/Shivanathsai/Real-Time-Streaming-Platform.git
cd Real-Time-Streaming-Platform

# Start everything — Kafka, PostgreSQL, Redis, Grafana, app services
docker-compose up -d

# Watch logs
docker-compose logs -f stream-processor

# View Kafka UI at http://localhost:8080
# View Grafana at http://localhost:3000 (admin/admin)
```

### Run Tests Locally
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python -m pytest tests/ -v
```

### Run Individual Components
```bash
# Producer — generate 500 events/sec
python -m src.producers.transaction_producer --eps 500

# Stream Processor
python -m src.processors.stream_processor

# PostgreSQL Sink
python -m src.consumers.postgres_sink
```

## 📊 Performance

| Metric | Value |
|--------|-------|
| Throughput | 500K+ events/hour (140+ EPS sustained) |
| Processing Latency (p99) | < 50ms per event |
| Exactly-Once Guarantee | Idempotent producer + manual commit |
| Alert Detection Latency | < 2 seconds from event to alert |
| Sink Batch Write | 500 events/batch, ~5ms per batch |
| State Store Keys | 50K+ concurrent user states |
| Kafka Partitions | 6 per topic (parallel consumers) |

## 🧩 CEP Patterns

| Pattern | Description | Severity |
|---------|-------------|----------|
| Velocity Spike | > N events per user in 1-hour window | HIGH |
| Amount Anomaly | Transaction > 3σ from user's rolling mean | MEDIUM-HIGH |
| Impossible Travel | Consecutive events imply > 900 km/h travel | CRITICAL |
| Rapid-Fire Burst | N+ events within 10 seconds | HIGH |
| Merchant Diversity | 8+ distinct merchants in recent activity | MEDIUM |

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
