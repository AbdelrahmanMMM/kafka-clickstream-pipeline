# 🚀 Real-Time Clickstream Data Pipeline

This project implements a real-time data pipeline using Apache Kafka, Python, and PostgreSQL.

## 🧠 Architecture

Producer → Kafka → Consumer → PostgreSQL

## 🔧 Tech Stack

- Apache Kafka (KRaft mode)
- Python (confluent-kafka)
- PostgreSQL
- Docker Compose

## 📊 Features

- Simulates real-time clickstream data
- Kafka producer with idempotence and delivery guarantees
- Consumer group with manual offset management
- Batch processing for performance optimization
- Real-time aggregation (user activity & page views)
- PostgreSQL UPSERT for analytics

## ▶️ How to Run

### 1. Start Kafka & PostgreSQL

```bash
docker compose up -d
