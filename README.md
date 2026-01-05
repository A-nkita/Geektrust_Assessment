```md
# Data Analytics Platform – Geektrust Case Study

## Overview
This project implements an **end-to-end, analytics-ready data platform** that ingests user, onboarding, and transaction data from multiple sources, transforms it into a clean data warehouse, and exposes curated analytics for downstream consumption.

The design follows **industry-standard layered architecture** with a strong focus on clarity, scalability, and interview readiness.

---

## Tech Stack
- **Database:** PostgreSQL  
- **Language:** Python  
- **SQL:** PostgreSQL SQL (CTEs, window functions, aggregations)  
- **Orchestration:** Python scripts  
- **Containerization:** Docker & Docker Compose  

---

## Architecture

### 1. Source Layer
- CSV files simulating independent source databases:
  - `users`
  - `onboarding`
  - `transactions`

---

### 2. Staging Layer (`staging` schema)
**Purpose:** Raw ingestion

- Tables mirror source data exactly
- No transformations or joins
- Supports reprocessing and auditing

Example:
- `staging.users`
- `staging.onboarding`
- `staging.transactions`

---

### 3. Intermediate Layer (`intermediate` schema)
**Purpose:** Clean & standardized data

Transformations include:
- Deduplication (latest record per user)
- Data type normalization
- Joining users with onboarding
- Clean transaction timestamps and amounts
- User-level aggregations

Example:
- `intermediate.users_clean`
- `intermediate.transactions_clean`
- `intermediate.user_activity`

---

### 4. Analytics Layer (`analytics` schema)
**Purpose:** Business-ready metrics

Analytics produced:
- Daily KYC completions & WoW growth
- Top 5 users per day by transaction amount with contribution %
- Transaction statistics (avg, median, min, max)
- Comparison of transaction behavior for KYC completed vs pending users

These tables/views are **directly consumable by APIs**.

---

### 5. Orchestration
- Python scripts handle:
  - Ingestion into staging
  - Transformation into intermediate
  - Analytics refresh
- Logging enabled for traceability
- Easily schedulable (cron / Airflow-ready)

---

### 6. API Layer (Optional)
- APIs read only from `analytics` schema
- Ensures single source of truth
- Clean separation from raw data logic

---

## Why This Design
- Clear separation of concerns
- Debug-friendly and auditable
- Scales with more data sources
- Mirrors real-world data warehouse patterns

---

## How to Run
1. Start services using Docker
2. Run ingestion script
3. Run transformation scripts
4. Query analytics schema or expose via APIs

---

## Summary
Raw data is preserved, transformations are layered, analytics are fast, and consumers stay decoupled.  
Simple, scalable, and production-aligned.
```
