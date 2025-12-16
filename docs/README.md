# Insider Trading Analysis — Documentation

This doc explains **what the project is**, **what it does**, and **how to run it** (setup + examples + DB access).

---

## Table of Contents

- [Overview](#overview)
- [What This Project Does](#what-this-project-does)
- [What Problem It Solves](#what-problem-it-solves)
- [Quick Setup](#quick-setup)
- [Environment Variables](#environment-variables)
- [Database Access](#database-access)
- [Design](#design)

---

## Overview

This project builds a **local, queryable dataset of SEC insider trading filings** (Forms 3, 4, and 5), enriches them with market metadata, and provides tooling for **analysis and visualization** of insider behavior.

The goal is **not prediction or trade automation**, but **data engineering + exploratory analysis** of legally disclosed insider transactions.

---

## What This Project Does

At a high level, the project:

1. Fetches insider trading filings from the SEC via an external API
2. Normalizes and cleans raw filing data
3. Enriches transactions with issuer metadata (exchange, sector, industry)
4. Stores data in a PostgreSQL database
5. Aggregates and visualizes insider activity (buy/sell value, reporters, sectors, time)

---

## What Problem It Solves

SEC insider filings are:

- Fragmented across millions of XML documents
- Poorly normalized
- Difficult to analyze at scale without preprocessing

This project turns raw filings into:

- Structured tables
- Clean time-series data
- Aggregated dollar-value metrics
- Analyst-friendly visual outputs


## Quick Setup

### 1) Create and activate a virtual environment

```bash
python -m venv env
source env/bin/activate
```

### 2) Install dependencies

```bash
pip install -r requirements.txt
```

### 3) Install in editable mode (enables `insider_cli`)

```bash
pip install -e .
```

### 4) Start PostgreSQL

```bash
docker compose up -d
make db-init
```

### 5) load sample raw dataset

```bash
unzip samples/raw_dataset.zip -d data
```

### 6) Build dataset

```bash
insider_cli build-dataset --raw-path .
```

---

## Environment Variables

`.env`:

```env
SEC_API_KEY=API_KEY

DB_HOST=localhost
DB_PORT=5432
DB_USER=insider
DB_PASSWORD=insider
DB_NAME=insider_trading_etl

RATE_LIMIT=30
RATE_PERIOD=60
```

---

## Database Access

Enter the Postgres container:

```bash
docker exec -it insider_db bash
```

Open `psql`:

```bash
psql -U insider -d insider_trading
```

Helpful commands:

```sql
\d                     -- list tables
\d insider_transactions -- view schema
```

---

## Design

[DESIGN](design.txt)
