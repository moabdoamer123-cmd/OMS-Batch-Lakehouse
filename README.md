# 🏗️ OMS Batch Lakehouse Pipeline

An end-to-end **Batch Data Lakehouse pipeline** built with Apache Spark and the **Medallion Architecture (Bronze → Silver → Gold)**, designed to extract operational data from PostgreSQL, process it through multiple quality layers, and produce analytics-ready data marts for business insights.

---

## 📌 Project Overview

This project simulates a real-world data engineering pipeline for an Order Management System (OMS). Raw transactional data is ingested, cleaned, optimized, and transformed into business-ready datasets — following the same architecture used in modern data platforms.

---

## 🏛️ Architecture

```
PostgreSQL (Source System)
          │
          ▼
     Spark JDBC
          │
          ▼
   🥉 Bronze Layer  →  Raw Data Ingestion (Parquet)
          │
          ▼
   🥈 Silver Layer  →  Cleaned & Standardized Data
          │
          ▼
   🥇 Gold Layer    →  Business Data Marts
          │
          ▼
  Business Analytics & Insights
```

---

## 🛠️ Technology Stack

| Tool | Purpose |
|------|---------|
| **Apache Spark / PySpark** | Distributed data processing |
| **PostgreSQL** | Source operational database |
| **MinIO** | S3-compatible object storage |
| **Parquet** | Columnar storage format |
| **Spark SQL** | Data mart creation and querying |

---

## 🔄 Pipeline Stages

### 🥉 Bronze Layer — Raw Data Ingestion

Extracts all tables from PostgreSQL using **Spark JDBC** and stores them as-is in Parquet format.

**Key Features:**
- Dynamically extracts all tables from source schema
- Preserves original data structure with no transformations
- Creates the foundation for all downstream processing

**Output Structure:**
```
s3a://bronze/
    ├── customers/
    ├── orders/
    ├── orderitems/
    ├── products/
    ├── stores/
    ├── employees/
    ├── suppliers/
    └── dates/
```

---

### 🔍 Data Exploration

Before any transformations, an exploration step analyzes Bronze datasets to understand data characteristics:
- Schema inspection
- Null value analysis
- Record count validation
- Data sampling

---

### 🥈 Silver Layer — Cleaning & Standardization

Transforms raw Bronze data into clean, reliable, and consistent datasets.

**Transformations Applied:**
- Removed duplicate records
- Standardized text fields (lowercase + trimming)
- Added ingestion timestamps
- Validated data types and structure

**Output Structure:**
```
s3a://silver/
    ├── customers/
    ├── orders/
    ├── orderitems/
    ├── products/
    └── stores/
```

---

### 🥇 Gold Layer — Business Data Marts

Creates analytics-ready datasets using **Spark SQL Views** for efficient joins and aggregations.

**Data Marts Created:**

| Mart | Description | Key Metrics |
|------|-------------|-------------|
| **Daily Revenue Mart** | Revenue insights at daily level | Daily revenue, Orders per day |
| **Customer Lifetime Value (CLV)** | Total value per customer | Lifetime spend, Total orders |
| **Operational Delivery Performance** | Order performance across stores | Orders per store, Status distribution |

---

## ⚡ Performance Optimization

Compared two Spark join strategies to improve query execution time:

### Before — SortMergeJoin (Default)
- Requires heavy data shuffling across nodes
- Slower execution for large datasets
- High network and memory usage

### After — BroadcastHashJoin
- Small tables broadcasted directly to worker nodes
- Eliminates expensive shuffle operations
- Significantly faster execution with lower resource usage

**Result:** Broadcast joins reduced query execution time and improved overall Spark performance across the pipeline.

---

## ✅ Data Quality & Pipeline Audit

A dedicated audit process validates data integrity across all three layers:

| Layer | Validation |
|-------|------------|
| **Bronze** | Source-to-target reconciliation — record counts match PostgreSQL |
| **Silver** | Standardization checks, timestamp validation, invalid financial value detection |
| **Gold** | Revenue calculation accuracy, unique customer identifiers, aggregation logic |

---

## 📁 Project Structure

```
OMS-Batch-Lakehouse/
│
├── config.py               # Spark & database configuration
├── bronze_pipeline.py      # Raw data ingestion from PostgreSQL
├── data_exploration.py     # Bronze layer exploration & profiling
├── silver_pipeline.py      # Data cleaning & standardization
├── gold_pipeline.py        # Business data mart creation
├── pipeline_audit.py       # Data quality validation across layers
│
├── jars/                   # JDBC and storage connector JARs
│
└── README.md
```

---

## 💡 Key Learning Outcomes

- Designing end-to-end batch data pipelines from scratch
- Implementing the Medallion Lakehouse Architecture (Bronze/Silver/Gold)
- Optimizing Spark performance using broadcast join strategies
- Building analytics-ready data marts with Spark SQL
- Validating data quality and pipeline integrity across all layers
- Working with S3-compatible object storage (MinIO) and Parquet format

---

## 🔮 Future Improvements

- Workflow orchestration using **Apache Airflow**
- Incremental data ingestion (CDC)
- Data versioning using **Delta Lake**
- Real-time streaming pipelines

---

## 👤 Author

**Mohamed Amer** — Computer Science Student | Data Engineering Learner | PySpark & Airflow

- 📧 mo.abdo.amer123@gmail.com
- 💼 [LinkedIn](https://www.linkedin.com/in/mohamed-amer-217342376)
- 🐙 [GitHub](https://github.com/moabdoamer123-cmd)
