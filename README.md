# 🚀 OMS Batch Lakehouse Pipeline (PySpark & MinIO)

This project implements a high-performance **Medallion Architecture** using **PySpark** to process Order Management System (OMS) data. It transitions data from a PostgreSQL Source to an S3-Compatible Data Lake (MinIO), focusing on scalability, performance tuning, and data integrity.

## 🏗️ Architecture Overview
The pipeline follows the **Multi-Hop (Medallion)** pattern, ensuring data evolves from raw state to business-ready insights:

1.  **Bronze Layer (Raw):** - **Dynamic Ingestion:** Automatically discovers and extracts all tables from the `oms_core` schema using JDBC metadata.
    - **Storage:** Persists raw data in **Parquet** format for efficient storage and schema evolution.

2.  **Silver Layer (Cleaned):**
    - **Data Standardization:** Implements schema enforcement, deduplication, and string normalization (trimming/casing).
    - **Metadata:** Adds ingestion timestamps for auditability.

3.  **Gold Layer (Aggregated & Optimized):**
    - **Performance Tuning:** Implemented **Broadcast Hash Joins**, achieving a **~92% reduction** in execution time by optimizing Dimension-Fact joins.
    - **Analytics Marts:** Generates specialized marts like `Daily Revenue`, `Customer CLV`, and `Store Performance` using Spark SQL.
    - **Partitioning:** Data is partitioned by `order_year` and `order_month` to optimize downstream query performance.



## ⚡ Performance Highlights
- **Optimization:** Strategic use of `broadcast()` hints for small lookup tables to eliminate expensive Shuffles.
- **Config-Driven:** Fully modular architecture using a centralized `config.py` for environment-agnostic deployment.
- **Storage Efficiency:** Leverages Parquet's columnar storage and Snappy compression.

## 🛠️ Tech Stack
- **Processing Engine:** Apache Spark (PySpark)
- **Data Lake:** MinIO (S3-Compatible Object Storage)
- **Source Database:** PostgreSQL
- **Orchestration:** Modular Python Execution Framework
- **Infrastructure:** Docker & Docker Compose

## 🚀 How to Run

1. **Spin up the Infrastructure:**
   ```bash
   docker-compose up -d