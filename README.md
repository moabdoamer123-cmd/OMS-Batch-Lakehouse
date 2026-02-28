OMS Batch Lakehouse Pipeline
This project implements a Medallion Architecture using PySpark to process Order Management System (OMS) data from a Postgres Source to a MinIO S3-Compatible Data Lake.

🏗️ Architecture Overview
The pipeline follows the ETL (Extract, Transform, Load) pattern across three distinct layers:

Bronze Layer (Raw): Ingests Raw Data from the Operational Database (RDBMS) into Parquet format without any modifications.

Silver Layer (Cleaned): Performs Data Cleansing, Deduplication, and Schema Enforcement. Data is Partitioned by status to optimize Downstream Queries.

Gold Layer (Aggregated): Calculates Business KPIs and Aggregations (e.g., Order Distribution and VIP Customers) for BI Reporting.

🛠️ Tech Stack
Engine: Apache Spark (PySpark)

Storage: MinIO (Object Storage / Data Lake)

Source: PostgreSQL

Orchestration: Custom Python Script (main_pipeline.py)

Infrastructure: Docker & Docker Compose

🚀 How to Run
Spin up the Infrastructure:

docker-compose up -d

Trigger the Pipeline:

python main_pipeline.py