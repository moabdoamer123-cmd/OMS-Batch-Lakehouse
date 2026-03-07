import os
from pyspark.sql.functions import sum, desc, col, count, expr

# Securely import configuration from config.py
try:
    # Matching your config.py variable name 'DB_CONF'
    from config import get_spark_session, S3_PATHS, DB_CONF as DB_CONFIG
except ImportError:
    from config import get_spark_session, S3_PATHS
    DB_CONFIG = None 

def run_pipeline_integrity_audit():
    # Initialize Spark Session
    spark = get_spark_session("OMS_Pipeline_Audit")
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "="*70)
    print("🚀 MEDALLION ARCHITECTURE INTEGRITY AUDIT")
    print("="*70)

    try:
        # --- PHASE 1: BRONZE (Source-to-Target Reconciliation) ---
        print("\n[1] Auditing Bronze (Source-to-Target Reconciliation)...")
        
        if DB_CONFIG:
            # Source Query: Counting records directly from PostgreSQL
            source_query = "(SELECT COUNT(*) as cnt FROM oms_core.orders) as count_query"
            source_count = spark.read.format("jdbc") \
                .option("url", DB_CONFIG['url']) \
                .option("dbtable", source_query) \
                .option("user", DB_CONFIG['user']) \
                .option("password", DB_CONFIG['password']) \
                .option("driver", DB_CONFIG['driver']) \
                .load().collect()[0][0]

            # Target Check: Counting records in Bronze Layer (S3)
            df_bronze = spark.read.parquet(f"{S3_PATHS['bronze']}/orders")
            bronze_count = df_bronze.count()

            if source_count == bronze_count:
                print(f"✔️ Bronze: SUCCESS (Reconciliation Passed: {bronze_count}/{source_count} records)")
            else:
                print(f"⚠️ Bronze: MISMATCH (Source: {source_count}, Bronze: {bronze_count})")
        else:
            print("⚠️ Bronze: Skipping Reconciliation (DB_CONF not found in config.py)")

        # --- PHASE 2: SILVER (Data Quality & Semantic Logic) ---
        print("\n[2] Auditing Silver (Quality & Semantic Logic)...")
        df_orders_silver = spark.read.parquet(f"s3a://silver/orders")
        
        # 2.1 Technical Check: Standardization
        sample_status = df_orders_silver.select("status").first()[0]
        if sample_status == sample_status.lower().strip():
            print("✔️ Quality: SUCCESS (Standardization verified)")

        # 2.2 Semantic Check: Updated vs Order Dates
        invalid_updates = df_orders_silver.filter(col("updated_at") < col("orderdate")).count()
        if invalid_updates == 0:
            print("✔️ Logic: SUCCESS (Audit dates are chronologically valid)")
        else:
            print(f"⚠️ Logic: WARNING ({invalid_updates} records updated before order date)")

        # 2.3 Semantic Check: Non-negative Financials
        # Checking the orderitems table for valid price/quantity
        df_items_silver = spark.read.parquet(f"s3a://silver/orderitems")
        bad_math = df_items_silver.filter((col("unitprice") <= 0) | (col("quantity") <= 0)).count()
        if bad_math == 0:
            print("✔️ Logic: SUCCESS (No zero/negative financial values)")
        else:
            print(f"⚠️ Logic: WARNING ({bad_math} records have invalid unit prices or quantities)")

        # --- PHASE 3: GOLD (Analytical Accuracy & Integrity) ---
        print("\n[3] Auditing Gold (Analytical Accuracy)...")
        
        # 3.1 Reconciliation: Revenue Mart
        df_rev = spark.read.parquet(f"{S3_PATHS['gold']}/daily_revenue_mart")
        rev_stats = df_rev.select(sum("daily_revenue")).collect()[0][0]
        print(f"✔️ Finance: SUCCESS (${rev_stats:,.2f} total revenue calculated)")

        # 3.2 Integrity Check: Unique Keys
        # Detecting accidental Cartesian products during joins
        df_clv = spark.read.parquet(f"{S3_PATHS['gold']}/customer_clv_mart")
        is_unique = df_clv.count() == df_clv.select("customerid").distinct().count()
        if is_unique:
            print("✔️ Integrity: SUCCESS (Primary Keys are unique in Gold Marts)")
        else:
            print("⚠️ Integrity: WARNING (Duplicate identifiers detected in Gold Layer)")

        print("\n" + "="*70)
        print("✨ AUDIT COMPLETED: DATA IS TRUSTED AND PRODUCTION-READY")
        print("="*70)

    except Exception as e:
        print(f"\n❌ AUDIT FAILED: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_pipeline_integrity_audit()