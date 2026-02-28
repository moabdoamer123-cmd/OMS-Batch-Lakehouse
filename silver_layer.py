from config import get_spark_session, S3_PATHS
from pyspark.sql.functions import col, current_timestamp, trim

def run_silver():
    spark = get_spark_session("OMS_Silver_Layer")
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        print("\n🧹 [SILVER] Cleaning & Partitioning data...")
        df_bronze = spark.read.parquet(S3_PATHS["bronze"])
        
        df_silver = df_bronze.select(
            col("orderid").cast("int"),
            trim(col("status")).alias("status"),
            col("customerid").cast("int"),
            col("orderdate"),
            current_timestamp().alias("ingestion_timestamp")
        ).dropDuplicates(["orderid"]).filter(col("orderid").isNotNull())

        print(f"✅ [SILVER] Transformation complete. Writing to Partitioned Parquet...")
        df_silver.write.mode("overwrite").partitionBy("status").parquet(S3_PATHS["silver"])
        print("✨ [SILVER] Done!")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver()