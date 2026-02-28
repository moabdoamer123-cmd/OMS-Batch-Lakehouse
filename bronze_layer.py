from config import get_spark_session, DB_CONF, S3_PATHS

def run_bronze():
    spark = get_spark_session("OMS_Bronze_Layer")
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        print("🚀 [BRONZE] Extracting from Postgres...")
        df = spark.read.jdbc(url=DB_CONF["url"], table="oms_core.orders", 
                             properties={"user": DB_CONF["user"], "password": DB_CONF["password"], 
                                         "driver": DB_CONF["driver"], "sslmode": "require"})
        
        print(f"✅ [BRONZE] Read {df.count()} records. Loading to MinIO...")
        df.coalesce(1).write.mode("overwrite").parquet(S3_PATHS["bronze"])
        print("✨ [BRONZE] Done!")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_bronze()