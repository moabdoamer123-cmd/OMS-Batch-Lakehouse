from config import get_spark_session, S3_PATHS
from pyspark.sql.functions import count, desc

def run_gold():
    spark = get_spark_session("OMS_Gold_Layer")
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        print("\n📊 [GOLD] Calculating Business KPIs...")
        df_silver = spark.read.parquet(S3_PATHS["silver"])

        # Report 1
        status_rep = df_silver.groupBy("status").count().orderBy(desc("count"))
        status_rep.show()
        status_rep.write.mode("overwrite").parquet(S3_PATHS["gold_status"])

        # Report 2
        vip_rep = df_silver.groupBy("customerid").count().orderBy(desc("count")).limit(5)
        vip_rep.show()
        vip_rep.write.mode("overwrite").parquet(S3_PATHS["gold_vip"])
        
        print("✨ [GOLD] Reports saved successfully!")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold()