from config import get_spark_session, S3_PATHS
from pyspark.sql.functions import col, count, when

def explore_bronze_layer():
    spark = get_spark_session("Data_Exploration")
    spark.sparkContext.setLogLevel("ERROR")
    
    tables = ['customers', 'dates', 'employees', 'products', 'suppliers', 'stores', 'orderitems', 'orders']
    
    for table in tables:
        print(f"\n" + "="*50)
        print(f"🔍 EXPLORING TABLE: {table.upper()}")
        print("="*50)
        
        path = f"s3a://bronze/orders//{table}" 
        df = spark.read.parquet(path)
        
        print("\n📋 1. Schema:")
        df.printSchema()
        
        print("❓ 2. Null Values Count:")
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        null_counts.show()
        
        total_rows = df.count()
        print(f"📊 3. Total Records: {total_rows}")
        
        print("👀 4. Data Sample (First 5 rows):")
        df.show(5, truncate=False)
        
    spark.stop()

if __name__ == "__main__":
    explore_bronze_layer()
    