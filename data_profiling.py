import os
from pyspark.sql.functions import col, count, when
try:
    from config import get_spark_session, S3_PATHS
except ImportError:
    print("❌ Error: config.py not found.")
    exit(1)

def explore_bronze_layer():
    # Initialize Spark Session
    spark = get_spark_session("Data_Exploration")
    spark.sparkContext.setLogLevel("ERROR")
    
    # List of tables to profile
    tables = ['customers', 'dates', 'employees', 'products', 'suppliers', 'stores', 'orderitems', 'orders']
    
    for table in tables:
        print(f"\n" + "="*50)
        print(f"🔍 EXPLORING TABLE: {table.upper()}")
        print("="*50)
        
        # CORRECTED PATH: Using the base bronze path from config
        # Usually, tables are saved as s3a://bronze/tablename
        path = f"{S3_PATHS['bronze']}/{table}" 
        
        try:
            df = spark.read.parquet(path)
            
            print("\n📋 1. Schema:")
            df.printSchema()
            
            print("❓ 2. Null Values Count:")
            # Dynamic null check for all columns
            null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
            null_counts.show()
            
            total_rows = df.count()
            print(f"📊 3. Total Records: {total_rows}")
            
            print("👀 4. Data Sample (First 5 rows):")
            df.show(5, truncate=False)
            
        except Exception as e:
            print(f"❌ Could not read table {table}: {str(e)}")
            
    # Properly stop the session
    spark.stop()

if __name__ == "__main__":
    explore_bronze_layer()