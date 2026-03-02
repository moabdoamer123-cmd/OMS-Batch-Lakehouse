from config import get_spark_session, S3_PATHS
from pyspark.sql.functions import col, lower, trim, current_timestamp, cast

def run_silver():
    spark = get_spark_session("OMS_Silver_Layer")
    spark.sparkContext.setLogLevel("ERROR")
    
    tables = ['customers', 'dates', 'employees', 'products', 'suppliers', 'stores', 'orderitems', 'orders']
    
    try:
        for table_name in tables:
            print(f"⚙️ [SILVER] Processing table: {table_name}...")
            
            input_path = f"{S3_PATHS['bronze']}/{table_name}"
            df = spark.read.parquet(input_path)
            
            df = df.dropDuplicates()
            
            for column in df.columns:
                if dict(df.dtypes)[column] == 'string':
                    df = df.withColumn(column, lower(trim(col(column))))
            
            df = df.withColumn("silver_ingestion_at", current_timestamp())
            
            output_path = f"s3a://silver/{table_name}"
            df.write.mode("overwrite").parquet(output_path)
            
            print(f"✅ [SILVER] Table '{table_name}' cleaned and saved to {output_path}")

        print("✨ [SILVER] All tables processed successfully!")

    except Exception as e:
        print(f"❌ [SILVER] Error: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver()