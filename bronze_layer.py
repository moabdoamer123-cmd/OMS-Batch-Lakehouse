from config import get_spark_session, DB_CONF, S3_PATHS

def run_bronze_full_schema():
  
    spark = get_spark_session("OMS_Full_Bronze_Ingestion")
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        print("🔍 [BRONZE] Fetching list of all tables in 'oms_core' schema...")
        
        query = """
            (SELECT table_name 
             FROM information_schema.tables 
             WHERE table_schema = 'oms_core' 
             AND table_type = 'BASE TABLE') AS tables_list
        """
        
        tables_df = spark.read.jdbc(
            url=DB_CONF["url"], 
            table=query, 
            properties={
                "user": DB_CONF["user"], 
                "password": DB_CONF["password"], 
                "driver": DB_CONF["driver"],
                "sslmode": "require"
            }
        )
        
        all_tables = [row['table_name'] for row in tables_df.collect()]
        print(f"📦 [BRONZE] Found {len(all_tables)} tables: {all_tables}")

      
        for table_name in all_tables:
            full_table_path = f"oms_core.{table_name}"
            print(f"🚀 [BRONZE] Extracting {full_table_path}...")
               
            df = spark.read.jdbc(
                url=DB_CONF["url"], 
                table=full_table_path, 
                properties={
                    "user": DB_CONF["user"], 
                    "password": DB_CONF["password"], 
                    "driver": DB_CONF["driver"],
                    "sslmode": "require"
                }
            )
            
            target_path = f"{S3_PATHS['bronze']}/{table_name}"
            
            df.write.mode("overwrite").parquet(target_path)
            print(f"✅ [BRONZE] Table '{table_name}' saved to {target_path} ({df.count()} records)")

        print("✨ [BRONZE] Full Schema Ingestion Done!")

    except Exception as e:
        print(f"❌ [BRONZE] Error occurred: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_bronze_full_schema()