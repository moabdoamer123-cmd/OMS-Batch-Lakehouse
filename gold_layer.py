import time
from config import get_spark_session, S3_PATHS
from pyspark.sql.functions import year, month

def run_gold():
    # 1. Start Spark Session
    spark = get_spark_session("OMS_Gold_Layer_Final")
    spark.sparkContext.setLogLevel("ERROR")
    
    # ===============================
    # 2️⃣ Load Silver Tables and Create Views 
    # ===============================
    print("📦 [GOLD] Loading Silver tables into Spark SQL Views...")
    
    
    tables = ['customers', 'orders', 'orderitems', 'products', 'stores']
    
    for table in tables:
        
        path = f"s3a://silver/{table}" 
        
        try:
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(f"view_{table}")
            print(f"✅ View created: view_{table} from {path}")
        except Exception as e:
            print(f"❌ Error loading {table}: Make sure it exists in s3a://silver/{table}")
            continue

    # =========================================================
    # 3. PERFORMANCE TEST: BEFORE VS AFTER OPTIMIZATION
    # =========================================================
    
    # --- [BEFORE] Disable Broadcast to force SortMergeJoin ---
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    
    print("\n" + "="*60)
    print("🧐 TEST 1: BEFORE OPTIMIZATION (SortMergeJoin)")
    print("="*60)
    
    start_raw = time.time()
    df_before = spark.sql("""
        SELECT o.orderid, i.unitprice, o.orderdate
        FROM view_orders o
        JOIN view_orderitems i ON o.orderid = i.orderid
    """)
    df_before.explain(extended=False)
    raw_count = df_before.count() # Action to trigger execution
    duration_raw = time.time() - start_raw
    print(f"⏱️ Execution Time (Before): {duration_raw:.4f} seconds")

    # --- [AFTER] Enable Broadcast Join Optimization ---
    # We use both the threshold and the SQL Hint for maximum impact
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10*1024*1024) 

    print("\n" + "="*60)
    print("🚀 TEST 2: AFTER OPTIMIZATION (BroadcastHashJoin)")
    print("="*60)
    
    start_opt = time.time()
    df_after = spark.sql("""
        SELECT /*+ BROADCAST(o) */ 
            o.orderid, i.unitprice, o.orderdate
        FROM view_orders o
        JOIN view_orderitems i ON o.orderid = i.orderid
    """)
    df_after.explain(extended=False)
    opt_count = df_after.count() # Action to trigger execution
    duration_opt = time.time() - start_opt
    print(f"⏱️ Execution Time (After): {duration_opt:.4f} seconds")

    # Show Improvement
    improvement = ((duration_raw - duration_opt) / duration_raw) * 100
    print(f"\n📈 Performance Optimization Result: {improvement:.2f}% Improvement")

    # =========================================================
    # 4. CREATE ANALYTICAL MARTS (Requirement 2, 3, 4)
    # =========================================================
    print("\n🏗️ Building Final Gold Marts...")

    # --- Mart 1: Daily Revenue (Partitioned by Year/Month) ---   
    daily_revenue_mart = spark.sql("""
        SELECT
            o.orderdate,
            year(o.orderdate) as order_year,
            month(o.orderdate) as order_month,
            ROUND(SUM(i.quantity * i.unitprice), 2) as daily_revenue,
            COUNT(DISTINCT o.orderid) as total_orders
        FROM view_orders o
        JOIN view_orderitems i ON o.orderid = i.orderid
        GROUP BY o.orderdate
    """)
    daily_revenue_mart.write.mode("overwrite") \
        .partitionBy("order_year", "order_month") \
        .parquet(f"{S3_PATHS['gold']}/daily_revenue_mart")
    print("✅ Daily Revenue Mart saved (Partitioned).")

    # --- Mart 2: Customer Lifetime Value (CLV) ---
    clv_mart = spark.sql("""
        SELECT
            c.customerid, c.firstname, c.lastname,
            ROUND(SUM(i.quantity * i.unitprice), 2) as lifetime_spend,
            COUNT(DISTINCT o.orderid) as total_orders_count
        FROM view_customers c
        JOIN view_orders o ON c.customerid = o.customerid
        JOIN view_orderitems i ON o.orderid = i.orderid
        GROUP BY c.customerid, c.firstname, c.lastname
    """)
    clv_mart.write.mode("overwrite").parquet(f"{S3_PATHS['gold']}/customer_clv_mart")
    print("✅ Customer CLV Mart saved.")

    # --- Mart 3: Operational Delivery Performance ---
    delivery_mart = spark.sql("""
        SELECT
            s.storename, o.status,
            COUNT(o.orderid) as order_count
        FROM view_orders o
        JOIN view_stores s ON o.storeid = s.storeid
        GROUP BY s.storename, o.status
    """)
    delivery_mart.write.mode("overwrite").parquet(f"{S3_PATHS['gold']}/operational_delivery_mart")
    print("✅ Operational Delivery Mart saved.")

    print("\n✨ [GOLD] All requirements completed successfully!")
    spark.stop()

if __name__ == "__main__":
    run_gold()