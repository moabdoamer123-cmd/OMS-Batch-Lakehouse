from config import get_spark_session, S3_PATHS
from pyspark.sql.functions import sum, desc

def run_validation():
    spark = get_spark_session("OMS_Validation_Test")
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "="*50)
    print("🧪 RUNNING GOLD LAYER VALIDATION TESTS")
    print("="*50)

    try:
        # 1️⃣ Test: Daily Revenue Mart
        print("\n📊 Checking 'Daily Revenue Mart'...")
        df_revenue = spark.read.parquet(f"{S3_PATHS['gold']}/daily_revenue_mart")
        
        total_rev = df_revenue.select(sum("daily_revenue")).collect()[0][0]
        total_ord = df_revenue.select(sum("total_orders")).collect()[0][0]
        
        print(f"✅ Total Revenue: ${total_rev:,.2f}")
        print(f"✅ Total Orders: {total_ord}")

        # 2️⃣ Test: Top Customers (CLV)
        print("\n🏆 Checking 'Customer CLV Mart' (Top 5)...")
        df_clv = spark.read.parquet(f"{S3_PATHS['gold']}/customer_clv_mart")
        
        # التعديل هنا: استخدام lifetime_spend بدلاً من total_spent
        df_clv.orderBy(desc("lifetime_spend")).show(5)

        # 3️⃣ Test: Data Partitioning
        print("\n📂 Checking S3 Partitioning Structure...")
        df_revenue.select("order_year", "order_month").distinct().orderBy("order_year", "order_month").show()

        print("="*50)
        print("✨ VALIDATION COMPLETED SUCCESSFULLY!")
        print("="*50)

    except Exception as e:
        print(f"❌ Validation Failed: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_validation()
    