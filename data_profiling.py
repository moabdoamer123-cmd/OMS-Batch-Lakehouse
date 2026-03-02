from config import get_spark_session, S3_PATHS
from pyspark.sql.functions import col, count, when

def explore_bronze_layer():
    spark = get_spark_session("Data_Exploration")
    spark.sparkContext.setLogLevel("ERROR")
    
    # لستة الجداول اللي إنت سحبتها فعلياً
    tables = ['customers', 'dates', 'employees', 'products', 'suppliers', 'stores', 'orderitems', 'orders']
    
    for table in tables:
        print(f"\n" + "="*50)
        print(f"🔍 EXPLORING TABLE: {table.upper()}")
        print("="*50)
        
        # قراءة الجدول من الـ Bronze (MinIO)
        # ملحوظة: تأكد من المسار حسب الـ Log اللي فات s3a://bronze/orders//table
        path = f"s3a://bronze/orders//{table}" 
        df = spark.read.parquet(path)
        
        # 1. طباعة الهيكل (Schema)
        print("\n📋 1. Schema:")
        df.printSchema()
        
        # 2. إحصاء الـ Nulls في كل عمود
        print("❓ 2. Null Values Count:")
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        null_counts.show()
        
        # 3. عدد الصفوف (Total Rows)
        total_rows = df.count()
        print(f"📊 3. Total Records: {total_rows}")
        
        # 4. عينة من البيانات (Sample)
        print("👀 4. Data Sample (First 5 rows):")
        df.show(5, truncate=False)
        
    spark.stop()

if __name__ == "__main__":
    explore_bronze_layer()
    