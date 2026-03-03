import os

def setup_windows_env():
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

SPARK_JARS = "jars/postgresql-42.7.3.jar,jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-bundle-1.12.262.jar"
MINIO_CONF = {
    "endpoint": "http://127.0.0.1:9000",
    "access_key": "admin",
    "secret_key": "password123"
}

DB_CONF = {
    "url": "jdbc:postgresql://ep-solitary-recipe-ah9qonat-pooler.c-3.us-east-1.aws.neon.tech/neondb",
    "user": "neondb_owner",
    "password": "npg_nf0Bpoz8SKtF",
    "driver": "org.postgresql.Driver"
}

S3_PATHS = {
    "bronze": "s3a://bronze",
    "silver": "s3a://silver",
    "gold": "s3a://gold"   
}

def get_spark_session(app_name):
    setup_windows_env()
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", SPARK_JARS) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    try:
        spark._jvm.org.apache.hadoop.io.nativeio.NativeIO.Windows.access0 = lambda path, access: True
    except:
        pass

    return spark