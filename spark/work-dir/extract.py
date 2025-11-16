from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date, max as spark_max
import argparse

parser = argparse.ArgumentParser(description="Ingestion from MSSQL to Delta Lake")

parser.add_argument("--mssql_table", required=True, help="Source MSSQL table (e.g. dbo.users)")
parser.add_argument("--primary_key", required=True, help="Primary key column name (e.g. user_id)")
parser.add_argument("--table_lakehouse", required=True, help="Logical table name (e.g. users, orders)")

args = parser.parse_args()

MSSQL_TABLE = args.mssql_table
PRIMARY_KEY = args.primary_key
TABLE_NAME = args.table_lakehouse

MSSQL_URL = "jdbc:sqlserver://source-mssql:1433;databaseName=AdventureWorks2022;encrypt=false"
MSSQL_USER = "sa"
MSSQL_PASSWORD = "YourStrong!Passw0rd"
EVENT_COL = "ModifiedDate"
BRONZE_TABLE = f"bronze.{TABLE_NAME}"
HWM_TABLE = "controll.high_water_mark"

# 1. Initialize Spark Session (Configuration loaded via spark-submit)
spark = SparkSession.builder \
    .appName("PostgresToMinIODelta") \
    .enableHiveSupport() \
    .getOrCreate()


jdbc_props = {
    "user": MSSQL_USER,
    "password": MSSQL_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def get_last_watermark(table_name):
    last_day = spark.sql(f"""
        SELECT last_event_time 
        FROM {HWM_TABLE}
        WHERE table_name = '{table_name}'
    """)

    if last_day.count() == 0:
        return None
    else:
        return last_day.collect()[0][0]

def full_ingest():
    query = f"(SELECT * FROM {MSSQL_TABLE}) AS src"
    df = spark.read.jdbc(url=MSSQL_URL, table=query, properties=jdbc_props)
    return df

def incremental_ingest(last_event_time):
    query = f"(SELECT * FROM {MSSQL_TABLE} WHERE {EVENT_COL} > '{last_event_time}') AS src"
    df = spark.read.jdbc(url=MSSQL_URL, table=query, properties=jdbc_props)
    return df

def load_data(df):
    df_with_date = df.withColumn("ingestion_date", to_date(current_timestamp()))
    
    df_with_date.write.format("delta") \
        .mode("append") \
        .partitionBy("ingestion_date") \
        .saveAsTable(BRONZE_TABLE)
    
    print(f"Appended {df_with_date.count()} records to bronze.{TABLE_NAME} "
          f"on ingestion_date = {df_with_date.select('ingestion_date').first()[0]}")
    
    return df_with_date

def update_watermark(df):
    new_hwm = df.select(
        lit(TABLE_NAME).alias("table_name"),
        spark_max(PRIMARY_KEY).alias("last_id"),  # Giả sử user_id là khóa chính tăng dần
        spark_max(EVENT_COL).alias("last_event_time"),
        spark_max("ingestion_date").alias("last_ingestion_date"),
        current_timestamp().alias("processed_at")
    )

    existing_hwm = spark.sql(f"SELECT * FROM {HWM_TABLE}")
    updated_hwm = existing_hwm.filter(col("table_name") != TABLE_NAME).union(new_hwm)
    updated_hwm.write.format("delta").mode("overwrite").saveAsTable(HWM_TABLE)
    print(f"💾 Updated watermark for table '{TABLE_NAME}'")

if __name__ == "__main__":
    last_event_time = get_last_watermark(TABLE_NAME)
    if last_event_time is None:
        print("🔄 Performing full ingestion...")
        df = full_ingest()
    else:
        print(f"🔄 Performing incremental ingestion since {last_event_time}...")
        df = incremental_ingest(last_event_time)

    if df.count() > 0:
        df_with_date = load_data(df)      
        update_watermark(df_with_date)
    else:
        print("ℹ️ No new data to ingest.")
    spark.stop()