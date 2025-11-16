from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
import argparse

parser = argparse.ArgumentParser(description="Transform Sales Data")

parser.add_argument("--mode", required=True, choices=["full", "incremental"], help="Ingestion mode: full or incremental")
args = parser.parse_args()
MODE = args.mode

spark = SparkSession.builder \
    .appName("TransformSalesData") \
    .enableHiveSupport() \
    .getOrCreate()

def load_full(table_name):
    return f"SELECT * FROM {table_name}"

def load_incremental(table_name, last_event_time):
    return f"SELECT * FROM {table_name} WHERE ModifiedDate = '{last_event_time}'"

def get_last_event_time(table_name):
    result = spark.sql(f"""
        SELECT last_event_time 
        FROM controll.high_water_mark
        WHERE table_name = '{table_name}'
    """)
    if result.count() == 0:
        return None
    else:
        return result.collect()[0][0]
    
def transform_data():
    sales_order_header = "bronze.sales_order_header"
    sales_order_detail = "bronze.sale_order_detail"

    if MODE == "full":
        query_header = load_full(sales_order_header)
        query_detail = load_full(sales_order_detail)
    else:
        last_event_time = get_last_event_time("sales")
        query_header = load_incremental(sales_order_header, last_event_time)
        query_detail = load_incremental(sales_order_detail, last_event_time)

    df_header = spark.sql(query_header)
    df_header.select()

