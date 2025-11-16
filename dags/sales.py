from datetime import datetime
from airflow.sdk import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date, date_format, col
from typing import List, Dict

import sys
sys.path.append('/opt/airflow/dags')
from utils.extract_load_template import ExtractLoadTemplate

default_args = {
    "owner": "airflow",
    "retries": 0,
}

SALES_TABLES_CONFIG = [
    {
        "source_table": "Sales.SalesOrderHeader",
        "primary_key": "SalesOrderID",
        "table_lakehouse": "sales_order_header",
    },
    {
        "source_table": "Sales.SalesOrderDetail",
        "primary_key": "SalesOrderDetailID",
        "table_lakehouse": "sales_order_detail",
    },
]

@dag(
    dag_id="sales",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,               
    max_active_tasks=3,
    tags=["spark"],
)
def sales_pipeline():
    @task
    def extract_to_bronze(config: Dict) -> Dict:
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
        """Extract using reusable template"""
        extractor = ExtractLoadTemplate(spark)
        
        extractor.extract_from_jdbc(
            jdbc_url="jdbc:sqlserver://source-mssql:1433;databaseName=AdventureWorks2022;encrypt=false",
            jdbc_user="sa",
            jdbc_password="YourStrong!Passw0rd",
            jdbc_driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
            source_table=config["source_table"],
            primary_key=config["primary_key"],
            table_lakehouse=config["table_lakehouse"]
        )
        spark.stop()
        return {
            "status": "loaded",
            "table": config["table_lakehouse"],
            "message": f"✅ Loaded {config['table_lakehouse']}"
        }

    @task
    def transform_load_gold():
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
        try:
            # Transform query joining the three bronze tables
            transform_query = """
            SELECT
                soh.orderdate,
                soh.duedate,
                soh.shipdate,
                sod.productid,
                soh.customerid,
                sod.specialofferid,
                soh.currencyrateid,
                soh.salesordernumber,
                soh.revisionnumber,
                sod.carriertrackingnumber,
                soh.shiptoaddressid,
                sod.orderqty,
                sod.unitprice,
                sod.unitpricediscount,
                sod.linetotal
            FROM bronze.sales_order_detail sod
            LEFT JOIN bronze.sales_order_header soh
                ON sod.salesorderid = soh.salesorderid
            """
            
            # Execute transformation
            df_sales = spark.sql(transform_query)

            # Convert date columns to integer format YYYYMMDD
            df_sales = df_sales \
                .withColumn("orderdatekey", date_format(col("orderdate"), "yyyyMMdd").cast("int")) \
                .withColumn("duedatekey", date_format(col("duedate"), "yyyyMMdd").cast("int")) \
                .withColumn("shipdatekey", date_format(col("shipdate"), "yyyyMMdd").cast("int")) \
                .drop("orderdate", "duedate", "shipdate") \
                .withColumn("ingestion_date", to_date(current_timestamp())) \
                .withColumn("processed_at", current_timestamp())

            record_count = df_sales.count()

            if record_count > 0:
                df_sales.write.format("delta") \
                    .mode("append") \
                    .option("overwriteSchema", "true") \
                    .partitionBy("ingestion_date") \
                    .saveAsTable("gold.fact_sales")
                
                print(f"✅ Transformed and loaded {record_count} records to gold.fact_sales")
                return {
                    "status": "success",
                    "table": "fact_sales",
                    "records_transformed": record_count,
                    "message": f"✅ Transformed {record_count} records to gold.fact_sales"
                }
            else:
                print("ℹ️ No data to transform")
                return {
                    "status": "no_data",
                    "table": "fact_sales",
                    "records_transformed": 0,
                    "message": "ℹ️ No data to transform"
                }
        except Exception as e:
            print(f"❌ Error in transformation: {str(e)}")
            raise
        finally:
            spark.stop()
    
    extract = extract_to_bronze.expand(config=SALES_TABLES_CONFIG)
    transform = transform_load_gold()

    extract >> transform
sales_pipeline()