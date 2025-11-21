from datetime import datetime
from airflow.sdk import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, to_date, lit, when, 
    md5, concat_ws, coalesce, max as spark_max, from_xml
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from typing import List, Dict

import sys
sys.path.append('/opt/airflow/dags')
from utils.extract_load_template import ExtractLoadTemplate

default_args = {
    "owner": "airflow",
    "retries": 0,
}

CUSTOMERS_TABLES_CONFIG = [
    {
        "source_table": "Sales.Customer",
        "primary_key": "CustomerID",
        "table_lakehouse": "customer",
    },
    {
        "source_table": "Person.Person",
        "primary_key": "BusinessEntityID",
        "table_lakehouse": "person",
    }
]

@dag(
    dag_id="customer_pipeline",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,               
    max_active_tasks=3,
    tags=["spark"],
)
def customer_pipeline():
    @task
    def extract_to_bronze(config: Dict):
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()

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
    def transform_customer_dempgraphics():
        spark = SparkSession.builder.remote("sc://spark-connect:15002").enableHiveSupport().getOrCreate()
        
        df = spark.sql("""
        select businessentityid, demographics 
        from bronze.person
        where persontype = 'IN'
        """)

        xml_schema = StructType([
            StructField("TotalPurchaseYTD", DoubleType()),
            StructField("DateFirstPurchase", StringType()),
            StructField("BirthDate", StringType()),
            StructField("MaritalStatus", StringType()),
            StructField("YearlyIncome", StringType()),
            StructField("Gender", StringType()),
            StructField("TotalChildren", StringType()),
            StructField("NumberChildrenAtHome", StringType()),
            StructField("Education", StringType()),
            StructField("Occupation", StringType()),
            StructField("HomeOwnerFlag", StringType()),
            StructField("NumberCarsOwned", StringType()),
            StructField("CommuteDistance", StringType())
        ])

        df_parsed = df.withColumn("parsed_xml", from_xml(col("demographics"), xml_schema))

        df_flattened = df_parsed.select(
        col("businessentityid").alias("businessentityid"),
        col("parsed_xml.TotalPurchaseYTD").alias("totalpurchaseytd"),
        col("parsed_xml.DateFirstPurchase").alias("datefirstpurchase"),
        col("parsed_xml.BirthDate").alias("birthdate"),
        col("parsed_xml.MaritalStatus").alias("maritalstatus"),
        col("parsed_xml.YearlyIncome").alias("yearlyincome"),
        col("parsed_xml.Gender").alias("gender"),
        col("parsed_xml.TotalChildren").alias("totalchildren"),
        col("parsed_xml.NumberChildrenAtHome").alias("numberchildrenathome"),
        col("parsed_xml.Education").alias("education"),
        col("parsed_xml.Occupation").alias("occupation"),
        col("parsed_xml.HomeOwnerFlag").alias("homeownerflag"),
        col("parsed_xml.NumberCarsOwned").alias("numbercarsowned"),
        col("parsed_xml.CommuteDistance").alias("commutedistance")
        ).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.person_demographics")

        spark.stop()

    @task
    def merge_and_load_gold():
        spark =  SparkSession.builder.remote("sc://spark-connect:15002").enableHiveSupport().getOrCreate()

        try:
            query = """
            select 
            	customer.customerid as customerkey,
            	customer.accountnumber ,
            	person.title ,
            	person.firstname ,
            	person.middlename ,
            	person.lastname ,
            	person.suffix ,
            	demographics.totalpurchaseytd,
            	demographics.datefirstpurchase ,
            	demographics.birthdate,
            	demographics.maritalstatus ,
            	demographics.yearlyincome ,
            	demographics.gender ,
            	demographics.totalchildren ,
            	demographics.numberchildrenathome ,
            	demographics.education ,
            	demographics.occupation ,
            	demographics.homeownerflag ,
            	demographics.numbercarsowned ,
            	demographics.commutedistance 
            from bronze.customer  
            join bronze.person 
            on customer.personid = person.businessentityid 
            join silver.person_demographics demographics
            on customer.personid = demographics.businessentityid 
            where person.persontype = 'IN'
            """
            dim_customer = spark.sql(query)
            record_count = dim_customer.count()
            if record_count > 0:
                dim_customer.write.format("delta") \
                    .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .saveAsTable("gold.dim_customers")
                
                print(f"✅ Transformed and loaded {record_count} records to gold.dim_customers")
                return {
                    "status": "success",
                    "table": "dim_customers",
                    "records_transformed": record_count,
                    "message": f"✅ Transformed {record_count} records to gold.dim_customers"
                }
            else:
                print("ℹ️ No data to transform")
                return {
                    "status": "no_data",
                    "table": "dim_customers",
                    "records_transformed": 0,
                    "message": "ℹ️ No data to transform"
                }
        except Exception as e:
            print(f"❌ Error in transformation: {str(e)}")
            raise
        finally:
            spark.stop()

    extracts = extract_to_bronze.expand(config=CUSTOMERS_TABLES_CONFIG)
    transform_demographic = transform_customer_dempgraphics()
    load_to_gold = merge_and_load_gold()

    extracts >> transform_demographic >> load_to_gold 
    

customer_pipeline()