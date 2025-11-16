from datetime import datetime
from airflow.sdk import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date
from typing import List, Dict

import sys
sys.path.append('/opt/airflow/dags')
from utils.extract_load_template import ExtractLoadTemplate

default_args = {
    "owner": "airflow",
    "retries": 0,
}

ADDRESS_TABLES_CONFIG = [
    {
        "source_table": "Person.Address",
        "primary_key": "AddressID",
        "table_lakehouse": "address",
    },
    {
        "source_table": "Person.StateProvince",
        "primary_key": "StateProvinceID",
        "table_lakehouse": "state_province",
    },
    {
        "source_table": "Person.CountryRegion",
        "primary_key": "CountryRegionCode",
        "table_lakehouse": "country_region",
    }
]

@dag(
    dag_id="address",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,               
    max_active_tasks=3,
    tags=["spark"],
)
def address_pipeline():
    @task
    def extract_to_bronze(config: Dict):
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
    def transform_to_gold():
        """Transform bronze tables to silver by joining address, state_province, and country_region"""
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
        
        try:
            # Transform query joining the three bronze tables
            transform_query = """
            SELECT
                address.addressid as addresskey,
                address.addressline1,
                address.addressline2,
                address.postalcode,
                address.city,
                state.stateprovincecode,
                state.name AS state_name,
                country.countryregioncode,
                country.name AS country_name
            FROM bronze.address address
            INNER JOIN bronze.state_province state
                ON address.stateprovinceid = state.stateprovinceid
            INNER JOIN bronze.country_region country
                ON state.countryregioncode = country.countryregioncode
            """
            
            # Execute transformation
            df_silver = spark.sql(transform_query)
            
            # Add metadata columns
            df_silver_with_metadata = df_silver \
                .withColumn("ingestion_date", to_date(current_timestamp())) \
                .withColumn("processed_at", current_timestamp())
            
            # Write to silver layer
            record_count = df_silver_with_metadata.count()
            
            if record_count > 0:
                df_silver_with_metadata.write.format("delta") \
                    .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .partitionBy("ingestion_date") \
                    .saveAsTable("gold.dim_address")
                
                print(f"✅ Transformed and loaded {record_count} records to gold.dim_address")
                return {
                    "status": "success",
                    "table": "dim_address",
                    "records_transformed": record_count,
                    "message": f"✅ Transformed {record_count} records to gold.dim_address"
                }
            else:
                print("ℹ️ No data to transform")
                return {
                    "status": "no_data",
                    "table": "dim_address",
                    "records_transformed": 0,
                    "message": "ℹ️ No data to transform"
                }
        except Exception as e:
            print(f"❌ Error in transformation: {str(e)}")
            raise
        finally:
            spark.stop()
    
    # Define task dependencies: extract first, then transform
    extract_tasks = extract_to_bronze.expand(config=ADDRESS_TABLES_CONFIG)
    transform_task = transform_to_gold()
    
    # Set dependency: transform runs after all extract tasks complete
    # extract_tasks >> transform_task means: extract runs first, then transform
    extract_tasks >> transform_task
    
address_pipeline()