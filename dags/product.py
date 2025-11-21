from datetime import datetime
from airflow.sdk import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, to_date, lit, when, 
    md5, concat_ws, coalesce, max as spark_max
)
from typing import List, Dict

import sys
sys.path.append('/opt/airflow/dags')
from utils.extract_load_template import ExtractLoadTemplate

default_args = {
    "owner": "airflow",
    "retries": 0,
}

PRODUCT_TABLES_CONFIG = [
    {
        "source_table": "Production.Product",
        "primary_key": "ProductID",
        "table_lakehouse": "product",
    },
    {
        "source_table": "Production.ProductCategory",
        "primary_key": "ProductCategoryID",
        "table_lakehouse": "product_category",
    },
    {
        "source_table": "Production.ProductSubcategory",
        "primary_key": "ProductSubcategoryID",
        "table_lakehouse": "product_subcategory",
    },
    {
        "source_table": "Production.ProductModel",
        "primary_key": "ProductModelID",
        "table_lakehouse": "product_model",
    },
]

@dag(
    dag_id="product",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,               
    max_active_tasks=3,
    tags=["spark"],
)
def product_pipeline():
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
    def transform_to_silver():
        """Transform bronze tables to silver by joining product tables"""
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
        
        try:
            # Transform query joining the product tables
            transform_query = """
            select
            	product.productid as productkey,
            	product.productnumber as productalternatekey,
            	product.name as productname,
            	category.name as categoryname,
            	sub.name as subcategoryname,
            	model.name as modelname,
            	product.color,
            	product.size,
            	product.sizeunitmeasurecode ,
            	product.weight ,
            	product.weightunitmeasurecode,
            	product.finishedgoodsflag ,
            	product.safetystocklevel ,
            	product.reorderpoint ,
            	product.standardcost ,
            	product.listprice ,
            	product.daystomanufacture ,
            	product.productline ,
            	product.class ,
            	product.style ,
            	product.sellstartdate ,
            	product.sellenddate ,
            	product.discontinueddate 
            from bronze.product product
            join bronze.product_subcategory sub
            on product.productsubcategoryid = sub.productsubcategoryid 
            join bronze.product_category category
            on sub.productcategoryid = category.productcategoryid 
            join bronze.product_model model
            on product.productmodelid = model.productmodelid 
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
                    .saveAsTable("silver.product_enriched")
                
                print(f"✅ Transformed and loaded {record_count} records to silver.product_enriched")
                return {
                    "status": "success",
                    "table": "product_enriched",
                    "records_transformed": record_count,
                    "message": f"✅ Transformed {record_count} records to silver.product_enriched"
                }
            else:
                print("ℹ️ No data to transform")
                return {
                    "status": "no_data",
                    "table": "product_enriched",
                    "records_transformed": 0,
                    "message": "ℹ️ No data to transform"
                }
        except Exception as e:
            print(f"❌ Error in transformation: {str(e)}")
            raise
        finally:
            spark.stop()
    
    @task
    def load_dim_product_scd2():
        """Load dim_product with SCD Type 2 - tracks historical changes"""

        # Configuration
        DIM_TABLE_NAME = "gold.dim_product"
        SOURCE_TABLE = "silver.product_enriched"
        TRACKING_ATTRIBUTES = [
            "productname", "categoryname", "subcategoryname", "modelname",
            "color", "size", "sizeunitmeasurecode", "weight", "weightunitmeasurecode",
            "finishedgoodsflag", "safetystocklevel", "reorderpoint",
            "standardcost", "listprice", "daystomanufacture",
            "productline", "class", "style",
            "sellstartdate", "sellenddate", "discontinueddate"
        ]

        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()

        try:
            # Read and deduplicate source data from silver layer
            df_silver = spark.table(SOURCE_TABLE)
            window = Window.partitionBy("productkey").orderBy(col("ingestion_date").desc())
            df_latest = (df_silver
                .withColumn("rn", row_number().over(window))
                .filter(col("rn") == 1)
                .drop("rn", "ingestion_date", "processed_at")
            )

            # Add hash column for change detection
            hash_expr = md5(concat_ws(
                "|", 
                *[coalesce(col(c).cast("string"), lit("")) for c in TRACKING_ATTRIBUTES]
            ))
            df_with_hash = df_latest.withColumn("attribute_hash", hash_expr)

            # Prepare source data with SCD Type 2 columns
            current_date = to_date(current_timestamp())
            df_source = (df_with_hash
                .withColumn("effective_date", current_date)
                .withColumn("expiration_date", lit(None).cast("date"))
                .withColumn("is_current", lit(True))
            )

            # Check if dimension table exists
            table_exists = True
            try:
                spark.table(DIM_TABLE_NAME)
            except Exception:
                table_exists = False

            # Cast to target schema
            def cast_to_schema(df):
                return df.select(
                    col("product_sk").cast(IntegerType()),
                    col("productkey").cast(IntegerType()),
                    col("productalternatekey").cast(StringType()),
                    col("productname").cast(StringType()),
                    col("categoryname").cast(StringType()),
                    col("subcategoryname").cast(StringType()),
                    col("modelname").cast(StringType()),
                    col("color").cast(StringType()),
                    col("size").cast(StringType()),
                    col("sizeunitmeasurecode").cast(StringType()),
                    col("weight").cast(FloatType()),
                    col("weightunitmeasurecode").cast(StringType()),
                    col("finishedgoodsflag").cast(BooleanType()),
                    col("safetystocklevel").cast(IntegerType()),
                    col("reorderpoint").cast(IntegerType()),
                    col("standardcost").cast(FloatType()),
                    col("listprice").cast(FloatType()),
                    col("daystomanufacture").cast(IntegerType()),
                    col("productline").cast(StringType()),
                    col("class").cast(StringType()),
                    col("style").cast(StringType()),
                    col("sellstartdate").cast(TimestampType()),
                    col("sellenddate").cast(TimestampType()),
                    col("discontinueddate").cast(TimestampType()),
                    col("attribute_hash").cast(StringType()),
                    col("effective_date").cast(DateType()),
                    col("expiration_date").cast(DateType()),
                    col("is_current").cast(BooleanType()),
                    col("created_at").cast(TimestampType()),
                    col("updated_at").cast(TimestampType())
                )

            # Initial load if table doesn't exist
            if not table_exists:
                window_sk = Window.orderBy("productkey")
                df_initial = (df_source
                    .withColumn("product_sk", row_number().over(window_sk))
                    .withColumn("created_at", current_timestamp())
                    .withColumn("updated_at", current_timestamp())
                )

                df_initial = cast_to_schema(df_initial)
                df_initial.write.format("delta").mode("overwrite").saveAsTable(DIM_TABLE_NAME)

                initial_count = df_initial.count()
                print(f"✅ Created {DIM_TABLE_NAME} with {initial_count} records (initial load)")

                return {
                    "status": "success",
                    "table": "dim_product",
                    "records_inserted": initial_count,
                    "message": f"✅ Created {DIM_TABLE_NAME} with {initial_count} records (initial load)"
                }

            # Incremental load
            # Get max surrogate key
            max_sk_result = spark.sql(f"SELECT COALESCE(MAX(product_sk), 0) AS max_sk FROM {DIM_TABLE_NAME}")
            max_sk = max_sk_result.collect()[0]["max_sk"]

            # Get existing current records
            df_existing = spark.sql(f"""
                SELECT 
                    productkey, 
                    COALESCE(attribute_hash, '') AS attribute_hash, 
                    product_sk
                FROM {DIM_TABLE_NAME}
                WHERE is_current = true
            """)

            # Identify new and changed products
            df_joined = (df_source.alias("source")
                .join(
                    df_existing.alias("target"),
                    col("source.productkey") == col("target.productkey"),
                    "left"
                )
            )

            df_compared = (df_joined
                .withColumn("is_new", col("target.productkey").isNull())
                .withColumn("is_changed", 
                    (col("target.productkey").isNotNull()) & 
                    (col("source.attribute_hash") != col("target.attribute_hash"))
                )
                .withColumn("old_product_sk", col("target.product_sk"))
            )

            # Select only source columns
            source_cols = [col(f"source.{c}").alias(c) for c in df_source.columns]
            df_compared = df_compared.select(
                *source_cols,
                col("is_new"),
                col("is_changed"),
                col("old_product_sk")
            )

            df_to_process = df_compared.filter(col("is_new") | col("is_changed"))

            if df_to_process.count() == 0:
                print("ℹ️ No new or changed products to load")
                return {
                    "status": "no_changes",
                    "table": "dim_product",
                    "records_inserted": 0,
                    "message": "ℹ️ No new or changed products to load"
                }

            # Expire changed records using temporary view and SQL
            df_changed = df_to_process.filter(col("is_changed"))
            if df_changed.count() > 0:
                print(f"🔄 Expiring {df_changed.count()} changed product records")

                # Create temp view of changed records
                df_changed.select("productkey", "old_product_sk").createOrReplaceTempView("temp_changed_products")

                # Read current dimension table
                df_dim = spark.table(DIM_TABLE_NAME)

                # Update records by joining with temp view
                df_dim_updated = df_dim.alias("dim").join(
                    spark.table("temp_changed_products").alias("changed"),
                    (col("dim.productkey") == col("changed.productkey")) &
                    (col("dim.product_sk") == col("changed.old_product_sk")) &
                    (col("dim.is_current") == True),
                    "left"
                ).select(
                    col("dim.*"),
                    col("changed.productkey").alias("matched_key")
                ).withColumn(
                    "expiration_date",
                    when(col("matched_key").isNotNull(), current_date).otherwise(col("dim.expiration_date"))
                ).withColumn(
                    "is_current",
                    when(col("matched_key").isNotNull(), lit(False)).otherwise(col("dim.is_current"))
                ).withColumn(
                    "updated_at",
                    when(col("matched_key").isNotNull(), current_timestamp()).otherwise(col("dim.updated_at"))
                ).drop("matched_key")

                # Overwrite dimension table with updated records
                df_dim_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "false").saveAsTable(DIM_TABLE_NAME)

            # Generate surrogate keys for new versions
            window_sk = Window.orderBy("productkey")
            df_with_sk = (df_to_process
                .withColumn("rn", row_number().over(window_sk))
                .withColumn("product_sk", col("rn") + max_sk)
                .drop("rn", "is_new", "is_changed", "old_product_sk")
                .withColumn("created_at", current_timestamp())
                .withColumn("updated_at", current_timestamp())
            )

            df_to_insert = cast_to_schema(df_with_sk)

            # Insert new records
            df_to_insert.write.format("delta").mode("append").saveAsTable(DIM_TABLE_NAME)

            inserted_count = df_to_insert.count()
            print(f"✅ Loaded {inserted_count} records to {DIM_TABLE_NAME} (SCD Type 2)")

            return {
                "status": "success",
                "table": "dim_product",
                "records_inserted": inserted_count,
                "message": f"✅ Loaded {inserted_count} records to {DIM_TABLE_NAME} (SCD Type 2)"
            }

        except Exception as e:
            print(f"❌ Error in SCD Type 2 transformation: {str(e)}")
            traceback.print_exc()
            raise
        finally:
            spark.stop()

    # Define task dependencies
    extract_tasks = extract_to_bronze.expand(config=PRODUCT_TABLES_CONFIG)
    silver_task = transform_to_silver()
    dim_task = load_dim_product_scd2()
    
    # Set dependencies: extract -> silver -> dim_product (SCD Type 2)
    extract_tasks >> silver_task >> dim_task
    
product_pipeline()