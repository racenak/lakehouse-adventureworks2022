from datetime import datetime
from airflow.sdk import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, to_date, lit, when, 
    md5, concat_ws, coalesce, max as spark_max
)
from typing import List, Dict
from delta.tables import DeltaTable

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
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
        
        try:
            # Read latest data from silver
            df_silver = spark.table("silver.product_enriched")
            
            # Get the latest record per productkey (in case of multiple ingestion dates)
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number
            
            window = Window.partitionBy("productkey").orderBy(col("ingestion_date").desc())
            df_latest = df_silver \
                .withColumn("rn", row_number().over(window)) \
                .filter(col("rn") == 1) \
                .drop("rn", "ingestion_date", "processed_at")
            
            # Create hash of attributes that should trigger SCD Type 2 changes
            # This includes all descriptive attributes but excludes technical fields
            attribute_cols = [
                "productname", "categoryname", "subcategoryname", "modelname",
                "color", "size", "sizeunitmeasurecode", "weight", "weightunitmeasurecode",
                "finishedgoodsflag", "safetystocklevel", "reorderpoint",
                "standardcost", "listprice", "daystomanufacture",
                "productline", "class", "style",
                "sellstartdate", "sellenddate", "discontinueddate"
            ]
            
            # Create hash column for change detection
            df_source = df_latest.withColumn(
                "attribute_hash",
                md5(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in attribute_cols]))
            )
            
            # Prepare source data with SCD Type 2 columns
            current_date = to_date(current_timestamp())
            df_source_prepared = df_source \
                .withColumn("effective_date", current_date) \
                .withColumn("expiration_date", lit(None).cast("date")) \
                .withColumn("is_current", lit(True))
            
            # Create or get dim_product table
            dim_table_name = "gold.dim_product"
            table_exists = False
            
            try:
                # Try to read existing dim table
                dim_table = DeltaTable.forName(spark, dim_table_name)
                table_exists = True
                print(f"📖 Found existing {dim_table_name} table")
            except Exception:
                # Table doesn't exist, will create with initial load
                print(f"📦 {dim_table_name} table does not exist, will create with initial load")
                table_exists = False
            
            if not table_exists:
                # Initial load - create table with all records as current
                from pyspark.sql.functions import row_number
                from pyspark.sql.window import Window
                from pyspark.sql.types import FloatType, IntegerType, BooleanType, DateType, TimestampType, StringType
                
                window_sk = Window.orderBy("productkey")
                df_initial = df_source_prepared \
                    .withColumn("product_sk", row_number().over(window_sk)) \
                    .withColumn("created_at", current_timestamp()) \
                    .withColumn("updated_at", current_timestamp()) \
                    .select(
                        col("product_sk").cast(IntegerType()).alias("product_sk"),
                        col("productkey").cast(IntegerType()).alias("productkey"),
                        col("productalternatekey").cast(StringType()).alias("productalternatekey"),
                        col("productname").cast(StringType()).alias("productname"),
                        col("categoryname").cast(StringType()).alias("categoryname"),
                        col("subcategoryname").cast(StringType()).alias("subcategoryname"),
                        col("modelname").cast(StringType()).alias("modelname"),
                        col("color").cast(StringType()).alias("color"),
                        col("size").cast(StringType()).alias("size"),
                        col("sizeunitmeasurecode").cast(StringType()).alias("sizeunitmeasurecode"),
                        col("weight").cast(FloatType()).alias("weight"),  # Explicit cast to REAL/FLOAT
                        col("weightunitmeasurecode").cast(StringType()).alias("weightunitmeasurecode"),
                        col("finishedgoodsflag").cast(BooleanType()).alias("finishedgoodsflag"),
                        col("safetystocklevel").cast(IntegerType()).alias("safetystocklevel"),
                        col("reorderpoint").cast(IntegerType()).alias("reorderpoint"),
                        col("standardcost").cast(FloatType()).alias("standardcost"),
                        col("listprice").cast(FloatType()).alias("listprice"),
                        col("daystomanufacture").cast(IntegerType()).alias("daystomanufacture"),
                        col("productline").cast(StringType()).alias("productline"),
                        col("class").cast(StringType()).alias("class"),
                        col("style").cast(StringType()).alias("style"),
                        col("sellstartdate").cast(TimestampType()).alias("sellstartdate"),
                        col("sellenddate").cast(TimestampType()).alias("sellenddate"),
                        col("discontinueddate").cast(TimestampType()).alias("discontinueddate"),
                        col("attribute_hash").cast(StringType()).alias("attribute_hash"),
                        col("effective_date").cast(DateType()).alias("effective_date"),
                        col("expiration_date").cast(DateType()).alias("expiration_date"),
                        col("is_current").cast(BooleanType()).alias("is_current"),
                        col("created_at").cast(TimestampType()).alias("created_at"),
                        col("updated_at").cast(TimestampType()).alias("updated_at")
                    )
                
                df_initial.write.format("delta").mode("overwrite").saveAsTable(dim_table_name)
                initial_count = df_initial.count()
                print(f"✅ Created {dim_table_name} with {initial_count} records (initial load)")
                
                return {
                    "status": "success",
                    "table": "dim_product",
                    "records_inserted": initial_count,
                    "message": f"✅ Created {dim_table_name} with {initial_count} records (initial load)"
                }
            else:
                # Incremental SCD Type 2 load
                # Get max product_sk for new records
                max_sk_result = spark.sql(f"SELECT COALESCE(MAX(product_sk), 0) AS max_sk FROM {dim_table_name}")
                max_sk = max_sk_result.collect()[0]["max_sk"] if max_sk_result.count() > 0 else 0
                
                # Get existing current records with their hash
                df_existing_current = spark.sql(f"""
                    SELECT 
                        productkey, 
                        COALESCE(attribute_hash, '') AS attribute_hash, 
                        product_sk
                    FROM {dim_table_name}
                    WHERE is_current = true
                """)
                
                # Add hash to existing if not present (for backward compatibility)
                if "attribute_hash" not in df_existing_current.columns:
                    # Calculate hash for existing records if needed
                    df_existing_current = spark.sql(f"""
                        SELECT 
                            productkey,
                            product_sk,
                            '' AS attribute_hash
                        FROM {dim_table_name}
                        WHERE is_current = true
                    """)
                
                # Identify new and changed products
                df_source_with_hash = df_source_prepared.withColumn(
                    "source_hash", col("attribute_hash")
                )
                
                # Left join to find new products (no match) and changed products (hash mismatch)
                df_joined = df_source_with_hash.alias("source") \
                    .join(
                        df_existing_current.alias("target"),
                        col("source.productkey") == col("target.productkey"),
                        "left"
                    )
                
                # Create flags and select only needed columns to avoid ambiguity
                df_compared = df_joined \
                    .withColumn("is_new", col("target.productkey").isNull()) \
                    .withColumn("is_changed", 
                        (col("target.productkey").isNotNull()) & 
                        (col("source.attribute_hash") != col("target.attribute_hash"))
                    ) \
                    .withColumn("old_product_sk", col("target.product_sk")) \
                    .select(
                        # All source columns (use explicit list to avoid duplicates)
                        col("source.productkey"),
                        col("source.productalternatekey"),
                        col("source.productname"),
                        col("source.categoryname"),
                        col("source.subcategoryname"),
                        col("source.modelname"),
                        col("source.color"),
                        col("source.size"),
                        col("source.sizeunitmeasurecode"),
                        col("source.weight"),
                        col("source.weightunitmeasurecode"),
                        col("source.finishedgoodsflag"),
                        col("source.safetystocklevel"),
                        col("source.reorderpoint"),
                        col("source.standardcost"),
                        col("source.listprice"),
                        col("source.daystomanufacture"),
                        col("source.productline"),
                        col("source.class"),
                        col("source.style"),
                        col("source.sellstartdate"),
                        col("source.sellenddate"),
                        col("source.discontinueddate"),
                        col("source.attribute_hash"),
                        col("source.effective_date"),
                        col("source.expiration_date"),
                        col("source.is_current"),
                        # Computed columns
                        col("is_new"),
                        col("is_changed"),
                        col("old_product_sk")
                    )
                
                # Filter to only new and changed products
                df_to_process = df_compared.filter(
                    col("is_new") | col("is_changed")
                )
                
                if df_to_process.count() > 0:
                    # Expire changed products
                    df_changed = df_to_process.filter(col("is_changed"))
                    if df_changed.count() > 0:
                        print(f"🔄 Expiring {df_changed.count()} changed product records")
                        # Select productkey from source (after join, it's just "productkey" since we dropped target.productkey)
                        df_changed_for_merge = df_changed.select(
                            col("productkey"),
                            col("old_product_sk")
                        )
                        dim_table.alias("target").merge(
                            df_changed_for_merge.alias("source"),
                            "target.productkey = source.productkey AND target.product_sk = source.old_product_sk AND target.is_current = true"
                        ).whenMatchedUpdate(
                            set={
                                "expiration_date": current_date,
                                "is_current": lit(False),
                                "updated_at": current_timestamp()
                            }
                        ).execute()
                    
                    # Generate product_sk for new records
                    from pyspark.sql.functions import row_number
                    from pyspark.sql.window import Window
                    
                    # Use explicit column reference to avoid ambiguity
                    window_sk = Window.orderBy(col("productkey"))
                    df_with_sk = df_to_process \
                        .withColumn("rn", row_number().over(window_sk)) \
                        .withColumn("product_sk", col("rn") + max_sk) \
                        .drop("rn", "source_hash", "is_new", "is_changed", "old_product_sk")
                    
                    # Prepare final insert with all required columns and explicit type casting
                    from pyspark.sql.types import FloatType, IntegerType, BooleanType, DateType, TimestampType, StringType
                    
                    df_to_insert = df_with_sk \
                        .withColumn("created_at", current_timestamp()) \
                        .withColumn("updated_at", current_timestamp()) \
                        .select(
                            col("product_sk").cast(IntegerType()).alias("product_sk"),
                            col("productkey").cast(IntegerType()).alias("productkey"),
                            col("productalternatekey").cast(StringType()).alias("productalternatekey"),
                            col("productname").cast(StringType()).alias("productname"),
                            col("categoryname").cast(StringType()).alias("categoryname"),
                            col("subcategoryname").cast(StringType()).alias("subcategoryname"),
                            col("modelname").cast(StringType()).alias("modelname"),
                            col("color").cast(StringType()).alias("color"),
                            col("size").cast(StringType()).alias("size"),
                            col("sizeunitmeasurecode").cast(StringType()).alias("sizeunitmeasurecode"),
                            col("weight").cast(FloatType()).alias("weight"),  # Explicit cast to REAL/FLOAT
                            col("weightunitmeasurecode").cast(StringType()).alias("weightunitmeasurecode"),
                            col("finishedgoodsflag").cast(BooleanType()).alias("finishedgoodsflag"),
                            col("safetystocklevel").cast(IntegerType()).alias("safetystocklevel"),
                            col("reorderpoint").cast(IntegerType()).alias("reorderpoint"),
                            col("standardcost").cast(FloatType()).alias("standardcost"),
                            col("listprice").cast(FloatType()).alias("listprice"),
                            col("daystomanufacture").cast(IntegerType()).alias("daystomanufacture"),
                            col("productline").cast(StringType()).alias("productline"),
                            col("class").cast(StringType()).alias("class"),
                            col("style").cast(StringType()).alias("style"),
                            col("sellstartdate").cast(TimestampType()).alias("sellstartdate"),
                            col("sellenddate").cast(TimestampType()).alias("sellenddate"),
                            col("discontinueddate").cast(TimestampType()).alias("discontinueddate"),
                            col("attribute_hash").cast(StringType()).alias("attribute_hash"),
                            col("effective_date").cast(DateType()).alias("effective_date"),
                            col("expiration_date").cast(DateType()).alias("expiration_date"),
                            col("is_current").cast(BooleanType()).alias("is_current"),
                            col("created_at").cast(TimestampType()).alias("created_at"),
                            col("updated_at").cast(TimestampType()).alias("updated_at")
                        )
                    
                    # Insert new/changed records
                    df_to_insert.write.format("delta").mode("append").saveAsTable(dim_table_name)
                    
                    inserted_count = df_to_insert.count()
                    print(f"✅ Loaded {inserted_count} records to {dim_table_name} (SCD Type 2)")
                    
                    return {
                        "status": "success",
                        "table": "dim_product",
                        "records_inserted": inserted_count,
                        "message": f"✅ Loaded {inserted_count} records to {dim_table_name} (SCD Type 2)"
                    }
                else:
                    print("ℹ️ No new or changed products to load")
                    return {
                        "status": "no_changes",
                        "table": "dim_product",
                        "records_inserted": 0,
                        "message": "ℹ️ No new or changed products to load"
                    }
                
        except Exception as e:
            print(f"❌ Error in SCD Type 2 transformation: {str(e)}")
            import traceback
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