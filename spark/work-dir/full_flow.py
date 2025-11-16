from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
import argparse

# ============================================================================
# ARGUMENT PARSING
# ============================================================================
parser = argparse.ArgumentParser(description="Incremental ETL from MSSQL to Delta Lake")

parser.add_argument("--mssql_table", required=True, help="Source MSSQL table (e.g. dbo.users)")
parser.add_argument("--primary_key", required=True, help="Primary key column name (e.g. user_id)")
parser.add_argument("--table_lakehouse", required=True, help="Logical table name (e.g. users, orders)")
parser.add_argument("--event_col", default="ModifiedDate", help="Watermark column (default: ModifiedDate)")
parser.add_argument("--transform_mode", choices=['none', 'custom'], default='none', 
                    help="Transformation mode: 'none' for direct load, 'custom' for custom transformation")
parser.add_argument("--silver_table", help="Silver table name (required if transform_mode=custom)")

args = parser.parse_args()

# ============================================================================
# CONFIGURATION
# ============================================================================
MSSQL_TABLE = args.mssql_table
PRIMARY_KEY = args.primary_key
BRONZE_TABLE = args.table_lakehouse
EVENT_COL = args.event_col
TRANSFORM_MODE = args.transform_mode
SILVER_TABLE = args.silver_table

MSSQL_URL = "jdbc:sqlserver://source-mssql:1433;databaseName=AdventureWorks2022;encrypt=false"
MSSQL_USER = "sa"
MSSQL_PASSWORD = "YourStrong!Passw0rd"

BRONZE_PATH = f"bronze.{BRONZE_TABLE}"
SILVER_PATH = f"silver.{SILVER_TABLE}" if SILVER_TABLE else None
HWM_TABLE = "controll.high_water_mark"

jdbc_props = {
    "user": MSSQL_USER,
    "password": MSSQL_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# ============================================================================
# SPARK SESSION
# ============================================================================
spark = SparkSession.builder \
    .appName(f"Incremental ETL - {BRONZE_TABLE}") \
    .enableHiveSupport() \
    .getOrCreate()

# ============================================================================
# EXTRACT FUNCTIONS
# ============================================================================
def get_last_watermark(table_name):
    """Get last watermark from control table"""
    try:
        last_day = spark.sql(f"""
            SELECT last_event_time 
            FROM {HWM_TABLE}
            WHERE table_name = '{table_name}'
        """)

        if last_day.count() == 0:
            return None
        else:
            return last_day.collect()[0][0]
    except Exception as e:
        print(f"⚠️ Warning: Could not read watermark. Assuming first run. Error: {e}")
        return None


def extract_full():
    """Full extraction from source"""
    print(f"🔄 Performing FULL ingestion from {MSSQL_TABLE}...")
    query = f"(SELECT * FROM {MSSQL_TABLE}) AS src"
    df = spark.read.jdbc(url=MSSQL_URL, table=query, properties=jdbc_props)
    print(f"📦 Extracted {df.count()} records (full load)")
    return df


def extract_incremental(last_event_time):
    """Incremental extraction from source"""
    print(f"🔄 Performing INCREMENTAL ingestion from {MSSQL_TABLE} since {last_event_time}...")
    query = f"(SELECT * FROM {MSSQL_TABLE} WHERE {EVENT_COL} > '{last_event_time}') AS src"
    df = spark.read.jdbc(url=MSSQL_URL, table=query, properties=jdbc_props)
    print(f"📦 Extracted {df.count()} new records (incremental)")
    return df


# ============================================================================
# TRANSFORM FUNCTIONS
# ============================================================================
def transform_business_entity_address():
    """
    Custom transformation for business entity address
    Joins bronze tables to create silver.business_entity_address
    """
    print(f"🔧 Applying custom transformation for {SILVER_TABLE}...")
    
    df_transformed = spark.sql("""
        SELECT
            bea.businessentityid,
            address.addressline1,
            address.addressline2,
            address.city,
            s.stateprovincecode,
            s.countryregioncode,
            s.name AS state_province_name,
            address.postalcode,
            CURRENT_TIMESTAMP() AS etl_loaded_at
        FROM bronze.bussiness_entity_address bea
        JOIN bronze.address address
            ON bea.addressid = address.addressid 
        JOIN bronze.state_province s
            ON address.stateprovinceid = s.stateprovinceid
    """)
    
    print(f"✨ Transformed {df_transformed.count()} records")
    return df_transformed


def transform_data(df, table_name):
    """
    Apply transformations based on table name
    Add your custom transformations here
    """
    if TRANSFORM_MODE == 'none':
        # No transformation, return as-is
        return df
    
    elif TRANSFORM_MODE == 'custom':
        # Apply custom transformation based on table
        if table_name == 'bussiness_entity_address' or SILVER_TABLE == 'business_entity_address':
            return transform_business_entity_address()
        else:
            # Add more custom transformations here
            print(f"⚠️ No custom transformation defined for {table_name}, loading raw data")
            return df
    
    return df


# ============================================================================
# LOAD FUNCTIONS
# ============================================================================
def load_to_bronze(df):
    """Load data to bronze layer (append mode)"""
    if df.count() == 0:
        print("ℹ️ No new data to load to bronze")
        return
    
    df.write.format("delta").mode("append").saveAsTable(BRONZE_PATH)
    print(f"✅ Appended {df.count()} records to {BRONZE_PATH}")


def load_to_silver(df):
    """Load data to silver layer (append mode)"""
    if df.count() == 0:
        print("ℹ️ No new data to load to silver")
        return
    
    if SILVER_PATH is None:
        print("⚠️ Silver table not specified, skipping silver load")
        return
    
    df.write.format("delta").mode("append").saveAsTable(SILVER_PATH)
    print(f"✅ Appended {df.count()} records to {SILVER_PATH}")


def update_watermark(df, table_name):
    """Update high water mark in control table"""
    if df.count() == 0:
        print("ℹ️ No watermark update needed (no new data)")
        return
    
    print(f"💾 Updating watermark for table '{table_name}'...")
    
    new_hwm = df.select(
        lit(table_name).alias("table_name"),
        spark_max(PRIMARY_KEY).alias("last_id"),
        spark_max(EVENT_COL).alias("last_event_time"),
        current_timestamp().alias("processed_at")
    )

    try:
        existing_hwm = spark.sql(f"SELECT * FROM {HWM_TABLE}")
        updated_hwm = existing_hwm.filter(col("table_name") != table_name).union(new_hwm)
        updated_hwm.write.format("delta").mode("overwrite").saveAsTable(HWM_TABLE)
    except Exception as e:
        print(f"⚠️ Control table might not exist, creating new one. Error: {e}")
        new_hwm.write.format("delta").mode("overwrite").saveAsTable(HWM_TABLE)
    
    print("✅ Watermark updated successfully")


# ============================================================================
# MAIN ETL PIPELINE
# ============================================================================
def run_etl_pipeline():
    """Main ETL pipeline orchestration"""
    print("=" * 80)
    print(f"🚀 Starting Incremental ETL Pipeline for {BRONZE_TABLE}")
    print("=" * 80)
    
    try:
        # STEP 1: EXTRACT
        print("\n[STEP 1] EXTRACT")
        print("-" * 80)
        last_event_time = get_last_watermark(BRONZE_TABLE)
        
        if last_event_time is None:
            df_extracted = extract_full()
        else:
            df_extracted = extract_incremental(last_event_time)
        
        if df_extracted.count() == 0:
            print("\n✅ Pipeline completed: No new data to process")
            return
        
        # STEP 2: LOAD TO BRONZE
        print("\n[STEP 2] LOAD TO BRONZE")
        print("-" * 80)
        load_to_bronze(df_extracted)
        
        # STEP 3: TRANSFORM (if needed)
        print("\n[STEP 3] TRANSFORM")
        print("-" * 80)
        if TRANSFORM_MODE != 'none':
            df_transformed = transform_data(df_extracted, BRONZE_TABLE)
            
            # STEP 4: LOAD TO SILVER
            print("\n[STEP 4] LOAD TO SILVER")
            print("-" * 80)
            load_to_silver(df_transformed)
        else:
            print("⏭️ Skipping transformation (transform_mode=none)")
        
        # STEP 5: UPDATE WATERMARK
        print("\n[STEP 5] UPDATE WATERMARK")
        print("-" * 80)
        update_watermark(df_extracted, BRONZE_TABLE)
        
        print("\n" + "=" * 80)
        print("✅ Pipeline completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print("\n" + "=" * 80)
        print(f"❌ Pipeline failed with error: {e}")
        print("=" * 80)
        raise
    finally:
        spark.stop()


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    run_etl_pipeline()