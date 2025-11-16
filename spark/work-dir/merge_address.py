from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

spark = SparkSession.builder \
    .appName("Merge Phone and Email - Incremental") \
    .enableHiveSupport() \
    .getOrCreate()

# Danh sách bảng Bronze cần đọc
BRONZE_TABLES = [
    "bronze.business_entity_address",
    "bronze.address",
    "bronze.state_province"
]

HWM_TABLE = "controll.high_water_mark"

# === 1. Lấy last_ingestion_date mới nhất từ tất cả các bảng Bronze ===
watermarks = spark.sql(f"""
    SELECT table_name, last_ingestion_date
    FROM {HWM_TABLE}
    WHERE table_name IN ('business_entity_address', 'address', 'state_province')
""")

if watermarks.count() == 0:
    print("No watermark found. Running full load...")
    last_ingestion_date = None
else:
    last_ingestion_date = watermarks.select(spark_max("last_ingestion_date")).collect()[0][0]
    print(f"Incremental load since ingestion_date > {last_ingestion_date}")

# === 2. Đọc dữ liệu từ Delta (lọc theo ingestion_date nếu cần) ===
condition = ""
if last_ingestion_date is not None:
    condition = f"WHERE bea.ingestion_date = DATE '{last_ingestion_date}'"

# === 3. Transform bằng SQL query có join và filter ingestion_date đồng bộ ===
query = f"""
SELECT
    bea.businessentityid,
    a.addressline1,
    a.addressline2,
    a.city,
    s.stateprovincecode,
    s.countryregioncode,
    s.name AS state_name,
    a.postalcode,
    bea.ingestion_date
FROM bronze.bussiness_entity_address AS bea
INNER JOIN bronze.address AS a
    ON bea.addressid = a.addressid
    AND bea.ingestion_date = a.ingestion_date
INNER JOIN bronze.state_province AS s
    ON a.stateprovinceid = s.stateprovinceid
    AND a.ingestion_date = s.ingestion_date
{condition}
"""

df_silver = spark.sql(query)

# === 4. Ghi vào Silver (append) ===
if df_silver.count() > 0:
    df_silver.write.format("delta") \
        .mode("append") \
        .saveAsTable("silver.business_entity_address")
    print(f"✅ Appended {df_silver.count()} new records to silver.business_entity_address")
else:
    print("ℹ️ No new data to transform.")

spark.stop()
