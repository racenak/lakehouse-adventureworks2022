from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date
from delta.tables import DeltaTable

# === 1. Khởi tạo Spark ===
spark = SparkSession.builder \
    .appName("Silver to Gold - dim_address SCD1") \
    .enableHiveSupport() \
    .getOrCreate()

# === 2. Đọc dữ liệu MỚI NHẤT từ Silver ===
# Giả sử silver có cột ingestion_date (từ pipeline trước)
df_silver = spark.table("silver.business_entity_address") \
    .withColumn("ingestion_date", to_date(col("ingestion_date")))  # đảm bảo kiểu DATE

# Lấy bản ghi MỚI NHẤT cho mỗi businessentityid
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, max as spark_max

window = Window.partitionBy("businessentityid").orderBy(col("ingestion_date").desc())

df_latest = df_silver \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .select(
        col("businessentityid").cast("int").alias("address_sk"),  # surrogate key
        col("addressline1"),
        col("addressline2"),
        col("city"),
        col("stateprovincecode").alias("state_code"),
        col("countryregioncode").alias("country_code"),
        col("state_name"),
        col("postalcode"),
        current_timestamp().alias("modified_date")
    )

# === 3. Ghi vào Gold: dim_address (SCD Type 1 - MERGE) ===
GOLD_TABLE = "gold.dim_address"

# Tạo bảng Gold nếu chưa tồn tại
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    address_sk INT,
    addressline1 STRING,
    addressline2 STRING,
    city STRING,
    state_code STRING,
    country_code STRING,
    state_name STRING,
    postalcode STRING,
    modified_date TIMESTAMP
) USING DELTA
PARTITIONED BY (country_code)
""")

# === 4. MERGE: Cập nhật nếu tồn tại, thêm nếu chưa có ===
gold_table = DeltaTable.forName(spark, GOLD_TABLE)

gold_table.alias("target") \
    .merge(
        df_latest.alias("source"),
        "target.address_sk = source.address_sk"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# === 5. In kết quả ===
updated_count = df_latest.count()
print(f"MERGED {updated_count} latest records into {GOLD_TABLE} (SCD Type 1)")

# === 6. Dừng Spark ===
spark.stop()