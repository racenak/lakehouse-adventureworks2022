from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark Session (Configuration loaded via spark-submit)
spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()

result = spark.sql("""
    select * from silver.sales
""")

if result.count() > 0 and result.collect()[0][0] is not None:
    print(result.collect()[0][0])

spark.stop()