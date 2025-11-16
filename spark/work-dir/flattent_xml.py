from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, from_xml, regexp_replace 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Parse Demographics XML Column from SQL Server") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("""
    select businessentityid, demographics from bronze.person
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
    col("parsed_xml.TotalPurchaseYTD").alias("totalpurchaseYTD"),
    col("parsed_xml.DateFirstPurchase").alias("dateFirstPurchase"),
    col("parsed_xml.BirthDate").alias("birthDate"),
    col("parsed_xml.MaritalStatus").alias("maritalStatus"),
    col("parsed_xml.YearlyIncome").alias("yearlyIncome"),
    col("parsed_xml.Gender").alias("gender"),
    col("parsed_xml.TotalChildren").alias("totalChildren"),
    col("parsed_xml.NumberChildrenAtHome").alias("numberChildrenAtHome"),
    col("parsed_xml.Education").alias("education"),
    col("parsed_xml.Occupation").alias("occupation"),
    col("parsed_xml.HomeOwnerFlag").alias("homeownerflag"),
    col("parsed_xml.NumberCarsOwned").alias("numbercarsowned"),
    col("parsed_xml.CommuteDistance").alias("commutedistance")
).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.person_demographics")

# spark.sql("SHOW CATALOGS").show()

spark.stop()