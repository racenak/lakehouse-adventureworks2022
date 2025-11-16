from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, from_xml, regexp_replace 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Parse Demographics XML Column from SQL Server") \
    .getOrCreate()

df = spark.read.format("delta").load("s3a://lake//bronze/person")
df = df.select("businessentityid", "demographics").filter(col("persontype") == "IN")

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

df_parsed.select("businessentityid", 
                 "parsed_xml.TotalPurchaseYTD", 
                 "parsed_xml.DateFirstPurchase",
                "parsed_xml.BirthDate",
                "parsed_xml.MaritalStatus",
                "parsed_xml.YearlyIncome",
                "parsed_xml.Gender",
                "parsed_xml.TotalChildren",
                "parsed_xml.NumberChildrenAtHome",
                "parsed_xml.Education",
                "parsed_xml.Occupation",
                "parsed_xml.HomeOwnerFlag",
                "parsed_xml.NumberCarsOwned",
                "parsed_xml.CommuteDistance"
).show()

spark.stop()