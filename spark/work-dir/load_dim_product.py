from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Load Dim Product") \
    .enableHiveSupport() \
    .getOrCreate()

dim_product = spark.sql("""
    SELECT
        p.productid AS productkey,
        p.productnumber AS productalternatekey,
        p.name AS productname,
        p.color,
        p.size,
        p.weight,
        p.standardcost,
        p.listprice,
        ps.name AS productsubcategory,
        pc.name AS productcategory,
        pm.name AS modelname,
        p.sellstartdate,
        p.sellenddate
FROM bronze.product p
LEFT JOIN bronze.subcategory ps 
     ON p.productsubcategoryid = ps.productsubcategoryid
LEFT JOIN bronze.category pc 
     ON ps.productcategoryid = pc.productcategoryid
LEFT JOIN bronze.productmodel pm 
      ON p.productmodelid = pm.productmodelid
""").write.format("delta").mode("append").saveAsTable("gold.dim_product")

spark.stop()