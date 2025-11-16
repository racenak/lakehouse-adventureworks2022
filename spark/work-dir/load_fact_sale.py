from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Load Fact Sale") \
    .enableHiveSupport() \
    .getOrCreate()

df_sale = spark.sql("""
    SELECT
    soh.salesordernumber,
    CAST(date_format(soh.orderdate, 'yyyyMMdd') AS INTEGER) AS orderdatekey,
    CAST(date_format(soh.duedate, 'yyyyMMdd') AS INTEGER) AS duedatekey,
    CAST(date_format(soh.shipdate, 'yyyyMMdd') AS INTEGER) AS shipdatekey,
    soh.territoryid AS territorykey,
    soh.salespersonid AS salespersonkey,
    soh.customerid as customerkey,
    sod.productid as productkey,
    sod.orderqty,
    sod.unitprice,
    (sod.unitpricediscount * sod.unitprice * sod.orderqty) AS discountamount,
    soh.taxamt,
    soh.freight,
    ((sod.unitprice * sod.orderqty) - (sod.unitprice * sod.unitpricediscount * sod.orderqty)) AS extendedamount
FROM bronze.sales_order_header AS soh
JOIN bronze.sales_order_detail AS sod 
    ON soh.salesorderid = sod.salesorderid;
""").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_sale")

spark.stop()