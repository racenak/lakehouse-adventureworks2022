from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Load Dim Customer") \
    .enableHiveSupport() \
    .getOrCreate()

df_customer = spark.sql("""
    select 
	cus.customerid as customerkey,
	cus.accountnumber,
	p.title,
	p.firstname ,
	p.middlename ,
	p.lastname ,
	phone.phonenumber ,
	email.emailaddress ,
	bea.addressline1 ,
	bea.addressline2 ,
	bea.city ,
	bea.stateprovincecode ,
	bea.countryregioncode ,
	bea.name as statename,
	bea.postalcode ,
	pd.totalpurchaseytd,
	pd.datefirstpurchase ,
	pd.birthdate ,
	pd.maritalstatus ,
	pd.yearlyincome ,
	pd.gender ,
	pd.totalchildren ,
	pd.numberchildrenathome ,
	pd.education ,
	pd.occupation ,
	pd.homeownerflag ,
	pd.numbercarsowned ,
	pd.commutedistance 
from bronze.person p
join bronze.phone phone
on p.businessentityid = phone.businessentityid 
join bronze.email email
on p.businessentityid = email.businessentityid
join silver.person_demographics pd
on p.businessentityid = pd.businessentityid 
join silver.business_entity_address bea
on p.businessentityid = bea.businessentityid 
join bronze.customer cus
on p.businessentityid = cus.personid 
where p.persontype = 'IN'

""").write.format("delta").mode("append").saveAsTable("gold.dim_customer")

spark.stop()