CREATE TABLE delta.mart.business_sales_summary AS
SELECT 
    f.orderdatekey AS order_date,
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_of_month,
    d.is_weekend,
    d.is_holiday,
    p.product_sk,
    p.productname,
    p.categoryname,
    p.subcategoryname,
    p.modelname,
    a.city,
    a.state_name,
    a.country_name,
    COUNT(f.salesordernumber IS NOT NULL) AS order_count,
    SUM(f.linetotal) AS revenue,
    SUM(f.unitprice) * SUM(f.orderqty) AS revenue_cost, -- ước lượng
    SUM(f.orderqty) AS quantity_sold,
    AVG(f.unitprice) AS avg_unit_price,
    SUM(f.unitpricediscount) * SUM(f.orderqty) AS total_discount
FROM delta.gold.fact_sales f
JOIN delta.gold.dim_date d ON f.orderdatekey = d.date_key
JOIN delta.gold.dim_product p ON f.productid = p.productkey
JOIN delta.gold.dim_address a ON f.shiptoaddressid = a.addresskey
WHERE f.ingestion_date = (SELECT MAX(ingestion_date) FROM delta.gold.fact_sales)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
