CREATE TABLE delta.mart.business_top_products AS
SELECT 
    p.product_sk,
    p.productname,
    p.categoryname,
    p.subcategoryname,
    SUM(f.linetotal) AS total_revenue,
    SUM(f.orderqty) AS total_qty,
    COUNT(DISTINCT f.salesordernumber) AS order_count
FROM delta.gold.fact_sales f
JOIN delta.gold.dim_product p ON f.productid = p.productkey
GROUP BY 1,2,3,4
ORDER BY total_revenue DESC
LIMIT 10;