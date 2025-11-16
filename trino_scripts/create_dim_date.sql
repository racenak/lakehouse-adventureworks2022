CREATE TABLE IF NOT EXISTS delta.gold.dim_date (
    date_key INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    month_abbr VARCHAR(3),
    week_of_year INT,
    day_of_month INT,
    day_of_week INT,
    day_name VARCHAR(20),
    day_abbr VARCHAR(3),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_month INT
)
WITH (
    location = 's3a://lake/gold/dim_date/'
);

INSERT INTO delta.gold.dim_date
SELECT
    CAST(date_format(full_date, '%Y%m%d') AS INT) AS date_key,
    full_date,
    year(full_date) AS year,
    quarter(full_date) AS quarter,
    month(full_date) AS month,
    date_format(full_date, '%B') AS month_name,
    date_format(full_date, '%b') AS month_abbr,
    week_of_year(full_date) AS week_of_year,
    day(full_date) AS day_of_month,
    day_of_week(full_date) AS day_of_week,
    date_format(full_date, '%A') AS day_name,
    date_format(full_date, '%a') AS day_abbr,
    CASE WHEN day_of_week(full_date) IN (6, 7) THEN true ELSE false END AS is_weekend,
    false AS is_holiday,
    -- Fiscal year logic (assuming fiscal year starts in July)
    CASE 
        WHEN month(full_date) >= 7 THEN year(full_date) + 1
        ELSE year(full_date)
    END AS fiscal_year,
    CASE 
        WHEN month(full_date) IN (7, 8, 9) THEN 1
        WHEN month(full_date) IN (10, 11, 12) THEN 2
        WHEN month(full_date) IN (1, 2, 3) THEN 3
        ELSE 4
    END AS fiscal_quarter,
    CASE 
        WHEN month(full_date) >= 7 THEN month(full_date) - 6
        ELSE month(full_date) + 6
    END AS fiscal_month
FROM (
    SELECT DATE '2010-01-01' + INTERVAL '1' DAY * day_offset AS full_date
    FROM UNNEST(sequence(0, 7304)) AS t(day_offset)  -- 7304 days = ~20 years (2010-2030)
) dates;