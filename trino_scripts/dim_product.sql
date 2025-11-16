-- SCD Type 2 Dimension Table for Products
-- Tracks historical changes to product attributes
CREATE TABLE IF NOT EXISTS delta.gold.dim_product (
    product_sk INTEGER,                    -- Surrogate key (auto-incremented)
    productkey INTEGER,                    -- Business key (ProductID from source)
    productalternatekey VARCHAR,           -- Product number/alternate key
    productname VARCHAR,                   -- Product name
    categoryname VARCHAR,                  -- Product category name
    subcategoryname VARCHAR,               -- Product subcategory name
    modelname VARCHAR,                     -- Product model name
    color VARCHAR,                         -- Product color
    size VARCHAR,                          -- Product size
    sizeunitmeasurecode VARCHAR,           -- Size unit of measure code
    weight REAL,                           -- Product weight
    weightunitmeasurecode VARCHAR,         -- Weight unit of measure code
    finishedgoodsflag BOOLEAN,             -- Finished goods flag
    safetystocklevel INTEGER,              -- Safety stock level
    reorderpoint INTEGER,                  -- Reorder point
    standardcost REAL,                      -- Standard cost
    listprice REAL,                        -- List price
    daystomanufacture INTEGER,             -- Days to manufacture
    productline VARCHAR,                   -- Product line
    class VARCHAR,                         -- Product class
    style VARCHAR,                         -- Product style
    sellstartdate TIMESTAMP,               -- Sell start date
    sellenddate TIMESTAMP,                 -- Sell end date
    discontinueddate TIMESTAMP,            -- Discontinued date
    attribute_hash VARCHAR,                -- MD5 hash of attributes for change detection
    effective_date DATE,                   -- SCD Type 2: When this version became effective
    expiration_date DATE,                  -- SCD Type 2: When this version expired (NULL for current)
    is_current BOOLEAN,                    -- SCD Type 2: Flag indicating if this is the current version
    created_at TIMESTAMP,                  -- Audit: When record was created
    updated_at TIMESTAMP                   -- Audit: When record was last updated
)
USING DELTA
LOCATION 's3a://lake/gold/dim_product/'


