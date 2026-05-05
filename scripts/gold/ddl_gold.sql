-- =========================================================
-- GOLD LAYER - CREATE VIEWS
-- This layer is designed for analytical consumption (BI / Reporting)
-- It follows a STAR SCHEMA model (Dimensions + Fact Table)
-- Source data comes from SILVER layer
-- =========================================================


-- =========================================================
-- DIMENSION: CUSTOMERS
-- Description: Contains master customer data combining CRM and ERP sources
-- =========================================================

CREATE VIEW gold.dim_customers AS
SELECT 
    -- Surrogate key generated for dimensional modeling
    ROW_NUMBER() OVER (ORDER BY ci.cst) AS customer_key,

    -- Business keys
    ci.cst AS customer_id,
    ci.cst_key AS customer_number,

    -- Customer attributes
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,

    -- Gender logic:
    -- Priority: CRM value if valid, otherwise ERP value, else 'n/a'
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

    -- Additional attributes from ERP
    ca.bdate AS birthday_date,

    -- Record creation date from CRM
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci

-- Join with ERP customer table to enrich demographic data
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid

-- Join with ERP location table to get country information
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;



-- =========================================================
-- DIMENSION: PRODUCTS
-- Description: Contains product master data and category information
-- =========================================================

CREATE VIEW gold.dim_products AS
SELECT
    -- Surrogate key for product dimension
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

    -- Business keys
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,

    -- Product attributes
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,

    -- Category enrichment from ERP
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,

    -- Financial / classification attributes
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,

    -- Product lifecycle
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn

-- Join with ERP category table
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id

-- Filter: Only active products (exclude historical records)
WHERE pn.prd_end_dt IS NULL;



-- =========================================================
-- FACT TABLE: SALES
-- Description: Contains transactional sales data
-- Linked to customers and products dimensions
-- =========================================================

CREATE VIEW gold.fact_sales AS
SELECT 
    -- Transaction identifier
    sd.sls_ord_num AS order_number,

    -- Foreign keys to dimensions
    pr.product_key AS product_key,
    cu.customer_key AS customer_key,

    -- Important business dates
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,

    -- Measures / metrics
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price

FROM silver.crm_sales_details sd

-- Join with product dimension using business key
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number

-- Join with customer dimension using business key
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
