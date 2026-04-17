--CUSTOMER TABLE

  INSERT INTO silver.crm_cust_info (
cst,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)

SELECT
cst,
cst_key,

TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN	UPPER(TRIM(cst_marital_status))  = 'S' THEN 'Single'
	 WHEN	UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END as cst_marital_status,


CASE WHEN	UPPER(TRIM(cst_gndr))  = 'F' THEN 'Female'
	 WHEN	UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END as cst_gndr,
cst_create_date
FROM (
select 
*,
ROW_NUMBER() over (partition by cst order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst is not null
) t
where flag_last=1


  -- PRODUCTION TABLE

  INSERT INTO silver.crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt

)
SELECT
prd_id,

REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
	WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
	WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
	WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

