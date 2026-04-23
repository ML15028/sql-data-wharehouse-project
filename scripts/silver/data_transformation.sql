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


--SALES DETAILS TABLE


INSERT INTO silver.crm_sales_details(
[sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
 )


SELECT 
       [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,case when [sls_order_dt] = 0 or len(sls_order_dt)!=8 then null
        else cast(cast(sls_order_dt as varchar) as date)
        end as sls_order_dt
      ,case when [sls_ship_dt] = 0 or len([sls_ship_dt])!=8 then null
        else cast(cast([sls_ship_dt] as varchar) as date)
        end as [sls_ship_dt]
      ,case when [sls_due_dt] = 0 or len([sls_due_dt])!=8 then null
  else cast(cast([sls_due_dt] as varchar) as date)
  end as [sls_due_dt]
      ,case when [sls_sales] is null or [sls_sales]<=0 or [sls_sales]!= sls_quantity*abs(sls_price)
       then sls_quantity*abs(sls_price)
       else [sls_sales]
       end as [sls_sales]
      ,[sls_quantity]
      ,case when [sls_price] is null or [sls_price]<=0
        then [sls_sales]/nullif(sls_quantity,0)
        else [sls_price]
        end AS [sls_price]
  FROM [bronze].[crm_sales_details]

-- CUSTOMER AZ12 TABLE


INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)


SELECT  case when [cid] like 'NAS%' then SUBSTRING (cid,4,len(cid))
        else cid 
       end as cid
      ,case when bdate >getdate() then null
      else bdate
      end as bdate
      ,case when  upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
            when  upper(trim(gen)) in ('M','MALE') THEN 'Male'
        else 'n/a'
        end as gen
  FROM [DataWarehouse].[bronze].[erp_cust_az12]
  
  



