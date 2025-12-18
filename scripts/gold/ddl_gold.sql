/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/
--CREATION OF CUSTOMER DIMENSION TABLE (VIEW)
IF OBJECT_ID('Gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW Gold.dim_customers;
GO
CREATE VIEW Gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- TO CREATE CONNECTIONS WITH TABLES(SURROGATE KEY)
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.CNTRY AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
		 ELSE COALESCE(ca.GEN,'Unknown')
	END AS gender, -- TO DISCARD THE UNWANTED VALUES
	ca.BDATE AS birthdate,
	ci.cst_create_date AS create_date
FROM Silver.crm_cust_info ci
LEFT JOIN Silver.erp_CUST_AZ12 ca
ON ci.cst_key = ca.CID
LEFT JOIN Silver.erp_LOC_A101 la
ON ci.cst_key = la.CID
GO


--CREATION OF PRODUCT DIMENSION TABLE (VIEW)
IF OBJECT_ID('Gold.dim_products', 'V') IS NOT NULL
	DROP VIEW Gold.dim_products;
GO
CREATE VIEW Gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY prd_start_dt,prd_key) AS product_key, -- TO CREATE CONNECTIONS WITH TABLES(SURROGATE KEY)
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.CAT AS category,
	pc.SUBCAT AS subcategory,
	pc.MAINTENANCE AS maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM Silver.crm_prd_info pn
LEFT JOIN Silver.erp_PX_CAT_G1V2 pc
ON pn.cat_id = pc.ID
WHERE prd_end_dt IS NULL -- TO FILTER THE HISTORICAL DATA AND GET THE CURRENT VALUES
GO

--CREATION OF SALES FACT TABLE (VIEW)
IF OBJECT_ID('Gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW Gold.fact_sales;
GO
CREATE VIEW Gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cr.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM Silver.crm_sales_detail sd
LEFT JOIN Gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN Gold.dim_customers cr
ON sd.sls_cust_id = cr.customer_id
GO
