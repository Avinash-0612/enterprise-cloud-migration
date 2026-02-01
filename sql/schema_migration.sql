-- ============================================================================
-- SCHEMA MIGRATION SCRIPTS
-- On-Prem SQL Server -> Azure Synapse Analytics Dedicated SQL Pool
-- Author: Avinash Chinnabattuni
-- Date: 2024
-- ============================================================================

-- ============================================================================
-- 1. DIMENSION TABLES (Star Schema)
-- ============================================================================

-- Drop and create schema
DROP SCHEMA IF EXISTS gold;
GO
CREATE SCHEMA gold;
GO

-- DimCustomer (Type 2 SCD - Slowly Changing Dimension)
CREATE TABLE gold.dim_customer (
    customer_sk BIGINT IDENTITY(1,1) NOT NULL,  -- Surrogate key
    customer_nk INT NOT NULL,                    -- Natural key (source ID)
    customer_name NVARCHAR(100) NOT NULL,
    email NVARCHAR(255),
    phone NVARCHAR(20),
    address NVARCHAR(500),
    city NVARCHAR(100),
    state NVARCHAR(50),
    country NVARCHAR(50),
    postal_code NVARCHAR(20),
    
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    expiration_date DATE NULL,
    is_current BIT DEFAULT 1,
    
    -- Metadata
    _source_system NVARCHAR(50) DEFAULT 'legacy_sql',
    _ingestion_timestamp DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT pk_dim_customer PRIMARY KEY NONCLUSTERED (customer_sk)
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED INDEX (customer_nk));

-- DimProduct
CREATE TABLE gold.dim_product (
    product_sk BIGINT IDENTITY(1,1) NOT NULL,
    product_nk INT NOT NULL,
    product_name NVARCHAR(200) NOT NULL,
    category NVARCHAR(100),
    subcategory NVARCHAR(100),
    brand NVARCHAR(100),
    unit_cost DECIMAL(18,2),
    unit_price DECIMAL(18,2),
    supplier_name NVARCHAR(100),
    
    -- Metadata
    _source_system NVARCHAR(50) DEFAULT 'legacy_sql',
    _ingestion_timestamp DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT pk_dim_product PRIMARY KEY NONCLUSTERED (product_sk)
)
WITH (DISTRIBUTION = REPLICATE);

-- DimDate (Standard date dimension)
CREATE TABLE gold.dim_date (
    date_sk INT NOT NULL,          -- YYYYMMDD format
    full_date DATE NOT NULL,
    day_of_week TINYINT,
    day_name NVARCHAR(10),
    day_of_month TINYINT,
    day_of_year SMALLINT,
    week_of_year TINYINT,
    month_number TINYINT,
    month_name NVARCHAR(10),
    quarter TINYINT,
    year_number SMALLINT,
    fiscal_quarter TINYINT,
    
    CONSTRAINT pk_dim_date PRIMARY KEY NONCLUSTERED (date_sk)
)
WITH (DISTRIBUTION = REPLICATE);

-- ============================================================================
-- 2. FACT TABLES
-- ============================================================================

-- FactSales (CURRENCY optimized for frequent access)
CREATE TABLE gold.fact_sales (
    sales_sk BIGINT IDENTITY(1,1),
    
    -- Foreign Keys
    customer_sk BIGINT NOT NULL,
    product_sk BIGINT NOT NULL,
    date_sk INT NOT NULL,
    
    -- Degenerate dimensions
    sales_order_nk NVARCHAR(50),
    line_number INT,
    
    -- Measures
    quantity INT,
    unit_price DECIMAL(18,2),
    unit_cost DECIMAL(18,2),
    sales_amount DECIMAL(18,2),
    cost_amount DECIMAL(18,2),
    profit_amount DECIMAL(18,2),
    profit_margin_pct DECIMAL(5,2),
    
    -- Metadata
    _source_system NVARCHAR(50) DEFAULT 'legacy_sql',
    _ingestion_timestamp DATETIME2 DEFAULT GETDATE(),
    _partition_key INT          -- For partition switching (YYYYMMDD)
)
WITH (
    DISTRIBUTION = HASH(customer_sk),  -- Optimize for customer queries
    CLUSTERED COLUMNSTORE INDEX,        -- Compression & analytics optimized
    PARTITION (_partition_key RANGE RIGHT FOR VALUES 
        (20230101, 20230201, 20230301)) -- Monthly partitions
);

-- ============================================================================
-- 3. INDEXING & PERFORMANCE OPTIMIZATION
-- ============================================================================

-- Non-clustered indexes for common query patterns
CREATE STATISTICS stat_fact_sales_customer ON gold.fact_sales (customer_sk);
CREATE STATISTICS stat_fact_sales_date ON gold.fact_sales (date_sk);
CREATE STATISTICS stat_fact_sales_product ON gold.fact_sales (product_sk);

-- ============================================================================
-- 4. SECURITY (Row-Level Security) - Example
-- ============================================================================

-- Security predicate function for multi-tenant access
CREATE SCHEMA security;
GO

CREATE FUNCTION security.fn_security_predicate(@customer_sk BIGINT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS access_granted
WHERE 
    -- Allow access if user is admin
    IS_MEMBER('db_owner') = 1
    
    -- OR if user owns this customer data
    OR EXISTS (
        SELECT 1 FROM gold.dim_customer c
        WHERE c.customer_sk = @customer_sk
        AND c.country = USER_NAME()  -- Simplified example
    );
GO

-- Apply RLS policy
CREATE SECURITY POLICY gold.customer_filter_policy
ADD FILTER PREDICATE security.fn_security_predicate(customer_sk)
ON gold.fact_sales
WITH (STATE = ON, SCHEMABINDING = ON);
