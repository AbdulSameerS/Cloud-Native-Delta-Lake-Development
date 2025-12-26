-- Redshift SQL Schema for Analytics

-- 1. Create Schema
CREATE SCHEMA IF NOT EXISTS ecommerce_analytics;

-- 2. Create Tables (Gold Layer)
CREATE TABLE IF NOT EXISTS ecommerce_analytics.product_performance (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    total_sales DECIMAL(10, 2),
    average_rating DECIMAL(3, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Example Query for Real-Time Analytics
-- Identify top performing products with low ratings (Action needed)
SELECT 
    product_id, 
    total_sales, 
    average_rating 
FROM 
    ecommerce_analytics.product_performance
WHERE 
    total_sales > 1000 
    AND average_rating < 3.0
ORDER BY 
    total_sales DESC;

-- 4. Example COPY command (If loading from S3 Parquet/CSV exported from Delta)
-- COPY ecommerce_analytics.product_performance
-- FROM 's3://my-gold-bucket/product_stats/'
-- IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole'
-- FORMAT AS PARQUET;
