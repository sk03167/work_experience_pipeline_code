-- ==============================================================================
-- Schema Initialization for Neon PostgreSQL OLTP
-- Multi-Schema Architecture:
--   1. master_data  -> Customer & Product catalog (Conformed Source)
--   2. billing_oltp -> Subscriptions & Ratecard Rules (Transactional Engine)
-- ==============================================================================

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS master_data;
CREATE SCHEMA IF NOT EXISTS billing_oltp;

-- Clean up existing structures if needed
DROP TABLE IF EXISTS billing_oltp.subscription_ratecard_rules CASCADE;
DROP TABLE IF EXISTS billing_oltp.subscriptions CASCADE;
DROP TABLE IF EXISTS master_data.customers CASCADE;
DROP TABLE IF EXISTS master_data.products CASCADE;
DROP TYPE IF EXISTS billing_oltp.rate_tier_struct CASCADE;

-- ------------------------------------------------------------------------------
-- 1. Schema: master_data
-- ------------------------------------------------------------------------------

-- Master Customers
CREATE TABLE master_data.customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    customer_name VARCHAR(128) NOT NULL,
    country_code VARCHAR(3) NOT NULL,
    industry_segment VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Master Products / Parts Catalog
CREATE TABLE master_data.products (
    part_number VARCHAR(32) PRIMARY KEY,
    product_name VARCHAR(128) NOT NULL,
    product_family VARCHAR(64) NOT NULL,
    unit_of_measure VARCHAR(32) NOT NULL, -- e.g. OCPU-Hour, GB-Month, Requests-Million
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------------------------
-- 2. Schema: billing_oltp
-- ------------------------------------------------------------------------------

-- Composite Type for Tiered Pricing Struct
CREATE TYPE billing_oltp.rate_tier_struct AS (
    tier_number INT,
    min_usage NUMERIC(18, 4),
    max_usage NUMERIC(18, 4),        -- 999999999999.0000 denotes unbounded upper tier
    unit_price NUMERIC(18, 6),
    overage_price NUMERIC(18, 6),
    is_overage_applicable BOOLEAN
);

-- Subscriptions Header Table
CREATE TABLE billing_oltp.subscriptions (
    subscription_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL REFERENCES master_data.customers(customer_id),
    status VARCHAR(20) NOT NULL, -- ACTIVE, EXPIRED, TERMINATED
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    currency_code VARCHAR(3) DEFAULT 'USD',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Subscription Ratecard Rules (The Core Massive Ingestion Target)
CREATE TABLE billing_oltp.subscription_ratecard_rules (
    pricing_rule_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL REFERENCES master_data.customers(customer_id),
    part_number VARCHAR(32) NOT NULL REFERENCES master_data.products(part_number),
    subscription_id VARCHAR(32) NOT NULL REFERENCES billing_oltp.subscriptions(subscription_id),
    subscription_line_id VARCHAR(32) NOT NULL,
    date_start DATE NOT NULL,
    date_end DATE NOT NULL,
    tier_pricing_rules billing_oltp.rate_tier_struct[] NOT NULL, -- Struct Array!
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Production Indexes for Ingestion, Watermark Scans, and Joins
CREATE INDEX idx_ratecard_updated_at ON billing_oltp.subscription_ratecard_rules(updated_at);
CREATE INDEX idx_ratecard_sub_line ON billing_oltp.subscription_ratecard_rules(subscription_id, subscription_line_id);
CREATE INDEX idx_ratecard_customer ON billing_oltp.subscription_ratecard_rules(customer_id);
CREATE INDEX idx_ratecard_part ON billing_oltp.subscription_ratecard_rules(part_number);
