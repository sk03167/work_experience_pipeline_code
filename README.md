# Enterprise Ratecard Lakehouse Pipeline: OLTP -> ADF -> Databricks (Medallion Star Schema)

## 1. High-Level Architecture Overview

```mermaid
flowchart TD
    subgraph OLTP_Source ["OLTP Database Tier (Neon / PostgreSQL)"]
        OLTP[("Subscription Ratecard OLTP\n(Customers, Products, Subscriptions,\nRatecard Rules with Struct Tiers)")]
    end

    subgraph Azure_Network ["Microsoft Azure Data Platform"]
        ADF["Azure Data Factory\n(Orchestration Engine)"]
        ADLS_Bronze[("ADLS Gen2 Bronze\n(Raw Staged Delta)")]
        ADB["Azure Databricks\n(PySpark Compute Engine)"]
        ADLS_Silver[("ADLS Gen2 Silver\n(Exploded Tiers & Cleansed)")]
        ADLS_Gold[("ADLS Gen2 Gold\n(Star Schema / Conformed Dims)")]

        OLTP -->|"ADF Copy Activity (JDBC / TLS)"| ADLS_Bronze
        ADF -->|"Triggers Copy"| ADLS_Bronze
        ADF -->|"Triggers Pipeline Run"| ADB

        ADB -->|"Read Raw Structs"| ADLS_Bronze
        ADB -->|"Explode Tiers & SCD2"| ADLS_Silver
        ADB -->|"Conformed SK Lookups\n(dim_customer, dim_part)"| ADLS_Gold
    end
```

## 2. Key Engineering Challenges Solved

### A. High-Fidelity Struct Unnesting & Tier Explosion
* **Problem**: In OLTP, pricing tiers are stored as nested arrays of structs (`rate_tier_struct[]`). Directly querying nested arrays in analytical and BI tools (PowerBI, Tableau) degrades performance.
* **Solution**: The Silver layer extracts the rules header and explodes nested tier structs into discrete interval records with surrogate hashing (`SHA-256`), cleanly handling open-ended tiers (`max_usage = 999999999999`).

### B. Conformed Dimensional Modeling
* **Problem**: OLTP stores natural business keys (`customer_id`, `part_number`).
* **Solution**: The Gold transformation performs high-performance lookups against pre-existing enterprise conformed dimensions (`dim_customer`, `dim_part`) to assign integer surrogate keys (`customer_key`, `part_key`), mapping missing or late-arriving records to `-1 (UNKNOWN)`.

### C. Automated ADF Orchestration
* **ADF Pipelines**: Configured with parameterization (`watermark_timestamp`, `environment`), invoking Copy Activity to land raw Delta tables into Bronze ADLS Gen2, followed by triggering Databricks notebook execution for Silver/Gold curation.
