# Production Engineering Challenges & Architectural Solutions

**Author:** Shivansh Kumar (Senior Data Engineer)  
**Domain:** Enterprise Cloud Billing & Subscriptions Lakehouse  
**Tech Stack:** Azure Data Factory (ADF), Azure Databricks (PySpark), Azure Data Lake Storage Gen2 (ADLS), PostgreSQL (Neon OLTP), Azure Key Vault, Terraform

---

## Challenge 1: Serialization of OLTP Composite Struct Arrays in Azure Data Factory

### 1. The Context
In Oracle / PostgreSQL transactional billing systems, subscription ratecard line items do not store pricing rules as flat denormalized records. Instead, they utilize composite User-Defined Types (UDTs) or struct arrays (`billing_oltp.rate_tier_struct[]`) to encapsulate multiple pricing tiers (min usage, max usage, unit price, overage price) inside a single row.

### 2. The Problem
When the Azure Data Factory (ADF) Copy Activity attempted to ingest records from the OLTP source using standard relational connectors, the pipeline failed with:
```text
ErrorCode=DataTypeNotSupported
Details: Column: tier_pricing_rules, The data type System.Array is not supported.
Source: Microsoft.DataTransfer.Common.Shared.HybridDeliveryException
```
ADF's native ODBC/.NET data movement engine is strictly tabular and cannot natively serialize custom engine-level composite struct arrays directly into Parquet sinks.

### 3. Architectural Solution
Rather than flattening and multiplying row counts upstream at the OLTP level (which would cause severe database I/O bottlenecks and lock contention), we implemented a **source-side serialization contract**:
- **Source Query Transformation**: The extraction SQL query leverages database-native serialization:
  ```sql
  SELECT 
      pricing_rule_id, 
      customer_id, 
      part_number, 
      subscription_id, 
      subscription_line_id, 
      date_start, 
      date_end, 
      to_json(tier_pricing_rules) AS tier_pricing_rules, 
      created_at, 
      updated_at 
  FROM billing_oltp.subscription_ratecard_rules 
  WHERE updated_at > '@{pipeline().parameters.p_watermark_timestamp}'
  ```
- **Bronze Staging**: ADF treats the serialized struct array as a standard string payload, writing it efficiently into ADLS Gen2 Bronze parquet files without schema corruption.
- **Silver Curation in Databricks**: Downstream PySpark applies explicit schema enforcement using `from_json()` with a strict `TierPricingStructSchema` before running distributed `explode()` operations.

---

## Challenge 2: Principle of Least Privilege & Key Vault Data Plane RBAC Separation

### 1. The Problem
During automated platform provisioning via GitHub Actions CI/CD, the deployment service principal (`sp-platform-infra-dev`) failed when attempting to populate database secrets:
```text
Status=403 Code="ForbiddenByRbac" Message="Caller is not authorized to perform action 'Microsoft.KeyVault/vaults/secrets/setSecret/action'"
```
Even though the Service Principal held the subscription-level `Contributor` role, Azure rejected secret creation.

### 2. Root Cause
In modern Azure RBAC, control plane access (creating a Key Vault instance) is strictly decoupled from data plane access (reading or writing secret values). The built-in `Contributor` role deliberately excludes `Microsoft.KeyVault/vaults/secrets/*` data plane actions.

### 3. Architectural Solution
We enforced strict segregation of duties without resorting to dangerous subscription-wide `Owner` permissions:
- Assigned **`Role Based Access Control Administrator`** (scoped to exclude privileged admin roles) to the platform CI/CD bot.
- Granted **`Key Vault Secrets Officer`** specifically on the Key Vault instance to allow automated CI/CD secret rotation.
- Configured ADF's **System-Assigned Managed Identity (SAMI)** with **`Key Vault Secrets User`**, ensuring the pipeline runtime can read secrets dynamically without human involvement or plaintext credentials.

---

## Challenge 3: Unbounded Upper Tiers in Star Schema Modeling

### 1. The Problem
Enterprise subscription ratecards feature graduating volume discounts where the final tier has no upper limit (e.g., "Tier 3: 5,000 units and above"). In transactional systems, this is represented by open-ended `NULL` values or sentinel numbers (`999999999999.0`). Joining metered usage queries on standard `BETWEEN min_usage AND max_usage` conditions causes query planner inefficiencies and null-handling errors.

### 2. Architectural Solution
- Standardized the sentinel threshold constant `UNBOUNDED_TIER_MAX = 999999999999.0` across PySpark transformations.
- In the Silver tier explosion layer, implemented conditional coalescing:
  ```python
  F.when(F.col("tier.max_usage").isNull(), F.lit(999999999999.0))
   .otherwise(F.col("tier.max_usage"))
  ```
- Generated deterministic surrogate hashes (`SHA-256(pricing_rule_id || tier_number)`) to ensure idempotent outrigger dimension joins in the Gold OLAP layer.

---

## Challenge 4: Unity Catalog External Location Governance vs. Legacy DBFS Mount Access

### 1. The Context
In modern Databricks Unity Catalog deployments, data access to cloud object storage (ADLS Gen2) transitions away from legacy DBFS mounts (`/mnt/...`) and storage keys to identity-based governance using **Storage Credentials** (Azure Managed Identity Access Connectors) and **External Locations**.

### 2. The Problem
When the Databricks notebook job was triggered by the Azure Data Factory orchestrator, reading the Bronze Parquet files landed in ADLS Gen2 threw:
```text
com.databricks.sql.managedcatalog.acl.UnauthorizedAccessException: 
PERMISSION_DENIED: User does not have READ FILES on External Location 'loc_bronze_lakehouse'.
```
When the script attempted a fallback to a legacy DBFS mount path (`/mnt/bronze/subscription_ratecard_rules`), the execution failed with:
```text
com.databricks.backend.daemon.data.client.DbfsUnsupportedOperationSparkException: 
[DBFS_DISABLED] Public DBFS root is disabled. Access is denied on path: /mnt/bronze/subscription_ratecard_rules
SQLSTATE: 56038
```

### 3. Root Cause
1. **Unity Catalog Authorization Model**: Creating an External Location (`CREATE EXTERNAL LOCATION loc_bronze_lakehouse...`) registers the object in the Unity Catalog metastore, but assigns privileges **strictly to the creating principal/metastore admin**. Automated jobs running under distinct pipeline service principals or workspace identities have zero default permissions on external locations.
2. **Zero-Trust Hardening (`[DBFS_DISABLED]`)**: Enterprise-grade Lakehouses deliberately enforce `[DBFS_DISABLED]`, completely deprecating root DBFS writes and unmanaged mounts to prevent credential leakage and untracked data sprawl.

### 4. Architectural Solution
Rather than re-enabling legacy DBFS or weakening storage firewalls:
- **Explicit Unity Catalog RBAC Grants**: Configured granular access control directly on the Unity Catalog External Locations, allowing automated pipeline identities and authorized user groups to read/write without needing storage account keys:
  ```sql
  GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_bronze_lakehouse TO `account users`;
  GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_silver_lakehouse TO `account users`;
  GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_gold_lakehouse TO `account users`;
  ```
- **Storage Credential Decoupling**: Storage access is securely delegated through the Azure Databricks Access Connector Managed Identity (`access-connector-databricks-dev`), ensuring no storage access keys or tokens reside in application code or notebook environments.
- **Fail-Fast Direct ABFSS Paths**: Standardized all Databricks PySpark pipelines to read directly from governed ABFSS URIs (`abfss://<container>@<storage_account>.dfs.core.windows.net/<path>`), eliminating deprecated mount dependencies.


