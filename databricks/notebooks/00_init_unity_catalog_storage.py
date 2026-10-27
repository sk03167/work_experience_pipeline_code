# Databricks notebook source
# MAGIC %md
# MAGIC # 00_init_unity_catalog_storage: Register ADLS Gen2 Storage Credential & External Locations
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Layer:** Unity Catalog Governance / Storage Access  
# MAGIC
# MAGIC ### Overview:
# MAGIC Links the Azure Access Connector Managed Identity provisioned via Terraform into Unity Catalog:
# MAGIC 1. Registers `cred_adls_lakehouse_dev` using Azure Managed Identity.
# MAGIC 2. Registers External Locations for `bronze`, `silver`, and `gold` ADLS Gen2 containers.
# MAGIC 3. Grants passwordless, native high-throughput access to the Databricks Lakehouse engine.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1: Register Storage Credential (Azure Managed Identity)
# MAGIC *Uses the Access Connector Resource ID provisioned in Azure resource group `rg-ratecard-lakehouse-dev`.*

# COMMAND ----------

# DBTITLE 1,Create storage credential
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import AzureManagedIdentityRequest

w = WorkspaceClient()
credential_name = "cred_adls_lakehouse_dev"
access_connector_id = "/subscriptions/09af7dc6-457c-4086-be07-9669631f8f04/resourceGroups/rg-ratecard-lakehouse-dev/providers/Microsoft.Databricks/accessConnectors/access-connector-databricks-dev"

try:
    cred = w.storage_credentials.get(name=credential_name)
except NotFound:
    cred = w.storage_credentials.create(
        name=credential_name,
        azure_managed_identity=AzureManagedIdentityRequest(
            access_connector_id=access_connector_id
        ),
        comment="Managed identity storage credential for ADLS Gen2 lakehouse dev environment",
    )

display(cred.as_dict())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Storage Credential
# MAGIC DESCRIBE STORAGE CREDENTIAL cred_adls_lakehouse_dev;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2: Register External Locations (Bronze, Silver, Gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Bronze Staging Location
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS loc_bronze_lakehouse
# MAGIC URL 'abfss://bronze@stlakehousedevsk01.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL cred_adls_lakehouse_dev)
# MAGIC COMMENT 'External location for Bronze raw landing zone';
# MAGIC
# MAGIC -- 2. Silver Curated Location
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS loc_silver_lakehouse
# MAGIC URL 'abfss://silver@stlakehousedevsk01.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL cred_adls_lakehouse_dev)
# MAGIC COMMENT 'External location for Silver cleansed and exploded tier rules';
# MAGIC
# MAGIC -- 3. Gold Dimensional Serving Location
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS loc_gold_lakehouse
# MAGIC URL 'abfss://gold@stlakehousedevsk01.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL cred_adls_lakehouse_dev)
# MAGIC COMMENT 'External location for Gold star schema dimensional tables';
# MAGIC
# MAGIC -- 4. Grant Permissions to Workspace Users & Automated Pipelines
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_bronze_lakehouse TO `account users`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_silver_lakehouse TO `account users`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_gold_lakehouse TO `account users`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3: Validate Direct Lakehouse Access
# MAGIC *Test listing files from the Bronze container landed by Azure Data Factory.*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List files in Bronze staging path landed by ADF
# MAGIC LIST 'abfss://bronze@stlakehousedevsk01.dfs.core.windows.net/subscription_ratecard_rules';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_bronze_lakehouse TO `account users`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_silver_lakehouse TO `account users`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_gold_lakehouse TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW Groups