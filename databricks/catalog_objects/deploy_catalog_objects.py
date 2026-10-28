# Databricks notebook source
# MAGIC %md
# MAGIC # deploy_catalog_objects: Dynamic Schema & RBAC Migration Runner
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Pattern:** Single Source of Truth (SSOT) DDL Execution  
# MAGIC
# MAGIC ### Mechanism:
# MAGIC 1. Dynamically scans `.sql` files in `table_definitions/` and `permissions/`.
# MAGIC 2. Interpolates runtime target catalog and schema parameters (`${catalog_name}`, `${schema_name}`).
# MAGIC 3. Parses and executes DDL and RBAC statements in sorted order.
# MAGIC 4. Provides granular audit logs of all provisioned lakehouse catalog objects.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1: Environment & Catalog Parameters

# COMMAND ----------

import os
import re

# Parameter widgets for multi-environment promotion (dev/qa/prod)
try:
    dbutils.widgets.text("catalog_name", "adb_ratecard_dev_001", "Target Catalog")
    dbutils.widgets.text("schema_name", "default", "Target Schema")
    dbutils.widgets.text("env", "dev", "Environment")
    
    catalog_name = dbutils.widgets.get("catalog_name")
    schema_name = dbutils.widgets.get("schema_name")
    env = dbutils.widgets.get("env")
except Exception:
    catalog_name = "adb_ratecard_dev_001"
    schema_name = "default"
    env = "dev"

print(f"=== Starting Lakehouse Schema Deployment ===")
print(f"Environment : {env}")
print(f"Catalog     : {catalog_name}")
print(f"Schema      : {schema_name}")

# Activate target catalog and schema context
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2: SQL File Loader & Execution Helper

# COMMAND ----------

def find_catalog_dirs():
    """Locate table_definitions and permissions folders reliably in Databricks Repos."""
    possible_roots = [
        # Relative to notebook location in Repos
        os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else None,
        os.getcwd(),
        "/Workspace/Repos/dev/work_experience_pipeline_code/ratecard_pipeline/databricks/catalog_objects",
        os.path.join(os.getcwd(), "ratecard_pipeline", "databricks", "catalog_objects"),
        os.path.join(os.getcwd(), "databricks", "catalog_objects")
    ]
    
    for root in possible_roots:
        if root and os.path.exists(os.path.join(root, "table_definitions")):
            return root
            
    raise FileNotFoundError("Could not locate 'table_definitions' directory. Please check workspace path.")

base_path = find_catalog_dirs()
table_defs_path = os.path.join(base_path, "table_definitions")
permissions_path = os.path.join(base_path, "permissions")

print(f"Resolved DDL Base Path: {base_path}")

def execute_sql_file(file_path, substitutions):
    """Reads a .sql file, performs variable interpolation, and executes statements."""
    filename = os.path.basename(file_path)
    print(f"\n---> Executing: {filename}")
    
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Interpolate parameters (${catalog_name}, ${schema_name})
    for var_name, var_value in substitutions.items():
        content = content.replace(f"${{{var_name}}}", var_value)
    
    # Remove full-line comments and split into individual statements by semicolon
    statements = [stmt.strip() for stmt in content.split(";") if stmt.strip()]
    
    executed_count = 0
    for stmt in statements:
        # Skip pure comment blocks
        lines = [line.strip() for line in stmt.split("\n") if line.strip() and not line.strip().startswith("--")]
        if not lines:
            continue
        
        cleaned_stmt = "\n".join(lines)
        first_line = lines[0][:80]
        try:
            spark.sql(cleaned_stmt)
            print(f"  [OK] {first_line}...")
            executed_count += 1
        except Exception as e:
            print(f"  [FAIL] Failed executing: {first_line}")
            print(f"  Error details: {e}")
            raise e
            
    print(f"Completed {filename} ({executed_count} statements executed successfully).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3: Execute Table Definitions (Silver & Gold)

# COMMAND ----------

substitutions = {
    "catalog_name": catalog_name,
    "schema_name": schema_name,
    "env": env
}

# 1. Run all table definition scripts in sorted order (01_silver, 02_gold, etc.)
table_sql_files = sorted([
    os.path.join(table_defs_path, f) 
    for f in os.listdir(table_defs_path) 
    if f.endswith(".sql")
])

for sql_file in table_sql_files:
    execute_sql_file(sql_file, substitutions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 4: Execute RBAC Governance Grants

# COMMAND ----------

# 2. Run all permission scripts
permission_sql_files = sorted([
    os.path.join(permissions_path, f) 
    for f in os.listdir(permissions_path) 
    if f.endswith(".sql")
])

for sql_file in permission_sql_files:
    execute_sql_file(sql_file, substitutions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 5: Deployment Verification

# COMMAND ----------

# Verify active catalog tables
df_tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")
display(df_tables)
print("\n=== Lakehouse Catalog Schema Deployment Complete ===")

# Verify active catalog tables
df_tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")
display(df_tables)
print("\n=== Lakehouse Catalog Schema Deployment Complete ===")

