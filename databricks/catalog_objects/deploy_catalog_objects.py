# Databricks notebook source
# MAGIC %md
# MAGIC # deploy_catalog_objects: Hierarchical Lakehouse Schema & RBAC Migration Runner
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Pattern:** Declarative Infrastructure & Schema as Code (Catalogs -> Schemas -> Tables -> Permissions)  
# MAGIC
# MAGIC ### Deployment Lifecycle:
# MAGIC 1. **Catalogs:** `catalog_objects/catalogs/*.sql`
# MAGIC 2. **Schemas:** `catalog_objects/schemas/*.sql`
# MAGIC 3. **Tables:** `catalog_objects/tables/*.sql`
# MAGIC 4. **Permissions:** `catalog_objects/permissions/*.sql`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1: Environment & Catalog Parameters

# COMMAND ----------

import os
import re

# Parameter widgets for multi-environment promotion (dev/qa/prod)
try:
    dbutils.widgets.text("catalog_name", "adb_ratecard_dev_001", "Target Catalog")
    dbutils.widgets.text("env", "dev", "Environment")
    
    catalog_name = dbutils.widgets.get("catalog_name")
    env = dbutils.widgets.get("env")
except Exception:
    catalog_name = "adb_ratecard_dev_001"
    env = "dev"

print(f"=== Starting Lakehouse Catalog Objects Deployment ===")
print(f"Environment : {env}")
print(f"Catalog     : {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2: SQL Execution Helper & Directory Resolution

# COMMAND ----------

def find_catalog_objects_root():
    """Locate catalog_objects root folder reliably in Databricks Repos."""
    possible_roots = [
        os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else None,
        os.getcwd(),
        "/Workspace/Repos/dev/work_experience_pipeline_code/ratecard_pipeline/databricks/catalog_objects",
        os.path.join(os.getcwd(), "ratecard_pipeline", "databricks", "catalog_objects"),
        os.path.join(os.getcwd(), "databricks", "catalog_objects")
    ]
    
    for root in possible_roots:
        if root and os.path.exists(os.path.join(root, "catalogs")):
            return root
            
    raise FileNotFoundError("Could not locate 'catalog_objects' directory. Please check workspace path.")

base_path = find_catalog_objects_root()
print(f"Resolved Catalog Objects Base Path: {base_path}")

def execute_sql_file(file_path, substitutions):
    """Reads a .sql file, performs variable interpolation, and executes statements."""
    filename = os.path.basename(file_path)
    print(f"\n---> Executing: {filename}")
    
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Interpolate parameters (${catalog_name}, ${env})
    for var_name, var_value in substitutions.items():
        content = content.replace(f"${{{var_name}}}", var_value)
    
    # Split into individual statements by semicolon
    statements = [stmt.strip() for stmt in content.split(";") if stmt.strip()]
    
    executed_count = 0
    for stmt in statements:
        # Filter comments
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
# MAGIC ### Cell 3: Execute Hierarchical Lifecycle (Catalogs -> Schemas -> Tables -> Permissions)

# COMMAND ----------

substitutions = {
    "catalog_name": catalog_name,
    "env": env
}

# The strict Unity Catalog deployment lifecycle
DEPLOYMENT_STAGES = [
    ("1. Catalogs", "catalogs"),
    ("2. Schemas", "schemas"),
    ("3. Tables", "tables"),
    ("4. Permissions", "permissions")
]

for stage_title, stage_dir_name in DEPLOYMENT_STAGES:
    stage_path = os.path.join(base_path, stage_dir_name)
    print(f"\n========================================================")
    print(f"Stage: {stage_title} ({stage_path})")
    print(f"========================================================")
    
    if not os.path.exists(stage_path):
        print(f"Directory {stage_dir_name} not found, skipping...")
        continue
        
    sql_files = sorted([
        os.path.join(stage_path, f) 
        for f in os.listdir(stage_path) 
        if f.endswith(".sql")
    ])
    
    for sql_file in sql_files:
        execute_sql_file(sql_file, substitutions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 4: Deployment Verification & Summary

# COMMAND ----------

print("\n=== Lakehouse Catalog Deployment Complete ===")
print(f"Active Catalog: {catalog_name}")

# List all schemas in the deployed catalog
df_schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
display(df_schemas)

# List all tables in the medallion schemas
for schema in ["silver", "gold", "control"]:
    try:
        print(f"\nTables in {catalog_name}.{schema}:")
        display(spark.sql(f"SHOW TABLES IN {catalog_name}.{schema}"))
    except Exception as e:
        print(f"  Could not list tables in {schema}: {e}")
