-- ==============================================================================
-- Control & Metadata Table Definitions (Audits & Incremental Watermarks)
-- Engine: Databricks Unity Catalog / Delta Lake
-- Target: ${catalog_name}.control.<table>
-- ==============================================================================

-- 1. Pipeline Incremental Watermark & Execution Control Table
CREATE TABLE IF NOT EXISTS ${catalog_name}.control.pipeline_watermark_control (
    pipeline_name STRING NOT NULL COMMENT 'Unique identifier of the ADF or Databricks pipeline',
    source_table STRING NOT NULL COMMENT 'Source relational schema and table name',
    watermark_column STRING NOT NULL COMMENT 'Timestamp column monitored for deltas',
    last_processed_watermark TIMESTAMP NOT NULL COMMENT 'High-watermark timestamp committed after successful run',
    rows_processed BIGINT COMMENT 'Count of records committed in the batch',
    last_run_status STRING NOT NULL COMMENT 'Final run execution status (SUCCESS / FAILED)',
    updated_at TIMESTAMP NOT NULL COMMENT 'Audit timestamp of the control table update'
)
USING DELTA
COMMENT 'Control Layer: ACID delta watermark tracking and audit history';
