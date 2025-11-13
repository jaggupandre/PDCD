-- ============================================
-- DETAILED SCHEMA CHANGE REPORT
-- Between two timestamps
-- ============================================

-- Set your time range here
\set start_time '2025-11-12 18:13:39'
\set end_time '2025-11-12 18:18:55'

-- ============================================
-- 1. EXECUTIVE SUMMARY
-- ============================================
SELECT 
    'EXECUTIVE SUMMARY' as report_section,
    COUNT(DISTINCT snapshot_id) as total_snapshots,
    COUNT(DISTINCT schema_name) as schemas_affected,
    COUNT(DISTINCT object_type_name) as tables_affected,
    COUNT(DISTINCT object_subtype_name) as columns_affected,
    COUNT(*) as total_changes,
    MIN(processed_time) as first_change,
    MAX(processed_time) as last_change
FROM pdcd_schema.md5_metadata_tbl
WHERE processed_time BETWEEN :'start_time' AND :'end_time';

-- ============================================
-- 2. CHANGE TYPE BREAKDOWN
-- ============================================
SELECT 
    'CHANGE TYPE BREAKDOWN' as report_section,
    change_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM pdcd_schema.md5_metadata_tbl
WHERE processed_time BETWEEN :'start_time' AND :'end_time'
GROUP BY change_type
ORDER BY count DESC;

-- ============================================
-- 3. CHANGES BY TABLE
-- ============================================
SELECT 
    'CHANGES BY TABLE' as report_section,
    schema_name,
    object_type_name as table_name,
    COUNT(*) as total_changes,
    COUNT(*) FILTER (WHERE change_type = 'ADDED') as added,
    COUNT(*) FILTER (WHERE change_type = 'MODIFIED') as modified,
    COUNT(*) FILTER (WHERE change_type = 'DELETED') as deleted,
    COUNT(*) FILTER (WHERE change_type = 'RENAMED') as renamed
FROM pdcd_schema.md5_metadata_tbl
WHERE processed_time BETWEEN :'start_time' AND :'end_time'
GROUP BY schema_name, object_type_name
ORDER BY total_changes DESC;

-- ============================================
-- 4. DETAILED COLUMN ADDITIONS
-- ============================================
SELECT 
    'COLUMN ADDITIONS' as report_section,
    snapshot_id,
    processed_time,
    schema_name,
    object_type_name as table_name,
    object_subtype_name as column_name,
    REGEXP_REPLACE(object_subtype_details, '.*data_type:([^,]+).*', '\1') as data_type,
    REGEXP_REPLACE(object_subtype_details, '.*max_length:([^,]*),.*', '\1') as max_length,
    REGEXP_REPLACE(object_subtype_details, '.*nullable:([^,]+).*', '\1') as nullable,
    REGEXP_REPLACE(object_subtype_details, '.*default_value:([^,]*),.*', '\1') as default_value,
    REGEXP_REPLACE(object_subtype_details, '.*ordinal_position:([0-9]+).*', '\1') as position
FROM pdcd_schema.md5_metadata_tbl
WHERE processed_time BETWEEN :'start_time' AND :'end_time'
    AND change_type = 'ADDED'
ORDER BY processed_time, schema_name, object_type_name, object_subtype_name;

-- ============================================
-- 5. DETAILED COLUMN MODIFICATIONS
-- ============================================
WITH current_changes AS (
    SELECT 
        snapshot_id,
        processed_time,
        schema_name,
        object_type_name,
        object_subtype_name,
        object_subtype_details,
        object_md5
    FROM pdcd_schema.md5_metadata_tbl
    WHERE processed_time BETWEEN :'start_time' AND :'end_time'
        AND change_type = 'MODIFIED'
),
previous_state AS (
    SELECT DISTINCT ON (cc.schema_name, cc.object_type_name, cc.object_subtype_name)
        cc.schema_name,
        cc.object_type_name,
        cc.object_subtype_name,
        m.object_subtype_details as old_details,
        m.processed_time as old_time
    FROM current_changes cc
    JOIN pdcd_schema.md5_metadata_tbl m 
        ON cc.schema_name = m.schema_name 
        AND cc.object_type_name = m.object_type_name
        AND cc.object_subtype_name = m.object_subtype_name
        AND m.processed_time < cc.processed_time
    ORDER BY cc.schema_name, cc.object_type_name, cc.object_subtype_name, m.processed_time DESC
)
SELECT 
    'COLUMN MODIFICATIONS' as report_section,
    cc.snapshot_id,
    cc.processed_time,
    cc.schema_name,
    cc.object_type_name as table_name,
    cc.object_subtype_name as column_name,
    -- Extract and compare key attributes
    CASE 
        WHEN REGEXP_REPLACE(ps.old_details, '.*data_type:([^,]+).*', '\1') != 
             REGEXP_REPLACE(cc.object_subtype_details, '.*data_type:([^,]+).*', '\1')
        THEN 'Data Type: ' || 
             REGEXP_REPLACE(ps.old_details, '.*data_type:([^,]+).*', '\1') || ' → ' ||
             REGEXP_REPLACE(cc.object_subtype_details, '.*data_type:([^,]+).*', '\1')
        ELSE ''
    END as data_type_change,
    CASE 
        WHEN REGEXP_REPLACE(ps.old_details, '.*max_length:([^,]*),.*', '\1') != 
             REGEXP_REPLACE(cc.object_subtype_details, '.*max_length:([^,]*),.*', '\1')
        THEN 'Max Length: ' || 
             REGEXP_REPLACE(ps.old_details, '.*max_length:([^,]*),.*', '\1') || ' → ' ||
             REGEXP_REPLACE(cc.object_subtype_details, '.*max_length:([^,]*),.*', '\1')
        ELSE ''
    END as length_change,
    CASE 
        WHEN REGEXP_REPLACE(ps.old_details, '.*nullable:([^,]+).*', '\1') != 
             REGEXP_REPLACE(cc.object_subtype_details, '.*nullable:([^,]+).*', '\1')
        THEN 'Nullable: ' || 
             REGEXP_REPLACE(ps.old_details, '.*nullable:([^,]+).*', '\1') || ' → ' ||
             REGEXP_REPLACE(cc.object_subtype_details, '.*nullable:([^,]+).*', '\1')
        ELSE ''
    END as nullable_change
FROM current_changes cc
LEFT JOIN previous_state ps 
    ON cc.schema_name = ps.schema_name 
    AND cc.object_type_name = ps.object_type_name
    AND cc.object_subtype_name = ps.object_subtype_name
ORDER BY cc.processed_time, cc.schema_name, cc.object_type_name;

-- ============================================
-- 6. COLUMN DELETIONS
-- ============================================
SELECT 
    'COLUMN DELETIONS' as report_section,
    snapshot_id,
    processed_time,
    schema_name,
    object_type_name as table_name,
    object_subtype_name as column_name,
    REGEXP_REPLACE(object_subtype_details, '.*data_type:([^,]+).*', '\1') as data_type,
    REGEXP_REPLACE(object_subtype_details, '.*ordinal_position:([0-9]+).*', '\1') as position
FROM pdcd_schema.md5_metadata_tbl
WHERE processed_time BETWEEN :'start_time' AND :'end_time'
    AND change_type = 'DELETED'
ORDER BY processed_time, schema_name, object_type_name;

-- ============================================
-- 7. COLUMN RENAMES
-- ============================================
WITH renames AS (
    SELECT 
        snapshot_id,
        processed_time,
        schema_name,
        object_type_name,
        object_subtype_name as new_name,
        object_md5,
        LAG(object_subtype_name) OVER (
            PARTITION BY schema_name, object_type_name, object_md5 
            ORDER BY processed_time
        ) as old_name
    FROM pdcd_schema.md5_metadata_tbl
    WHERE processed_time BETWEEN :'start_time' AND :'end_time'
        AND change_type = 'RENAMED'
)
SELECT 
    'COLUMN RENAMES' as report_section,
    snapshot_id,
    processed_time,
    schema_name,
    object_type_name as table_name,
    COALESCE(old_name, '(unknown)') as old_column_name,
    new_name as new_column_name
FROM renames
ORDER BY processed_time, schema_name, object_type_name;

-- ============================================
-- 8. CHRONOLOGICAL TIMELINE
-- ============================================
SELECT 
    'CHRONOLOGICAL TIMELINE' as report_section,
    snapshot_id,
    processed_time,
    schema_name,
    object_type_name as table_name,
    object_subtype_name as column_name,
    change_type,
    CASE change_type
        WHEN 'ADDED' THEN 'Added: ' || object_subtype_name
        WHEN 'MODIFIED' THEN 'Modified: ' || object_subtype_name
        WHEN 'DELETED' THEN 'Deleted: ' || object_subtype_name
        WHEN 'RENAMED' THEN 'Renamed: ' || object_subtype_name
    END as change_description
FROM pdcd_schema.md5_metadata_tbl
WHERE processed_time BETWEEN :'start_time' AND :'end_time'
ORDER BY processed_time, snapshot_id, schema_name, object_type_name;

-- ============================================
-- 9. HIGH-RISK CHANGES
-- ============================================
SELECT 
    'HIGH-RISK CHANGES' as report_section,
    processed_time,
    schema_name,
    object_type_name as table_name,
    object_subtype_name as column_name,
    change_type,
    CASE 
        WHEN change_type = 'DELETED' THEN 'CRITICAL: Column deleted - potential data loss'
        WHEN change_type = 'MODIFIED' 
            AND object_subtype_details LIKE '%nullable:YES%' 
            AND object_subtype_details LIKE '%nullable:NO%' 
                THEN 'HIGH: Made NOT NULL - may fail if nulls exist'
        WHEN change_type = 'MODIFIED' 
            AND object_subtype_details LIKE '%nullable:NO%'
            AND object_subtype_details LIKE '%nullable:YES%' 
                THEN 'MEDIUM: Made NULLABLE - constraint relaxed, may allow unexpected nulls'
        WHEN change_type = 'MODIFIED' AND object_subtype_details LIKE '%data_type%' 
                THEN 'MEDIUM: Data type changed - check compatibility'
        WHEN change_type = 'RENAMED' 
                THEN 'MEDIUM: Column renamed - update application code'
        ELSE 'LOW: Standard change'
    END as risk_level
FROM pdcd_schema.md5_metadata_tbl
WHERE processed_time BETWEEN :'start_time' AND :'end_time'
    AND change_type IN ('DELETED', 'MODIFIED', 'RENAMED')
ORDER BY 
    CASE 
        WHEN change_type = 'DELETED' THEN 1
        WHEN change_type = 'MODIFIED' THEN 2
        ELSE 3
    END,
    processed_time;