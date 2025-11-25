-- Combined metadata change detection for columns, constraints, indexes, references, triggers, sequences, and functions
-- Detects: RENAMED, MODIFIED, ADDED, DELETED changes

CREATE OR REPLACE FUNCTION pdcd_schema.compare_load_md5_metadata_tbl(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    metadata_id BIGINT,
    snapshot_id INTEGER,
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
    object_subtype TEXT,
    object_subtype_name TEXT,
    object_subtype_details TEXT,
    object_md5 TEXT,
    processed_time TIMESTAMP,
    change_type TEXT
)
LANGUAGE SQL
AS $function$
    WITH processing_context AS (
        -- Calculate once for consistency
        SELECT 
            (SELECT MAX(snapshot_id) FROM pdcd_schema.snapshot_tbl) AS snapshot_id,
            clock_timestamp() AS processed_time
    ),
    
    -- Get current metadata for all object types
    current_metadata_cte AS (
        -- Columns
        SELECT 
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        FROM pdcd_schema.get_table_columns_md5(p_table_list)
        
        UNION ALL
        
        -- Constraints
        SELECT 
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        FROM pdcd_schema.get_table_constraints_md5(p_table_list)
        
        UNION ALL
        
        -- Indexes
        SELECT 
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        FROM pdcd_schema.get_table_indexes_md5(p_table_list)
        
        UNION ALL
        
        -- References
        SELECT 
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        FROM pdcd_schema.get_table_references_md5(p_table_list)
        
        UNION ALL
        
        -- Triggers
        SELECT 
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        FROM pdcd_schema.get_table_triggers_md5(p_table_list)
        
        UNION ALL
        
        -- Sequences
        SELECT 
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        FROM pdcd_schema.get_table_sequences_md5(p_table_list)
        
    --     UNION ALL
        
    --     -- Functions (NEW)
    --     SELECT 
    --         schema_name,
    --         object_type,
    --         object_type_name,
    --         object_subtype,
    --         object_subtype_name,
    --         object_subtype_details,
    --         object_md5
    --     FROM pdcd_schema.get_table_functions_md5(p_table_list)
    ),
    
    -- Get staging metadata (previous snapshot) for all object types
    staging_metadata_cte AS (
        SELECT 
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        FROM pdcd_schema.md5_metadata_staging_tbl
    ),
    
    -- Step 1: Detect RENAMED objects (same MD5, different name)
    -- Applies to all object types
    renamed_objects AS (
        SELECT
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            c.object_subtype_name,
            c.object_subtype_details,
            c.object_md5,
            'RENAMED' AS change_type,
            s.object_subtype_name AS prev_object_subtype_name,
            s.object_subtype_details AS prev_object_subtype_details,
            s.object_md5 AS prev_object_md5
        FROM current_metadata_cte c
        INNER JOIN staging_metadata_cte s
            ON s.schema_name = c.schema_name
            AND s.object_type = c.object_type
            AND s.object_type_name = c.object_type_name
            AND s.object_subtype = c.object_subtype
            AND s.object_md5 = c.object_md5  -- Same content (MD5)
        WHERE s.object_subtype_name IS DISTINCT FROM c.object_subtype_name  -- Different name
    ),
    
    -- Step 2: Detect MODIFIED objects (same name, different definition)
    -- Applies to all object types
    modified_objects AS (
        SELECT
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            c.object_subtype_name,
            c.object_subtype_details,
            c.object_md5,
            'MODIFIED' AS change_type,
            s.object_subtype_name AS prev_object_subtype_name,
            s.object_subtype_details AS prev_object_subtype_details,
            s.object_md5 AS prev_object_md5
        FROM current_metadata_cte c
        INNER JOIN staging_metadata_cte s
            ON s.schema_name = c.schema_name
            AND s.object_type = c.object_type
            AND s.object_type_name = c.object_type_name
            AND s.object_subtype = c.object_subtype
            AND s.object_subtype_name = c.object_subtype_name  -- Same name
        WHERE s.object_md5 IS DISTINCT FROM c.object_md5  -- Different definition
    ),
    
    -- Create exclusion sets for performance
    -- CRITICAL: Include both old and new names of renamed objects
    -- to prevent false DELETED/ADDED flags
    processed_current_objects AS (
        -- Current name of renamed objects
        SELECT schema_name, object_type, object_type_name, object_subtype, object_subtype_name
        FROM renamed_objects
        UNION
        -- Previous name of renamed objects (to exclude from ADDED)
        SELECT schema_name, object_type, object_type_name, object_subtype, prev_object_subtype_name AS object_subtype_name
        FROM renamed_objects
        UNION
        -- Modified objects
        SELECT schema_name, object_type, object_type_name, object_subtype, object_subtype_name
        FROM modified_objects
    ),
    
    processed_staging_objects AS (
        -- Previous name of renamed objects (to exclude from DELETED)
        SELECT schema_name, object_type, object_type_name, object_subtype, object_subtype_name
        FROM renamed_objects
        UNION
        -- Current name of renamed objects
        SELECT schema_name, object_type, object_type_name, object_subtype, prev_object_subtype_name AS object_subtype_name
        FROM renamed_objects
        UNION
        -- Modified objects
        SELECT schema_name, object_type, object_type_name, object_subtype, object_subtype_name
        FROM modified_objects
    ),
    
    -- Step 3: Detect ADDED objects (new objects)
    added_objects AS (
        SELECT
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            c.object_subtype_name,
            c.object_subtype_details,
            c.object_md5,
            'ADDED' AS change_type,
            NULL::TEXT AS prev_object_subtype_name,
            NULL::TEXT AS prev_object_subtype_details,
            NULL::TEXT AS prev_object_md5
        FROM current_metadata_cte c
        WHERE NOT EXISTS (
            SELECT 1 
            FROM staging_metadata_cte s
            WHERE s.schema_name = c.schema_name
                AND s.object_type = c.object_type
                AND s.object_type_name = c.object_type_name
                AND s.object_subtype = c.object_subtype
                AND s.object_subtype_name = c.object_subtype_name
        )
        AND NOT EXISTS (
            SELECT 1 
            FROM processed_current_objects p
            WHERE p.schema_name = c.schema_name
                AND p.object_type = c.object_type
                AND p.object_type_name = c.object_type_name
                AND p.object_subtype = c.object_subtype
                AND p.object_subtype_name = c.object_subtype_name
        )
    ),
    
    -- Step 4: Detect DELETED objects (removed objects)
    deleted_objects AS (
        SELECT
            s.schema_name,
            s.object_type,
            s.object_type_name,
            s.object_subtype,
            s.object_subtype_name,
            s.object_subtype_details,
            s.object_md5,
            'DELETED' AS change_type,
            s.object_subtype_name AS prev_object_subtype_name,
            s.object_subtype_details AS prev_object_subtype_details,
            s.object_md5 AS prev_object_md5
        FROM staging_metadata_cte s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM current_metadata_cte c
            WHERE c.schema_name = s.schema_name
                AND c.object_type = s.object_type
                AND c.object_type_name = s.object_type_name
                AND c.object_subtype = s.object_subtype
                AND c.object_subtype_name = s.object_subtype_name
        )
        AND NOT EXISTS (
            SELECT 1 
            FROM processed_staging_objects p
            WHERE p.schema_name = s.schema_name
                AND p.object_type = s.object_type
                AND p.object_type_name = s.object_type_name
                AND p.object_subtype = s.object_subtype
                AND p.object_subtype_name = s.object_subtype_name
        )
    ),
    
    -- Step 5: Combine all changes for all object types
    unified_changes AS (
        SELECT * FROM renamed_objects
        UNION ALL
        SELECT * FROM modified_objects
        UNION ALL
        SELECT * FROM added_objects
        UNION ALL
        SELECT * FROM deleted_objects
    ),
    
    -- Step 6: Insert into metadata table
    inserted AS (
        INSERT INTO pdcd_schema.md5_metadata_tbl (
            snapshot_id,
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5,
            change_type
        )
        SELECT
            pc.snapshot_id,
            u.schema_name,
            u.object_type,
            u.object_type_name,
            u.object_subtype,
            u.object_subtype_name,
            u.object_subtype_details,
            u.object_md5,
            u.change_type
        FROM unified_changes u
        CROSS JOIN processing_context pc
        RETURNING 
            metadata_id, 
            snapshot_id, 
            schema_name, 
            object_type, 
            object_type_name,
            object_subtype, 
            object_subtype_name, 
            object_subtype_details,
            object_md5, 
            change_type
    )
    
    -- Step 7: Return results for all object types
    SELECT
        i.metadata_id,
        i.snapshot_id,
        i.schema_name,
        i.object_type,
        i.object_type_name,
        i.object_subtype,
        i.object_subtype_name,
        i.object_subtype_details,
        i.object_md5,
        pc.processed_time,
        i.change_type
    FROM inserted i
    CROSS JOIN processing_context pc
    ORDER BY 
        i.schema_name, 
        i.object_type_name, 
        i.object_subtype,
        i.object_subtype_name,
        i.change_type;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/load_compare/compare_load_md5_metadata_tbl.sql'