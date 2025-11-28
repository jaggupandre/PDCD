-- =====================================================================
-- NEW: Dedicated function for tracking standalone schema objects (Functions)
-- Simpler logic since functions don't have subtypes like table objects do
-- =====================================================================
CREATE OR REPLACE FUNCTION pdcd_schema.compare_load_md5_metadata_functions(
    p_function_list TEXT[] DEFAULT NULL  -- Optional: specific function names to track
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
        SELECT 
            (SELECT MAX(snapshot_id) FROM pdcd_schema.snapshot_tbl) AS snapshot_id,
            clock_timestamp() AS processed_time
    ),
    
    -- Get current function metadata
    current_functions AS (
        SELECT 
            schema_name,
            object_type,
            object_type_name,  -- Function name
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        FROM pdcd_schema.get_table_functions_md5(p_function_list)
    ),
    
    -- Get staging function metadata (previous snapshot)
    staging_functions AS (
        SELECT 
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        FROM pdcd_schema.md5_metadata_staging_functions
    ),
    
    -- Step 1: RENAMED functions (same MD5, different function name)
    renamed_functions AS (
        SELECT
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            c.object_subtype_name,
            c.object_subtype_details,
            c.object_md5,
            'RENAMED' AS change_type,
            s.object_type_name AS prev_function_name
        FROM current_functions c
        INNER JOIN staging_functions s
            ON s.schema_name = c.schema_name
            AND s.object_md5 = c.object_md5  -- Same content
            AND s.object_type_name != c.object_type_name  -- Different name
    ),
    
    -- Step 2: MODIFIED functions (same name, different MD5)
    modified_functions AS (
        SELECT
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            c.object_subtype_name,
            c.object_subtype_details,
            c.object_md5,
            'MODIFIED' AS change_type,
            c.object_type_name AS prev_function_name
        FROM current_functions c
        INNER JOIN staging_functions s
            ON s.schema_name = c.schema_name
            AND s.object_type_name = c.object_type_name  -- Same name
            AND s.object_md5 != c.object_md5  -- Different content
    ),
    
    -- Exclusion set: functions already processed
    processed_functions AS (
        -- Current names of renamed functions
        SELECT schema_name, object_type_name
        FROM renamed_functions
        UNION
        -- Previous names of renamed functions
        SELECT schema_name, prev_function_name AS object_type_name
        FROM renamed_functions
        UNION
        -- Modified functions
        SELECT schema_name, object_type_name
        FROM modified_functions
    ),
    
    -- Step 3: ADDED functions (new functions)
    added_functions AS (
        SELECT
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            c.object_subtype_name,
            c.object_subtype_details,
            c.object_md5,
            'ADDED' AS change_type,
            NULL::TEXT AS prev_function_name
        FROM current_functions c
        WHERE NOT EXISTS (
            SELECT 1 
            FROM staging_functions s
            WHERE s.schema_name = c.schema_name
                AND s.object_type_name = c.object_type_name
        )
        AND NOT EXISTS (
            SELECT 1 
            FROM processed_functions p
            WHERE p.schema_name = c.schema_name
                AND p.object_type_name = c.object_type_name
        )
    ),
    
    -- Step 4: DELETED functions (removed functions)
    deleted_functions AS (
        SELECT
            s.schema_name,
            s.object_type,
            s.object_type_name,
            s.object_subtype,
            s.object_subtype_name,
            s.object_subtype_details,
            s.object_md5,
            'DELETED' AS change_type,
            s.object_type_name AS prev_function_name
        FROM staging_functions s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM current_functions c
            WHERE c.schema_name = s.schema_name
                AND c.object_type_name = s.object_type_name
        )
        AND NOT EXISTS (
            SELECT 1 
            FROM processed_functions p
            WHERE p.schema_name = s.schema_name
                AND p.object_type_name = s.object_type_name
        )
    ),
    
    -- Step 5: Combine all changes
    unified_changes AS (
        SELECT 
            schema_name, object_type, object_type_name, object_subtype,
            object_subtype_name, object_subtype_details, object_md5, change_type
        FROM renamed_functions
        UNION ALL
        SELECT 
            schema_name, object_type, object_type_name, object_subtype,
            object_subtype_name, object_subtype_details, object_md5, change_type
        FROM modified_functions
        UNION ALL
        SELECT 
            schema_name, object_type, object_type_name, object_subtype,
            object_subtype_name, object_subtype_details, object_md5, change_type
        FROM added_functions
        UNION ALL
        SELECT 
            schema_name, object_type, object_type_name, object_subtype,
            object_subtype_name, object_subtype_details, object_md5, change_type
        FROM deleted_functions
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
    
    -- Step 7: Return results
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
        i.change_type;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/load_compare/compare_load_md5_metadata_functions.sql'