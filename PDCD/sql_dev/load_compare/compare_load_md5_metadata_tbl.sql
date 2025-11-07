

-- Main metadata load function, load_md5_metadata_tbl
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
    WITH new_snapshot AS (
        SELECT MAX(snapshot_id) AS snapshot_id
        FROM pdcd_schema.snapshot_tbl
    ),

    current_md5_metadata_cte AS (
        SELECT * FROM pdcd_schema.get_table_columns_md5(p_table_list)
    ),

    -- Step 1: Detect renamed objects (same MD5 but name changed)
    -- RENAMED: same MD5, different column name
    renamed_objects AS (
        SELECT
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            s.object_subtype_name AS prev_object_subtype_name,
            c.object_subtype_name AS new_object_subtype_name,
            s.object_subtype_details AS prev_object_subtype_details,
            c.object_subtype_details AS new_object_subtype_details,
            s.object_md5 AS prev_object_md5,
            c.object_md5 AS object_md5,
            clock_timestamp() AS processed_time,
            'RENAMED' AS change_type
        FROM current_md5_metadata_cte c
        JOIN pdcd_schema.md5_metadata_staging_tbl s
          ON  s.schema_name = c.schema_name
          AND s.object_type = c.object_type
          AND s.object_type_name = c.object_type_name
          AND s.object_subtype = c.object_subtype
          AND s.object_md5 = c.object_md5
        WHERE s.object_subtype_name IS DISTINCT FROM c.object_subtype_name
    ),

    -- Step 2: Detect modified objects (same object, MD5 changed)
    modified_objects AS (
        SELECT
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            c.object_subtype_name,
            s.object_subtype_details AS prev_object_subtype_details,
            c.object_subtype_details AS new_object_subtype_details,
            s.object_md5 AS prev_object_md5,
            c.object_md5 AS object_md5,
            clock_timestamp() AS processed_time,
            'MODIFIED' AS change_type
        FROM current_md5_metadata_cte c
        JOIN pdcd_schema.md5_metadata_staging_tbl s
          ON  s.schema_name = c.schema_name
          AND s.object_type = c.object_type
          AND s.object_type_name = c.object_type_name
          AND s.object_subtype = c.object_subtype
          AND s.object_subtype_name = c.object_subtype_name
        WHERE s.object_md5 IS DISTINCT FROM c.object_md5
    ),

    -- Step 3: Detect newly added objects
    -- ADDED: MD5 not present in staging AND not part of renamed/modified (safety)
    added_objects AS (
        SELECT
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            c.object_subtype_name,
            NULL::text AS prev_object_subtype_details,
            c.object_subtype_details AS new_object_subtype_details,
            NULL::text AS prev_object_md5,
            c.object_md5 AS object_md5,
            clock_timestamp() AS processed_time,
            'ADDED' AS change_type
        FROM current_md5_metadata_cte c
        LEFT JOIN pdcd_schema.md5_metadata_staging_tbl s
        ON s.object_md5 = c.object_md5
        WHERE s.object_md5 IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM renamed_objects r WHERE r.object_md5 = c.object_md5
          )
          AND NOT EXISTS (
              SELECT 1 FROM modified_objects m WHERE m.object_md5 = c.object_md5
          )
    ),

    -- Step 4: Detect deleted objects (exist in staging but missing in current)
    -- DELETED: present in staging but missing in current AND NOT renamed/modified
    deleted_objects AS (
        SELECT
            s.schema_name,
            s.object_type,
            s.object_type_name,
            s.object_subtype,
            s.object_subtype_name,
            s.object_subtype_details AS prev_object_subtype_details,
            NULL::text AS new_object_subtype_details,
            s.object_md5 AS prev_object_md5,
            NULL::text AS object_md5,
            clock_timestamp() AS processed_time,
            'DELETED' AS change_type
        FROM pdcd_schema.md5_metadata_staging_tbl s
        LEFT JOIN current_md5_metadata_cte c
          ON  s.schema_name = c.schema_name
          AND s.object_type = c.object_type
          AND s.object_type_name = c.object_type_name
          AND s.object_subtype = c.object_subtype
          AND s.object_subtype_name = c.object_subtype_name
        WHERE c.object_md5 IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM renamed_objects r WHERE r.prev_object_md5 = s.object_md5
          )
          AND NOT EXISTS (
              SELECT 1 FROM modified_objects m WHERE m.prev_object_md5 = s.object_md5
          )
    ),

    -- Step 5: Combine all changes
    unified_changes AS (
        SELECT * FROM renamed_objects
        UNION ALL
        SELECT * FROM modified_objects
        UNION ALL
        SELECT * FROM added_objects
        UNION ALL
        SELECT * FROM deleted_objects
    ),

    -- Step 6: Insert into final table
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
            ns.snapshot_id,
            u.schema_name,
            u.object_type,
            u.object_type_name,
            u.object_subtype,
            COALESCE(u.new_object_subtype_name, u.object_subtype_name),
            COALESCE(u.new_object_subtype_details, u.prev_object_subtype_details),
            COALESCE(u.object_md5, u.prev_object_md5),
            u.change_type
        FROM unified_changes u
        CROSS JOIN new_snapshot ns
        RETURNING metadata_id, snapshot_id, schema_name, object_type, object_type_name,
                  object_subtype, object_subtype_name, object_subtype_details,
                  object_md5, change_type
    )

    -- Step 6: Final output
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
        clock_timestamp() AS processed_time,
        i.change_type
    FROM inserted i
    JOIN new_snapshot ns ON TRUE
    ORDER BY i.schema_name, i.object_type_name, i.object_subtype_name;
$function$;


-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/load_compare/compare_load_md5_metadata_tbl.sql'


-- md5_metadata_tbl

-- SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);

-- SELECT * FROM pdcd_schema.load_snapshot_tbl();
-- SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);