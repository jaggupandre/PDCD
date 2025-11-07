WITH current_md5_metadata_cte AS (
    SELECT * FROM pdcd_schema.get_table_columns_md5(ARRAY['analytics_schema'])
),

-- RENAMED: same MD5, different column name
renamed_objects AS (
    SELECT
        c.schema_name,
        c.object_type,
        c.object_type_name,
        c.object_subtype,
        c.object_subtype_name,
        NULL::text AS prev_object_subtype_details,
        NULL::text AS new_object_subtype_details,
        s.object_md5   AS prev_object_md5,
        c.object_md5   AS object_md5,
        clock_timestamp() AS processed_time,
        'RENAMED' AS change_type
    FROM current_md5_metadata_cte c
    JOIN pdcd_schema.md5_metadata_staging_tbl s
      ON  s.schema_name = c.schema_name
      AND s.object_type = c.object_type
      AND s.object_type_name = c.object_type_name
      AND COALESCE(s.object_subtype, '') = COALESCE(c.object_subtype, '')
      AND s.object_md5 = c.object_md5
    WHERE s.object_subtype_name IS DISTINCT FROM c.object_subtype_name
),

-- MODIFIED: same identity (name) but MD5 changed
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
      AND COALESCE(s.object_subtype, '') = COALESCE(c.object_subtype, '')
      AND COALESCE(s.object_subtype_name, '') = COALESCE(c.object_subtype_name, '')
    WHERE s.object_md5 IS DISTINCT FROM c.object_md5
),

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
      AND c.object_md5 NOT IN (
          SELECT prev_object_md5 FROM renamed_objects
          UNION
          SELECT prev_object_md5 FROM modified_objects
      )
),

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
      AND COALESCE(s.object_subtype, '') = COALESCE(c.object_subtype, '')
      AND COALESCE(s.object_subtype_name, '') = COALESCE(c.object_subtype_name, '')
    WHERE c.object_md5 IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM renamed_objects r WHERE r.prev_object_md5 = s.object_md5
      )
      AND NOT EXISTS (
          SELECT 1 FROM modified_objects m WHERE m.prev_object_md5 = s.object_md5
      )
)

-- UNIFIED OUTPUT (same column shape for each CTE)
SELECT
    schema_name,
    object_type,
    object_type_name,
    object_subtype,
    object_subtype_name,
    -- COALESCE(new_object_subtype_details, prev_object_subtype_details) AS object_subtype_details,
    -- COALESCE(object_md5, prev_object_md5) AS object_md5,
    -- new_object_subtype_details AS object_subtype_details,
    object_md5,
    processed_time,
    change_type
FROM (
    SELECT * FROM added_objects
    UNION ALL
    SELECT * FROM renamed_objects
    UNION ALL
    SELECT * FROM modified_objects
    UNION ALL
    SELECT * FROM deleted_objects
) t
ORDER BY schema_name, object_type_name, object_subtype_name, change_type;