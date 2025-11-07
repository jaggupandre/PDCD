--===============================================================================
-- FUNCTION: Working
--===============================================================================


WITH current_md5_metadata_cte AS (
    SELECT * FROM pdcd_schema.get_table_columns_md5(ARRAY['analytics_schema'])
),

-- 1. RENAMED: same MD5, only name changed
renamed_objects AS (
    SELECT
        c.schema_name,
        c.object_type,
        c.object_type_name,
        c.object_subtype,
        -- c.object_subtype_name AS new_object_subtype_name,
        -- s.object_subtype_name AS prev_object_subtype_name,
        CONCAT(s.object_subtype_name,'-', c.object_subtype_name) AS object_subtype_name,
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

-- 2. MODIFIED: same name, MD5 changed
modified_objects AS (
    SELECT
        c.schema_name,
        c.object_type,
        c.object_type_name,
        c.object_subtype,
        c.object_subtype_name,
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

-- 3. ADDED: new MD5 not in staging (exclude renamed & modified)
added_objects AS (
    SELECT
        c.schema_name,
        c.object_type,
        c.object_type_name,
        c.object_subtype,
        c.object_subtype_name,
        NULL::text AS prev_object_md5,
        c.object_md5 AS object_md5,
        clock_timestamp() AS processed_time,
        'ADDED' AS change_type
    FROM current_md5_metadata_cte c
    LEFT JOIN pdcd_schema.md5_metadata_staging_tbl s
      ON s.object_md5 = c.object_md5
    WHERE s.object_md5 IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM renamed_objects r
          WHERE r.object_md5 = c.object_md5
      )
      AND NOT EXISTS (
          SELECT 1 FROM modified_objects m
          WHERE m.object_md5 = c.object_md5
      )
),

-- 4. DELETED: missing in current (exclude renamed & modified)
deleted_objects AS (
    SELECT
        s.schema_name,
        s.object_type,
        s.object_type_name,
        s.object_subtype,
        s.object_subtype_name,
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
          SELECT 1 FROM renamed_objects r
          WHERE r.prev_object_md5 = s.object_md5
      )
      AND NOT EXISTS (
          SELECT 1 FROM modified_objects m
          WHERE m.prev_object_md5 = s.object_md5
      )
)

-- FINAL UNION (same shape)
SELECT
    schema_name,
    object_type,
    object_type_name,
    object_subtype,
    object_subtype_name,
    object_md5,
    processed_time,
    change_type
FROM (
    SELECT * FROM renamed_objects
    UNION ALL
    SELECT * FROM modified_objects
    UNION ALL
    SELECT * FROM added_objects
    UNION ALL
    SELECT * FROM deleted_objects
) t
ORDER BY schema_name, object_type_name, object_subtype_name, change_type;


-------------------------***************----------------------
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

    -- current (new) metadata (per your function)
    current_md5 AS (
        SELECT * FROM pdcd_schema.get_table_columns_md5(p_table_list)
    ),

    -- old staged metadata (previous snapshot)
    staged_md5 AS (
        SELECT *
        FROM pdcd_schema.md5_metadata_staging_tbl
    ),

    -- RENAMED: same MD5 (structure) but column name changed
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
        FROM current_md5 c
        JOIN staged_md5 s
          ON  s.schema_name = c.schema_name
         AND s.object_type = c.object_type
         AND s.object_type_name = c.object_type_name
         AND COALESCE(s.object_subtype, '') = COALESCE(c.object_subtype, '')
         -- same md5 -> same definition, but name changed
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
            s.object_subtype_name AS prev_object_subtype_name,
            c.object_subtype_name AS new_object_subtype_name,
            s.object_subtype_details AS prev_object_subtype_details,
            c.object_subtype_details AS new_object_subtype_details,
            s.object_md5 AS prev_object_md5,
            c.object_md5 AS object_md5,
            clock_timestamp() AS processed_time,
            'MODIFIED' AS change_type
        FROM current_md5 c
        JOIN staged_md5 s
          ON  s.schema_name = c.schema_name
         AND s.object_type = c.object_type
         AND s.object_type_name = c.object_type_name
         AND COALESCE(s.object_subtype, '') = COALESCE(c.object_subtype, '')
         -- same canonical name -> same "identity"
         AND s.object_subtype_name = c.object_subtype_name
        WHERE s.object_md5 IS DISTINCT FROM c.object_md5
    ),

    -- ADDED: present in current but not in staged, and NOT already classified as MODIFIED or RENAMED
    added_objects AS (
        SELECT
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            NULL::text AS prev_object_subtype_name,
            c.object_subtype_name AS new_object_subtype_name,
            NULL::text AS prev_object_subtype_details,
            c.object_subtype_details AS new_object_subtype_details,
            NULL::text AS prev_object_md5,
            c.object_md5 AS object_md5,
            clock_timestamp() AS processed_time,
            'ADDED' AS change_type
        FROM current_md5 c
        LEFT JOIN staged_md5 s
          ON  s.schema_name = c.schema_name
         AND s.object_type = c.object_type
         AND s.object_type_name = c.object_type_name
         AND COALESCE(s.object_subtype, '') = COALESCE(c.object_subtype, '')
         AND s.object_subtype_name = c.object_subtype_name
        WHERE s.object_subtype_name IS NULL
          -- exclude rows that are already detected as MODIFIED by identity
          AND NOT EXISTS (
              SELECT 1 FROM modified_objects m
               WHERE m.schema_name = c.schema_name
                 AND m.object_type = c.object_type
                 AND m.object_type_name = c.object_type_name
                 AND COALESCE(m.object_subtype, '') = COALESCE(c.object_subtype, '')
                 AND m.new_object_subtype_name = c.object_subtype_name
          )
          -- exclude rows that are already detected as RENAMED (the new name)
          AND NOT EXISTS (
              SELECT 1 FROM renamed_objects r
               WHERE r.schema_name = c.schema_name
                 AND r.object_type = c.object_type
                 AND r.object_type_name = c.object_type_name
                 AND COALESCE(r.object_subtype, '') = COALESCE(c.object_subtype, '')
                 AND r.new_object_subtype_name = c.object_subtype_name
          )
    ),

    -- DELETED: present in staged but not in current, and NOT part of renamed/modified (use prev_object_md5 exclusions)
    deleted_objects AS (
        SELECT
            s.schema_name,
            s.object_type,
            s.object_type_name,
            s.object_subtype,
            s.object_subtype_name AS prev_object_subtype_name,
            NULL::text AS new_object_subtype_name,
            s.object_subtype_details AS prev_object_subtype_details,
            NULL::text AS new_object_subtype_details,
            s.object_md5 AS prev_object_md5,
            NULL::text AS object_md5,
            clock_timestamp() AS processed_time,
            'DELETED' AS change_type
        FROM staged_md5 s
        LEFT JOIN current_md5 c
          ON  s.schema_name = c.schema_name
         AND s.object_type = c.object_type
         AND s.object_type_name = c.object_type_name
         AND COALESCE(s.object_subtype, '') = COALESCE(c.object_subtype, '')
         AND s.object_subtype_name = c.object_subtype_name
        WHERE c.object_subtype_name IS NULL
          -- exclude if this staged row was actually the "prev" side of a RENAMED
          AND NOT EXISTS (
              SELECT 1 FROM renamed_objects r WHERE r.prev_object_md5 = s.object_md5
          )
          -- exclude if this staged row was the prev side of a MODIFIED detection
          AND NOT EXISTS (
              SELECT 1 FROM modified_objects m WHERE m.prev_object_md5 = s.object_md5
          )
    ),

    -- unified shape for insertion: keep columns consistent across all branches
    unified_changes AS (
        SELECT
            schema_name, object_type, object_type_name, object_subtype,
            prev_object_subtype_name, new_object_subtype_name,
            prev_object_subtype_details, new_object_subtype_details,
            prev_object_md5, object_md5,
            processed_time, change_type
        FROM renamed_objects

        UNION ALL

        SELECT
            schema_name, object_type, object_type_name, object_subtype,
            prev_object_subtype_name, new_object_subtype_name,
            prev_object_subtype_details, new_object_subtype_details,
            prev_object_md5, object_md5,
            processed_time, change_type
        FROM modified_objects

        UNION ALL

        SELECT
            schema_name, object_type, object_type_name, object_subtype,
            prev_object_subtype_name, new_object_subtype_name,
            prev_object_subtype_details, new_object_subtype_details,
            prev_object_md5, object_md5,
            processed_time, change_type
        FROM added_objects

        UNION ALL

        SELECT
            schema_name, object_type, object_type_name, object_subtype,
            prev_object_subtype_name, new_object_subtype_name,
            prev_object_subtype_details, new_object_subtype_details,
            prev_object_md5, object_md5,
            processed_time, change_type
        FROM deleted_objects
    ),

    -- Insert into md5_metadata_tbl using normalized name/details/md5
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
            COALESCE(u.new_object_subtype_name, u.prev_object_subtype_name) AS object_subtype_name,
            COALESCE(u.new_object_subtype_details, u.prev_object_subtype_details) AS object_subtype_details,
            COALESCE(u.object_md5, u.prev_object_md5) AS object_md5,
            u.change_type
        FROM unified_changes u
        CROSS JOIN new_snapshot ns
        RETURNING metadata_id, snapshot_id, schema_name, object_type, object_type_name,
                  object_subtype, object_subtype_name, object_subtype_details,
                  object_md5, change_type
    )

    -- Final output: show what we inserted (with a current timestamp as processed_time)
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


