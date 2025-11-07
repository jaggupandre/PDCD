WITH 
old_data AS (
    SELECT * FROM pdcd_schema.md5_metadata_staging_tbl WHERE snapshot_id = :old_snapshot
),
new_data AS (
    SELECT * FROM pdcd_schema.md5_metadata_staging_tbl WHERE snapshot_id = :new_snapshot
),

-- 1. Detect modified columns
modified AS (
    SELECT n.*
    FROM new_data n
    JOIN old_data o
      ON n.schema_name = o.schema_name
     AND n.object_type_name = o.object_type_name
     AND n.object_subtype_name = o.object_subtype_name
    WHERE n.object_md5 <> o.object_md5
),

-- 2. Detect renamed columns
renamed AS (
    SELECT n.*
    FROM new_data n
    JOIN old_data o
      ON n.schema_name = o.schema_name
     AND n.object_type_name = o.object_type_name
    WHERE n.object_md5 = o.object_md5
      AND n.object_subtype_name <> o.object_subtype_name
),

-- 3. Detect deleted columns
deleted AS (
    SELECT o.*
    FROM old_data o
    LEFT JOIN new_data n
      ON n.schema_name = o.schema_name
     AND n.object_type_name = o.object_type_name
     AND n.object_subtype_name = o.object_subtype_name
    WHERE n.object_subtype_name IS NULL
),

-- 4. Detect added columns (excluding modified & renamed)
added AS (
    SELECT n.*
    FROM new_data n
    LEFT JOIN old_data o
      ON n.schema_name = o.schema_name
     AND n.object_type_name = o.object_type_name
     AND n.object_subtype_name = o.object_subtype_name
    WHERE o.object_subtype_name IS NULL
      AND n.object_subtype_name NOT IN (
            SELECT object_subtype_name FROM modified
            UNION
            SELECT object_subtype_name FROM renamed
      )
)

-- 5. Combine results
SELECT *, 'ADDED' AS change_type FROM added
UNION ALL
SELECT *, 'DELETED' AS change_type FROM deleted
UNION ALL
SELECT *, 'MODIFIED' AS change_type FROM modified
UNION ALL
SELECT *, 'RENAMED' AS change_type FROM renamed;
