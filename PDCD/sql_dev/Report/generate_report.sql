WITH 
-- pick snapshots to compare
params AS (
    SELECT 
        'snapshot_20251101_120000'::text AS old_snapshot,
        'snapshot_20251106_120000'::text AS new_snapshot
),

-- expand old snapshot columns
old_cols AS (
    SELECT 
        schema_name,
        object_type_name AS table_name,
        col->>'column_name' AS column_name,
        col->>'data_type' AS data_type,
        col->>'max_length' AS max_length,
        col->>'numeric_precision' AS numeric_precision,
        col->>'numeric_scale' AS numeric_scale
    FROM md5_metadata_table m, params,
         jsonb_array_elements(m.object_subtype_details) AS col
    WHERE m.snapshot_name = params.old_snapshot
),

-- expand new snapshot columns
new_cols AS (
    SELECT 
        schema_name,
        object_type_name AS table_name,
        col->>'column_name' AS column_name,
        col->>'data_type' AS data_type,
        col->>'max_length' AS max_length,
        col->>'numeric_precision' AS numeric_precision,
        col->>'numeric_scale' AS numeric_scale
    FROM md5_metadata_table m, params,
         jsonb_array_elements(m.object_subtype_details) AS col
    WHERE m.snapshot_name = params.new_snapshot
),

-- added columns: in new but not in old
added AS (
    SELECT n.schema_name, n.table_name, n.column_name, 'added' AS change_type
    FROM new_cols n
    LEFT JOIN old_cols o 
      ON o.schema_name = n.schema_name 
     AND o.table_name = n.table_name 
     AND o.column_name = n.column_name
    WHERE o.column_name IS NULL
),

-- deleted columns: in old but not in new
deleted AS (
    SELECT o.schema_name, o.table_name, o.column_name, 'deleted' AS change_type
    FROM old_cols o
    LEFT JOIN new_cols n 
      ON n.schema_name = o.schema_name 
     AND n.table_name = o.table_name 
     AND n.column_name = o.column_name
    WHERE n.column_name IS NULL
),

-- modified columns: name same but datatype/length/precision changed
modified AS (
    SELECT 
        n.schema_name,
        n.table_name,
        n.column_name,
        'modified' AS change_type
    FROM new_cols n
    JOIN old_cols o 
      ON o.schema_name = n.schema_name 
     AND o.table_name = n.table_name 
     AND o.column_name = n.column_name
    WHERE (n.data_type, n.max_length, n.numeric_precision, n.numeric_scale)
        IS DISTINCT FROM 
          (o.data_type, o.max_length, o.numeric_precision, o.numeric_scale)
),

-- renamed columns: heuristic — data_type same but column name changed
renamed AS (
    SELECT 
        n.schema_name,
        n.table_name,
        o.column_name AS old_column_name,
        n.column_name AS new_column_name,
        'renamed' AS change_type
    FROM new_cols n
    JOIN old_cols o 
      ON o.schema_name = n.schema_name 
     AND o.table_name = n.table_name
     AND o.data_type = n.data_type
     AND (o.max_length, o.numeric_precision, o.numeric_scale) = 
         (n.max_length, n.numeric_precision, n.numeric_scale)
    WHERE o.column_name <> n.column_name
),

-- combine all
union_all_changes AS (
    SELECT schema_name, table_name, column_name, change_type FROM added
    UNION ALL
    SELECT schema_name, table_name, column_name, change_type FROM deleted
    UNION ALL
    SELECT schema_name, table_name, column_name, change_type FROM modified
),

-- count summary
summary AS (
    SELECT 
        schema_name,
        table_name,
        change_type,
        COUNT(*) AS total_changes
    FROM union_all_changes
    GROUP BY schema_name, table_name, change_type
)

SELECT 
    s.schema_name,
    s.table_name,
    s.change_type,
    s.total_changes,
    COALESCE(r.old_column_name || ' → ' || r.new_column_name, '') AS rename_details
FROM summary s
LEFT JOIN renamed r 
  ON r.schema_name = s.schema_name 
 AND r.table_name = s.table_name
ORDER BY schema_name, table_name, change_type;
