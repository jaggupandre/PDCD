-- db	    pdcd_db
-- user	    jagdish_pandre
-- password	
-- schema  	public

-- CREATE TABLE public.table_column_metadata_details (
--     metadata_id SERIAL PRIMARY KEY,               -- unique record id
--     schema_name TEXT NOT NULL,                    -- schema where table resides
--     table_name TEXT NOT NULL,                     -- table name
--     column_name TEXT NOT NULL,                    -- column name
--     data_type TEXT,                               -- column data type
--     max_length TEXT,                              -- character_maximum_length
--     numeric_precision TEXT,                       -- numeric precision (for numeric data types)
--     numeric_scale TEXT,                           -- numeric scale (for numeric data types)
--     nullable TEXT,                                -- YES/NO
--     default_value TEXT,                           -- default value or expression
--     is_identity TEXT,                             -- identity column flag
--     is_generated TEXT,                            -- generated (computed) column flag
--     generation_expression TEXT,                   -- expression for generated column
--     constraint_name TEXT,                         -- PK/FK/Unique constraint name if applicable
--     column_position INT,                          -- ordinal position in table
--     object_md5 TEXT,                                -- MD5 hash of the column metadata
--     captured_at TIMESTAMP DEFAULT clock_timestamp(),          -- metadata snapshot timestamp
--     snapshot_id INT REFERENCES public.snapshot_tbl(snapshot_id) -- link to snapshot table
-- );

DROP TABLE IF EXISTS pdcd_schema.snapshot_tbl;
CREATE TABLE pdcd_schema.snapshot_tbl (
  snapshot_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  snapshot_name TEXT NOT NULL,
  snapshot_process_time TIMESTAMP DEFAULT clock_timestamp()
);

ALTER TABLE pdcd_schema.md5_metadata_table
RENAME TO md5_metadata_tbl;

-- md5 metadata table (versioned)
CREATE TABLE IF NOT EXISTS pdcd_schema.md5_metadata_tbl (
  metadata_id BIGSERIAL PRIMARY KEY,
  snapshot_id INT NOT NULL REFERENCES pdcd_schema.snapshot_tbl(snapshot_id) ON DELETE CASCADE,
  schema_name TEXT NOT NULL,
  object_type TEXT NOT NULL,          -- TABLE, VIEW, FUNCTION, ...
  object_type_name TEXT NOT NULL,     -- table/view name
  object_subtype TEXT,                -- Column, Index, Trigger, ...
  object_subtype_name TEXT,           -- column name or index name
  object_subtype_details TEXT,        -- raw detail string
  object_md5 TEXT NOT NULL,           -- md5 fingerprint of the object_subtype_details (or full row)
  processed_time TIMESTAMP DEFAULT clock_timestamp()
--   change_type TEXT,   -- ADDED | MODIFIED | UNCHANGED | DELETED (we'll use ADDED/MODIFIED/DELETED on insert)
--   prev_object_md5 TEXT                -- store previous md5 for reference (NULL for new)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_md5_snapshot ON pdcd_schema.md5_metadata_tbl(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_md5_obj ON pdcd_schema.md5_metadata_tbl(schema_name, object_type, object_type_name, object_subtype, object_subtype_name, object_md5);
CREATE INDEX IF NOT EXISTS idx_md5_lookup ON pdcd_schema.md5_metadata_tbl(schema_name, object_type, object_type_name, object_subtype, object_subtype_name);

INSERT INTO pdcd_schema.snapshot_tbl(snapshot_name)
VALUES (
  'baseline_' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD')
)
RETURNING snapshot_id;


INSERT INTO pdcd_schema.md5_metadata_tbl (
    snapshot_id,
    schema_name,
    object_type,
    object_type_name,
    object_subtype,
    object_subtype_name,
    object_subtype_details,
    object_md5,
    change_type,
    prev_object_md5
)
SELECT
    1 AS snapshot_id,                               -- Replace with your latest snapshot_id
    c.table_schema AS schema_name,
    'Table' AS object_type,
    c.table_name AS object_type_name,
    'Column' AS object_subtype,
    c.column_name AS object_subtype_name,

    -- Build full object_subtype_details string
    (
        'schema_name:' || COALESCE(c.table_schema, '') ||
        ',table_name:' || COALESCE(c.table_name, '') ||
        ',column_name:' || COALESCE(c.column_name, '') ||
        ',data_type:' || COALESCE(c.data_type, '') ||
        ',max_length:' || COALESCE(c.character_maximum_length::TEXT, '') ||
        ',numeric_precision:' || COALESCE(c.numeric_precision::TEXT, '') ||
        ',numeric_scale:' || COALESCE(c.numeric_scale::TEXT, '') ||
        ',nullable:' || COALESCE(c.is_nullable, '') ||
        ',default_value:' || COALESCE(c.column_default, '') ||
        ',is_identity:' || COALESCE(c.is_identity, '') ||
        ',is_generated:' || COALESCE(c.is_generated, '') ||
        ',generation_expression:' || COALESCE(c.generation_expression, '') ||
        ',constraint_name:' || COALESCE(tc.constraint_name, '') ||
        ',column_position:' || c.ordinal_position::TEXT
    ) AS object_subtype_details,

    -- Compute MD5 hash for this detailed metadata
    md5(
        'schema_name:' || COALESCE(c.table_schema, '') ||
        ',table_name:' || COALESCE(c.table_name, '') ||
        ',column_name:' || COALESCE(c.column_name, '') ||
        ',data_type:' || COALESCE(c.data_type, '') ||
        ',max_length:' || COALESCE(c.character_maximum_length::TEXT, '') ||
        ',numeric_precision:' || COALESCE(c.numeric_precision::TEXT, '') ||
        ',numeric_scale:' || COALESCE(c.numeric_scale::TEXT, '') ||
        ',nullable:' || COALESCE(c.is_nullable, '') ||
        ',default_value:' || COALESCE(c.column_default, '') ||
        ',is_identity:' || COALESCE(c.is_identity, '') ||
        ',is_generated:' || COALESCE(c.is_generated, '') ||
        ',generation_expression:' || COALESCE(c.generation_expression, '') ||
        ',constraint_name:' || COALESCE(tc.constraint_name, '') ||
        ',column_position:' || c.ordinal_position::TEXT
    ) AS object_md5

    -- '' AS change_type,
    -- NULL AS prev_object_md5

FROM information_schema.columns c
LEFT JOIN information_schema.key_column_usage tc
    ON c.table_name = tc.table_name
   AND c.column_name = tc.column_name
   AND c.table_schema = tc.table_schema
WHERE c.table_schema = 'analytics_schema';
