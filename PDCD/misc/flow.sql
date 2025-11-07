-- userName: jagdish_pandre
-- password: 
-- database: test_db
-- schema: analytics_schema



-- Goals recap:
-- On first run, capture a full baseline.
-- On subsequent runs, store only new or changed rows (based on object_md5).
-- Be able to compute what changed (ADDED, MODIFIED, DELETED) between any two snapshots.

-- snapshot table (if not exists)
DROP TABLE IF EXISTS pdcd_schema.snapshot_tbl;
CREATE TABLE pdcd_schema.snapshot_tbl (
  snapshot_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  snapshot_name TEXT NOT NULL,
  snapshot_process_time TIMESTAMP DEFAULT clock_timestamp()
);


-- md5 metadata table (versioned)
CREATE TABLE IF NOT EXISTS pdcd_schema.md5_metadata_table (
  metadata_id BIGSERIAL PRIMARY KEY,
  snapshot_id INT NOT NULL REFERENCES pdcd_schema.snapshot_tbl(snapshot_id) ON DELETE CASCADE,
  schema_name TEXT NOT NULL,
  object_type TEXT NOT NULL,          -- TABLE, VIEW, FUNCTION, ...
  object_type_name TEXT NOT NULL,     -- table/view name
  object_subtype TEXT,                -- Column, Index, Trigger, ...
  object_subtype_name TEXT,           -- column name or index name
  object_subtype_details TEXT,        -- raw detail string
  object_md5 TEXT NOT NULL,           -- md5 fingerprint of the object_subtype_details (or full row)
  processed_time TIMESTAMP DEFAULT clock_timestamp(),
  change_type TEXT DEFAULT 'ADDED',   -- ADDED | MODIFIED | UNCHANGED | DELETED (we'll use ADDED/MODIFIED/DELETED on insert)
  prev_object_md5 TEXT                -- store previous md5 for reference (NULL for new)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_md5_snapshot ON pdcd_schema.md5_metadata_table(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_md5_obj ON pdcd_schema.md5_metadata_table(schema_name, object_type, object_type_name, object_subtype, object_subtype_name, object_md5);
CREATE INDEX IF NOT EXISTS idx_md5_lookup ON pdcd_schema.md5_metadata_table(schema_name, object_type, object_type_name, object_subtype, object_subtype_name);

-- ==============================================================
-- I) Baseline (first run) — capture everything
-- ==============================================================
-- 1. Create a new snapshot row and capture the returned snapshot_id:
INSERT INTO pdcd_schema.snapshot_tbl(snapshot_name)
VALUES (
  'baseline_' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD')
)
RETURNING snapshot_id;
-- Assume you get snapshot_id = :new_snapshot_id.

-- 2. Extract current metadata into a temporary staging table and compute object_md5. 
-- Example (customize extraction to match your object_subtype_details construction):
DROP TABLE IF EXISTS tmp_md5_metadata;
CREATE TEMP TABLE tmp_md5_metadata AS
SELECT
  schema_name,
  object_type,
  object_type_name,
  object_subtype,
  object_subtype_name,
  object_subtype_details,
  md5(object_subtype_details) AS object_md5
FROM (
  -- Replace this subquery with your exact metadata generation logic:
  SELECT
    c.table_schema AS schema_name,
    'Table' AS object_type,
    c.table_name AS object_type_name,
    'Column' AS object_subtype,
    c.column_name AS object_subtype_name,
            CONCAT_WS(
            ',',
            CONCAT('schema_name:', COALESCE(c.table_schema, '')),
            CONCAT('table_name:', COALESCE(c.table_name, '')),
            CONCAT('column_name:', COALESCE(c.column_name, '')),
            CONCAT('data_type:', COALESCE(c.data_type, '')),
            CONCAT('max_length:', COALESCE(c.character_maximum_length::TEXT, '')),
            CONCAT('numeric_precision:', COALESCE(c.numeric_precision::TEXT, '')),
            CONCAT('numeric_scale:', COALESCE(c.numeric_scale::TEXT, '')),
            CONCAT('nullable:', COALESCE(c.is_nullable, '')),
            CONCAT('default_value:', COALESCE(c.column_default, '')),
            CONCAT('is_identity:', COALESCE(c.is_identity, '')),
            CONCAT('is_generated:', COALESCE(c.is_generated, '')),
            CONCAT('generation_expression:', COALESCE(c.generation_expression, '')),
            CONCAT('constraint_name:', COALESCE(tc.constraint_name, '')),
            CONCAT('ordinal_position:', c.ordinal_position::TEXT)
        ) AS object_subtype_details
  FROM information_schema.columns c
  WHERE c.table_schema = 'analytics_schema' -- or other filter; use all schemas if needed
) s;

-- 3. Insert all rows from tmp_md5_metadata into the metadata table with change_type = 'ADDED' and the snapshot reference:
INSERT INTO pdcd_schema.md5_metadata_table (
  snapshot_id,
  schema_name, object_type, object_type_name,
  object_subtype, object_subtype_name,
  object_subtype_details, object_md5, change_type, prev_object_md5
)
SELECT
  1 /*:new_snapshot_id*/,
  schema_name, object_type, object_type_name,
  object_subtype, object_subtype_name,
  object_subtype_details, object_md5, 'ADDED', NULL
FROM tmp_md5_metadata;

TRUNCATE TABLE pdcd_schema.md5_metadata_table CASCADE;
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE tmp_md5_metadata;
DROP TABLE tmp_md5_metadata;


-- III) Subsequent run — insert only differences (NEW / MODIFIED) and record deletions
-- a) Create a new snapshot row
INSERT INTO pdcd_schema.snapshot_tbl(snapshot_name)
VALUES (
  'baseline_' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD')
)
RETURNING snapshot_id;
-- use returned :new_snapshot_id

-- b) Build tmp_md5_metadata again (same query as baseline), compute object_md5.
-- c) Insert NEW or MODIFIED records — compare against the latest snapshot (not all historical rows)

-- get previous snapshot id
WITH prev_snapshot AS (
  SELECT snapshot_id FROM pdcd_schema.snapshot_tbl
  WHERE snapshot_id < :new_snapshot_id
  ORDER BY snapshot_id DESC LIMIT 1
)
INSERT INTO pdcd_schema.md5_metadata_table (
  snapshot_id,
  schema_name, object_type, object_type_name,
  object_subtype, object_subtype_name,
  object_subtype_details, object_md5, change_type, prev_object_md5
)
SELECT
  3, --:new_snapshot_id,
  t.schema_name, t.object_type, t.object_type_name,
  t.object_subtype, t.object_subtype_name,
  t.object_subtype_details, t.object_md5,
  CASE WHEN p.object_md5 IS NULL THEN 'ADDED' ELSE 'MODIFIED' END AS change_type,
  p.object_md5 AS prev_object_md5
FROM tmp_md5_metadata t
LEFT JOIN (
  -- fetch last known object_md5 per object (from previous snapshot)
  SELECT schema_name, object_type, object_type_name, object_subtype, object_subtype_name, object_md5
  FROM pdcd_schema.md5_metadata_table
  WHERE snapshot_id = 2 --(SELECT snapshot_id FROM prev_snapshot)
) p
  ON t.schema_name = p.schema_name
  AND t.object_type = p.object_type
  AND t.object_type_name = p.object_type_name
  AND t.object_subtype = p.object_subtype
  AND t.object_subtype_name = p.object_subtype_name
WHERE p.object_md5 IS DISTINCT FROM t.object_md5
   OR p.object_md5 IS NULL;  -- new objects



SELECT
  c.table_schema AS schema_name,
  'Table' AS object_type,
  c.table_name AS object_type_name,
  'Column' AS object_subtype,
  c.column_name AS object_subtype_name,
  -- compose details string exactly as you used previously:
  ('data_type:' || COALESCE(c.data_type,'') || ',is_nullable:' || COALESCE(c.is_nullable,'') ||
    ',column_default:' || COALESCE(c.column_default,'') || ',character_maximum_length:' || COALESCE(c.character_maximum_length::TEXT,'') ||
    ',ordinal_position:' || c.ordinal_position::TEXT) AS object_subtype_details
FROM information_schema.columns c
WHERE c.table_schema = 'analytics_schema'