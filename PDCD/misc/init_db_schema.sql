-- Initialize the database schema
-- snapshot table (if not exists)
DROP TABLE IF EXISTS pdcd_schema.snapshot_tbl;
CREATE TABLE pdcd_schema.snapshot_tbl (
  snapshot_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  snapshot_name TEXT NOT NULL,
  snapshot_process_time TIMESTAMP DEFAULT clock_timestamp()
);

-- pdcd_schema.md5_metadata_table
DROP TABLE IF EXISTS pdcd_schema.md5_metadata_table;
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

DROP TABLE IF EXISTS pdcd_schema.audit_md5_metadata;
CREATE TABLE IF NOT EXISTS pdcd_schema.audit_md5_metadata (
  metadata_id TEXT PRIMARY KEY,
  snapshot_id INT NOT NULL REFERENCES pdcd_schema.snapshot_tbl(snapshot_id) ON DELETE CASCADE,
  schema_name TEXT NOT NULL,
  object_type TEXT NOT NULL,          -- TABLE, VIEW, FUNCTION, ...
  object_type_name TEXT NOT NULL,     -- table/view name
  object_subtype TEXT,                -- Column, Index, Trigger, ...
  object_subtype_name TEXT,           -- column name or index name
  object_subtype_details TEXT,        -- raw detail string
  object_md5 TEXT NOT NULL,           -- md5 fingerprint of the object_subtype_details (or full row)
  processed_time TIMESTAMP DEFAULT clock_timestamp(),
  change_type TEXT   -- ADDED | MODIFIED | UNCHANGED | DELETED (we'll use ADDED/MODIFIED/DELETED on insert)
--   prev_object_md5 TEXT                -- store previous md5 for reference (NULL for new)
);
-- ==============================================================