-- Initialize the database schema
-- snapshot table (if not exists)
DB: test_db
user: test_user
pass: test_pass

DROP TABLE IF EXISTS pdcd_schema.snapshot_tbl;
CREATE TABLE pdcd_schema.snapshot_tbl (
  snapshot_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  snapshot_name TEXT NOT NULL,
  processed_time TIMESTAMP DEFAULT clock_timestamp()
);
INSERT INTO pdcd_schema.snapshot_tbl(snapshot_name)
VALUES (
  'baseline_' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD')
)
RETURNING snapshot_id;


--=================================================
-- Table: pdcd_schema.md5_metadata_tbl
--=================================================
DROP TABLE IF EXISTS pdcd_schema.md5_metadata_tbl;
CREATE TABLE pdcd_schema.md5_metadata_tbl (
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
  change_type TEXT DEFAULT 'ADDED'   -- ADDED | MODIFIED | UNCHANGED | DELETED (we'll use ADDED/MODIFIED/DELETED on insert)
);

--=================================================
-- Table: pdcd_schema.md5_metadata_staging_tbl
--=================================================
DROP TABLE IF EXISTS pdcd_schema.md5_metadata_staging_tbl;
CREATE TABLE pdcd_schema.md5_metadata_staging_tbl (
  metadata_id BIGSERIAL PRIMARY KEY,
  snapshot_id INT NOT NULL REFERENCES pdcd_schema.snapshot_tbl(snapshot_id) ON DELETE CASCADE,
  schema_name TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_type_name TEXT NOT NULL,
  object_subtype TEXT,
  object_subtype_name TEXT,
  object_subtype_details TEXT,
  object_md5 TEXT NOT NULL,
  processed_time TIMESTAMP DEFAULT clock_timestamp()
);
-- ==============================================================

--=================================================
-- Table: pdcd_schema.md5_metadata_staging_function
--=================================================
DROP TABLE IF EXISTS pdcd_schema.md5_metadata_staging_functions;
CREATE TABLE pdcd_schema.md5_metadata_staging_functions (
  metadata_id BIGSERIAL PRIMARY KEY,
  snapshot_id INT NOT NULL REFERENCES pdcd_schema.snapshot_tbl(snapshot_id) ON DELETE CASCADE,
  schema_name TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_type_name TEXT NOT NULL,
  object_subtype TEXT, -- Not Required
  object_subtype_name TEXT, -- Not Required
  object_subtype_details TEXT,
  object_md5 TEXT NOT NULL,
  processed_time TIMESTAMP DEFAULT clock_timestamp()
);


Create schema back_up_schema;

CREATE TABLE back_up_schema.md5_metadata_tbl AS
SELECT *
FROM pdcd_schema.md5_metadata_tbl;

CREATE TABLE back_up_schema.md5_metadata_staging_tbl AS
SELECT *
FROM pdcd_schema.md5_metadata_staging_tbl;

