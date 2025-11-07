drop function pdcd_schema.load_snapshot_tbl(TEXT[]);
drop function pdcd_schema.load_md5_metadata_tbl(TEXT[]);
drop function pdcd_schema.load_md5_metadata_staging_tbl(TEXT[]);

drop table pdcd_schema.snapshot_tbl;
drop table pdcd_schema.md5_metadata_tbl;
drop table pdcd_schema.md5_metadata_staging_tbl;

TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;

DROP table IF EXISTS analytics_schema.departments;
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL UNIQUE,
    location VARCHAR(100),
    primary_location VARCHAR(100),
    secondary_location VARCHAR(100),
    ternary_location VARCHAR(100)
);

-- First Run
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);


--! CHANGES: Renaming, Dropping, Adding columns  !--
    -- before_changes
    -- department_id | department_name | location | primary_location | secondary_location | ternary_location
    
    -- renaming column
    alter table analytics_schema.departments rename column location to location_old;
    --dropping column
    alter table analytics_schema.departments drop column ternary_location;
    --adding new column
    alter table analytics_schema.departments add column location_new TEXT;
    -- after_changes
    -- department_id | department_name | location_old | primary_location | secondary_location | location_new


-- Second Run and subsequent runs to compare 
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

--! Changes: Modifying column !--
    -- before_changes
    -- department_id | department_name | location_old | primary_location | secondary_location | location_new
    
    -- modifying column
    alter table analytics_schema.departments alter column department_name type VARCHAR(150);
    -- rename column
    alter table analytics_schema.departments rename column location_new to ternary_location;
    -- adding new column
    alter table analytics_schema.departments add column manager_id TEXT;
    -- dropping column
    alter table analytics_schema.departments drop column location_old;
    
    -- after_changes
    -- department_id | department_name | primary_location | secondary_location | ternary_location | manager_id

-- Third Run and subsequent runs to compare
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);


SELECT metadata_id, snapshot_id, schema_name, object_type, object_type_name, object_subtype, object_subtype_name,
-- object_subtype_details,
object_md5, processed_time, change_type
FROM pdcd_schema.md5_metadata_tbl
WHERE schema_name = 'analytics_schema'

