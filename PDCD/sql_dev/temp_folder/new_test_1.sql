drop function pdcd_schema.load_snapshot_tbl(TEXT[]);
drop function pdcd_schema.load_md5_metadata_tbl(TEXT[]);
drop function pdcd_schema.load_md5_metadata_staging_tbl(TEXT[]);

drop table pdcd_schema.snapshot_tbl;
drop table pdcd_schema.md5_metadata_tbl;
drop table pdcd_schema.md5_metadata_staging_tbl;

TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;

DROP table IF EXISTS analytics_schema.departments;
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    main_location VARCHAR(100),
    ternary_location VARCHAR(100),
    manager_id INT,
    budget_code VARCHAR(50)
);

-- Test Run 1 — Initial Additions & Modifications
ALTER TABLE analytics_schema.departments ADD COLUMN region TEXT;
ALTER TABLE analytics_schema.departments ADD COLUMN established_year INTEGER;
ALTER TABLE analytics_schema.departments ALTER COLUMN department_name TYPE VARCHAR(150);
ALTER TABLE analytics_schema.departments ALTER COLUMN main_location SET DEFAULT 'Head Office';
--  Purpose: initial schema growth + default and data type changes.

-- Test Run 2 — Renaming and Adding
ALTER TABLE analytics_schema.departments RENAME COLUMN region TO department_region;
ALTER TABLE analytics_schema.departments RENAME COLUMN established_year TO founded_year;
ALTER TABLE analytics_schema.departments ADD COLUMN last_updated_by TEXT;
ALTER TABLE analytics_schema.departments ADD COLUMN budget_allocated NUMERIC(12,2);
--  Purpose: check detection of renames and new column additions in same run.

-- Test Run 3 — Type, Default, and Rename Updates
ALTER TABLE analytics_schema.departments ALTER COLUMN budget_allocated TYPE FLOAT;
ALTER TABLE analytics_schema.departments ALTER COLUMN founded_year SET DEFAULT EXTRACT(YEAR FROM NOW());
ALTER TABLE analytics_schema.departments RENAME COLUMN last_updated_by TO updated_by;
ALTER TABLE analytics_schema.departments ALTER COLUMN updated_by SET DEFAULT CURRENT_USER;
--  Purpose: test multiple attribute updates (type, default, rename).

-- Test Run 4 — Dropping and Adding Back
ALTER TABLE analytics_schema.departments DROP COLUMN updated_by;
ALTER TABLE analytics_schema.departments DROP COLUMN budget_allocated;
ALTER TABLE analytics_schema.departments ADD COLUMN total_employees INTEGER;
ALTER TABLE analytics_schema.departments ADD COLUMN active_status BOOLEAN DEFAULT TRUE;
--  Purpose: test column removal + re-creation patterns — very common in evolution.

-- Test Run 5 — Final Mixed Changes
ALTER TABLE analytics_schema.departments RENAME COLUMN total_employees TO headcount;
ALTER TABLE analytics_schema.departments ALTER COLUMN headcount TYPE BIGINT;
ALTER TABLE analytics_schema.departments DROP COLUMN active_status;
ALTER TABLE analytics_schema.departments ADD COLUMN remarks TEXT;
--  Purpose: simulate rename + modify + drop + add together — final full-cycle test.