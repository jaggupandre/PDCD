drop function pdcd_schema.load_snapshot_tbl(TEXT[]);
drop function pdcd_schema.load_md5_metadata_tbl(TEXT[]);
drop function pdcd_schema.load_md5_metadata_staging_tbl(TEXT[]);

drop table pdcd_schema.snapshot_tbl;
drop table pdcd_schema.md5_metadata_tbl;
drop table pdcd_schema.md5_metadata_staging_tbl;

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;

-- ======================================================
-- TEST CASES: CONSTRAINTS METADATA VALIDATION
-- Purpose : To validate that constraint-related schema changes
--           are detected correctly by metadata comparison process
-- ======================================================

-- Clean start
DROP SCHEMA IF EXISTS analytics_schema CASCADE;
CREATE SCHEMA analytics_schema;

-- Base Tables (Before any changes)
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    budget_code VARCHAR(20)
);

CREATE TABLE analytics_schema.employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department_id INT
);

-- ZERO Run
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
select metadata_id, snapshot_id, object_type_name, object_subtype, object_subtype, object_subtype_name, object_md5, processed_time, change_type FROM pdcd_schema.md5_metadata_tbl;

 metadata_id | snapshot_id | object_type_name | object_subtype | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+----------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | Column         | Column         | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-14 13:03:23.406788 | ADDED
           2 |           1 | departments      | Column         | Column         | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-14 13:03:23.410126 | ADDED
           3 |           1 | departments      | Column         | Column         | budget_code         | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-14 13:03:23.410147 | ADDED
           4 |           1 | employees        | Column         | Column         | employee_id         | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-14 13:03:23.410153 | ADDED
           5 |           1 | employees        | Column         | Column         | first_name          | b537a75e4c2744b85e478bf937370ba9 | 2025-11-14 13:03:23.410159 | ADDED
           6 |           1 | employees        | Column         | Column         | last_name           | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-14 13:03:23.410181 | ADDED
           7 |           1 | employees        | Column         | Column         | department_id       | 23d773c000aa706b00d05e17a2b453dd | 2025-11-14 13:03:23.410187 | ADDED
           8 |           1 | departments      | Constraint     | Constraint     | departments_pkey    | 6f00aac61324af405b95307a19340808 | 2025-11-14 13:03:23.413649 | ADDED
           9 |           1 | employees        | Constraint     | Constraint     | employees_pkey      | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-14 13:03:23.413677 | ADDED

test_db=# \d+ analytics_schema.employees
                                                                          Table "analytics_schema.employees"
    Column     |         Type          | Collation | Nullable |                             Default                             | Storage  | Compression | Stats target | Description
---------------+-----------------------+-----------+----------+-----------------------------------------------------------------+----------+-------------+--------------+-------------
 employee_id   | integer               |           | not null | nextval('analytics_schema.employees_employee_id_seq'::regclass) | plain    |             |              |
 first_name    | character varying(50) |           |          |                                                                 | extended |             |              |
 last_name     | character varying(50) |           |          |                                                                 | extended |             |              |
 department_id | integer               |           |          |                                                                 | plain    |             |              |
Indexes:
    "employees_pkey" PRIMARY KEY, btree (employee_id)
Access method: heap

test_db=# \d+ analytics_schema.departments
                                                                            Table "analytics_schema.departments"
     Column      |          Type          | Collation | Nullable |                               Default                               | Storage  | Compression | Stats target | Description
-----------------+------------------------+-----------+----------+---------------------------------------------------------------------+----------+-------------+--------------+-------------
 department_id   | integer                |           | not null | nextval('analytics_schema.departments_department_id_seq'::regclass) | plain    |             |              |
 department_name | character varying(100) |           | not null |                                                                     | extended |             |              |
 budget_code     | character varying(20)  |           |          |                                                                     | extended |             |              |
Indexes:
    "departments_pkey" PRIMARY KEY, btree (department_id)
Access method: heap

-- ======================================================
-- TEST RUN 1: ADD new constraints
-- ======================================================
-- Expected Metadata Change: ADD for constraint objects

-- Add UNIQUE constraint
ALTER TABLE analytics_schema.departments
    ADD CONSTRAINT uq_department_name UNIQUE (department_name);
-- Add CHECK constraint
ALTER TABLE analytics_schema.departments
    ADD CONSTRAINT chk_budget_code CHECK (budget_code ~ '^[A-Z0-9]+$');

-- Add FOREIGN KEY constraint
ALTER TABLE analytics_schema.employees
    ADD CONSTRAINT fk_emp_dept FOREIGN KEY (department_id)
    REFERENCES analytics_schema.departments(department_id);
-- Add NOT NULL constraint
ALTER TABLE analytics_schema.employees
    ALTER COLUMN first_name SET NOT NULL; --this is a column level changes i.e. this changes will be captured in column as Modified


SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

select metadata_id, snapshot_id, object_type_name, object_subtype, object_subtype_name, object_md5, processed_time, change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | Column         | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-14 13:13:01.047559 | ADDED
           2 |           1 | departments      | Column         | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-14 13:13:01.053514 | ADDED
           3 |           1 | departments      | Column         | budget_code         | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-14 13:13:01.053536 | ADDED
           4 |           1 | employees        | Column         | employee_id         | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-14 13:13:01.053542 | ADDED
           5 |           1 | employees        | Column         | first_name          | b537a75e4c2744b85e478bf937370ba9 | 2025-11-14 13:13:01.053546 | ADDED
           6 |           1 | employees        | Column         | last_name           | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-14 13:13:01.053551 | ADDED
           7 |           1 | employees        | Column         | department_id       | 23d773c000aa706b00d05e17a2b453dd | 2025-11-14 13:13:01.053555 | ADDED
           8 |           1 | departments      | Constraint     | departments_pkey    | 6f00aac61324af405b95307a19340808 | 2025-11-14 13:13:01.055954 | ADDED
           9 |           1 | employees        | Constraint     | employees_pkey      | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-14 13:13:01.055982 | ADDED

          10 |           2 | departments      | Column         | department_name     | 3f6424e5a67d5056eefa66dd02360a96 | 2025-11-14 13:13:55.925285 | MODIFIED
          11 |           2 | employees        | Column         | department_id       | 9867d232b4f9a5e95b8776ce0ef0c5c8 | 2025-11-14 13:13:55.925338 | MODIFIED
          12 |           2 | employees        | Column         | first_name          | c920113b40e4428ce69da7212c98ee46 | 2025-11-14 13:13:55.925344 | MODIFIED
          13 |           2 | departments      | Constraint     | chk_budget_code     | 07fd21fccdf78d5531b5fe1a6125b037 | 2025-11-14 13:13:55.925397 | ADDED
          14 |           2 | departments      | Constraint     | uq_department_name  | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 13:13:55.925402 | ADDED
          15 |           2 | employees        | Constraint     | fk_emp_dept         | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-14 13:13:55.925407 | ADDED

test_db=# \d+ analytics_schema.departments
                                                                            Table "analytics_schema.departments"
     Column      |          Type          | Collation | Nullable |                               Default                               | Storage  | Compression | Stats target | Description
-----------------+------------------------+-----------+----------+---------------------------------------------------------------------+----------+-------------+--------------+-------------
 department_id   | integer                |           | not null | nextval('analytics_schema.departments_department_id_seq'::regclass) | plain    |             |              |
 department_name | character varying(100) |           | not null |                                                                     | extended |             |              |
 budget_code     | character varying(20)  |           |          |                                                                     | extended |             |              |
Indexes:
    "departments_pkey" PRIMARY KEY, btree (department_id)
    "uq_department_name" UNIQUE CONSTRAINT, btree (department_name)
Check constraints:
    "chk_budget_code" CHECK (budget_code::text ~ '^[A-Z0-9]+$'::text)
Referenced by:
    TABLE "analytics_schema.employees" CONSTRAINT "fk_emp_dept" FOREIGN KEY (department_id) REFERENCES analytics_schema.departments(department_id)
Access method: heap

test_db=# \d+ analytics_schema.employees
                                                                          Table "analytics_schema.employees"
    Column     |         Type          | Collation | Nullable |                             Default                             | Storage  | Compression | Stats target | Description
---------------+-----------------------+-----------+----------+-----------------------------------------------------------------+----------+-------------+--------------+-------------
 employee_id   | integer               |           | not null | nextval('analytics_schema.employees_employee_id_seq'::regclass) | plain    |             |              |
 first_name    | character varying(50) |           | not null |                                                                 | extended |             |              |
 last_name     | character varying(50) |           |          |                                                                 | extended |             |              |
 department_id | integer               |           |          |                                                                 | plain    |             |              |
Indexes:
    "employees_pkey" PRIMARY KEY, btree (employee_id)
Foreign-key constraints:
    "fk_emp_dept" FOREIGN KEY (department_id) REFERENCES analytics_schema.departments(department_id)
Access method: heap

-- ======================================================
-- TEST RUN 2: MODIFY constraints
-- ======================================================
-- Expected Metadata Change: MODIFIED for constraint object subtype

-- Drop old CHECK and add new pattern
ALTER TABLE analytics_schema.departments
    DROP CONSTRAINT chk_budget_code;

ALTER TABLE analytics_schema.departments
    ADD CONSTRAINT chk_budget_code CHECK (budget_code ~ '^[A-Z]{3}[0-9]{3}$');

-- Change FK behavior (drop and recreate with ON DELETE CASCADE)
ALTER TABLE analytics_schema.employees
    DROP CONSTRAINT fk_emp_dept;

ALTER TABLE analytics_schema.employees
    ADD CONSTRAINT fk_emp_dept FOREIGN KEY (department_id)
    REFERENCES analytics_schema.departments(department_id)
    ON DELETE CASCADE;

SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

test_db=# select metadata_id, snapshot_id, object_type_name, object_subtype, object_subtype_name, object_md5, processed_time, change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | Column         | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-14 13:13:01.047559 | ADDED
           2 |           1 | departments      | Column         | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-14 13:13:01.053514 | ADDED
           3 |           1 | departments      | Column         | budget_code         | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-14 13:13:01.053536 | ADDED
           4 |           1 | employees        | Column         | employee_id         | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-14 13:13:01.053542 | ADDED
           5 |           1 | employees        | Column         | first_name          | b537a75e4c2744b85e478bf937370ba9 | 2025-11-14 13:13:01.053546 | ADDED
           6 |           1 | employees        | Column         | last_name           | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-14 13:13:01.053551 | ADDED
           7 |           1 | employees        | Column         | department_id       | 23d773c000aa706b00d05e17a2b453dd | 2025-11-14 13:13:01.053555 | ADDED
           8 |           1 | departments      | Constraint     | departments_pkey    | 6f00aac61324af405b95307a19340808 | 2025-11-14 13:13:01.055954 | ADDED
           9 |           1 | employees        | Constraint     | employees_pkey      | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-14 13:13:01.055982 | ADDED

          10 |           2 | departments      | Column         | department_name     | 3f6424e5a67d5056eefa66dd02360a96 | 2025-11-14 13:13:55.925285 | MODIFIED
          11 |           2 | employees        | Column         | department_id       | 9867d232b4f9a5e95b8776ce0ef0c5c8 | 2025-11-14 13:13:55.925338 | MODIFIED
          12 |           2 | employees        | Column         | first_name          | c920113b40e4428ce69da7212c98ee46 | 2025-11-14 13:13:55.925344 | MODIFIED
          13 |           2 | departments      | Constraint     | chk_budget_code     | 07fd21fccdf78d5531b5fe1a6125b037 | 2025-11-14 13:13:55.925397 | ADDED
          14 |           2 | departments      | Constraint     | uq_department_name  | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 13:13:55.925402 | ADDED
          15 |           2 | employees        | Constraint     | fk_emp_dept         | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-14 13:13:55.925407 | ADDED

          16 |           3 | departments      | Constraint     | chk_budget_code     | 752637a13b9c9f03a6b0ccdd119bbbb2 | 2025-11-14 13:34:23.498988 | MODIFIED
          17 |           3 | employees        | Constraint     | fk_emp_dept         | 89df7ceb79051c0169615bee6c678bbe | 2025-11-14 13:34:23.499048 | MODIFIED


-- ======================================================
-- TEST RUN 3: DROP constraints
-- ======================================================
-- Expected Metadata Change: DELETED for constraint object subtype

ALTER TABLE analytics_schema.departments
    DROP CONSTRAINT uq_department_name;

ALTER TABLE analytics_schema.employees
    DROP CONSTRAINT fk_emp_dept;

ALTER TABLE analytics_schema.departments
    DROP CONSTRAINT chk_budget_code;

SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
select metadata_id, snapshot_id, object_type_name, object_subtype, object_subtype_name, object_md5, processed_time, change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | Column         | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-14 13:13:01.047559 | ADDED
           2 |           1 | departments      | Column         | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-14 13:13:01.053514 | ADDED
           3 |           1 | departments      | Column         | budget_code         | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-14 13:13:01.053536 | ADDED
           4 |           1 | employees        | Column         | employee_id         | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-14 13:13:01.053542 | ADDED
           5 |           1 | employees        | Column         | first_name          | b537a75e4c2744b85e478bf937370ba9 | 2025-11-14 13:13:01.053546 | ADDED
           6 |           1 | employees        | Column         | last_name           | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-14 13:13:01.053551 | ADDED
           7 |           1 | employees        | Column         | department_id       | 23d773c000aa706b00d05e17a2b453dd | 2025-11-14 13:13:01.053555 | ADDED
           8 |           1 | departments      | Constraint     | departments_pkey    | 6f00aac61324af405b95307a19340808 | 2025-11-14 13:13:01.055954 | ADDED
           9 |           1 | employees        | Constraint     | employees_pkey      | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-14 13:13:01.055982 | ADDED

          10 |           2 | departments      | Column         | department_name     | 3f6424e5a67d5056eefa66dd02360a96 | 2025-11-14 13:13:55.925285 | MODIFIED
          11 |           2 | employees        | Column         | department_id       | 9867d232b4f9a5e95b8776ce0ef0c5c8 | 2025-11-14 13:13:55.925338 | MODIFIED
          12 |           2 | employees        | Column         | first_name          | c920113b40e4428ce69da7212c98ee46 | 2025-11-14 13:13:55.925344 | MODIFIED
          13 |           2 | departments      | Constraint     | chk_budget_code     | 07fd21fccdf78d5531b5fe1a6125b037 | 2025-11-14 13:13:55.925397 | ADDED
          14 |           2 | departments      | Constraint     | uq_department_name  | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 13:13:55.925402 | ADDED
          15 |           2 | employees        | Constraint     | fk_emp_dept         | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-14 13:13:55.925407 | ADDED

          16 |           3 | departments      | Constraint     | chk_budget_code     | 752637a13b9c9f03a6b0ccdd119bbbb2 | 2025-11-14 13:34:23.498988 | MODIFIED
          17 |           3 | employees        | Constraint     | fk_emp_dept         | 89df7ceb79051c0169615bee6c678bbe | 2025-11-14 13:34:23.499048 | MODIFIED

          18 |           4 | departments      | Column         | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-14 13:48:46.043964 | MODIFIED
          19 |           4 | employees        | Column         | department_id       | 23d773c000aa706b00d05e17a2b453dd | 2025-11-14 13:48:46.044021 | MODIFIED
          20 |           4 | employees        | Constraint     | fk_emp_dept         | 89df7ceb79051c0169615bee6c678bbe | 2025-11-14 13:48:46.044092 | DELETED
          21 |           4 | departments      | Constraint     | uq_department_name  | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 13:48:46.044107 | DELETED
          22 |           4 | departments      | Constraint     | chk_budget_code     | 752637a13b9c9f03a6b0ccdd119bbbb2 | 2025-11-14 13:48:46.044112 | DELETED
-- ======================================================
-- TEST RUN 4: RENAME constraints
-- ======================================================
-- Expected Metadata Change: RENAMED for constraint object subtype

-- Recreate constraints to rename
ALTER TABLE analytics_schema.departments
    ADD CONSTRAINT uq_dept_name UNIQUE (department_name);

ALTER TABLE analytics_schema.employees
    ADD CONSTRAINT fk_emp_dept FOREIGN KEY (department_id)
    REFERENCES analytics_schema.departments(department_id);

-- Rename them
ALTER TABLE analytics_schema.departments
    RENAME CONSTRAINT uq_dept_name TO uq_department_title;

ALTER TABLE analytics_schema.employees
    RENAME CONSTRAINT fk_emp_dept TO fk_employee_department;

SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

test_db=# select metadata_id, snapshot_id, object_type_name, object_subtype, object_subtype_name, object_md5, processed_time, change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype |  object_subtype_name   |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+----------------+------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | Column         | department_id          | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-14 13:13:01.047559 | ADDED
           2 |           1 | departments      | Column         | department_name        | 5a841b9bbc928694255504765a33a956 | 2025-11-14 13:13:01.053514 | ADDED
           3 |           1 | departments      | Column         | budget_code            | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-14 13:13:01.053536 | ADDED
           4 |           1 | employees        | Column         | employee_id            | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-14 13:13:01.053542 | ADDED
           5 |           1 | employees        | Column         | first_name             | b537a75e4c2744b85e478bf937370ba9 | 2025-11-14 13:13:01.053546 | ADDED
           6 |           1 | employees        | Column         | last_name              | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-14 13:13:01.053551 | ADDED
           7 |           1 | employees        | Column         | department_id          | 23d773c000aa706b00d05e17a2b453dd | 2025-11-14 13:13:01.053555 | ADDED
           8 |           1 | departments      | Constraint     | departments_pkey       | 6f00aac61324af405b95307a19340808 | 2025-11-14 13:13:01.055954 | ADDED
           9 |           1 | employees        | Constraint     | employees_pkey         | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-14 13:13:01.055982 | ADDED
          10 |           2 | departments      | Column         | department_name        | 3f6424e5a67d5056eefa66dd02360a96 | 2025-11-14 13:13:55.925285 | MODIFIED
          11 |           2 | employees        | Column         | department_id          | 9867d232b4f9a5e95b8776ce0ef0c5c8 | 2025-11-14 13:13:55.925338 | MODIFIED
          12 |           2 | employees        | Column         | first_name             | c920113b40e4428ce69da7212c98ee46 | 2025-11-14 13:13:55.925344 | MODIFIED
          13 |           2 | departments      | Constraint     | chk_budget_code        | 07fd21fccdf78d5531b5fe1a6125b037 | 2025-11-14 13:13:55.925397 | ADDED
          14 |           2 | departments      | Constraint     | uq_department_name     | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 13:13:55.925402 | ADDED
          15 |           2 | employees        | Constraint     | fk_emp_dept            | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-14 13:13:55.925407 | ADDED
          16 |           3 | departments      | Constraint     | chk_budget_code        | 752637a13b9c9f03a6b0ccdd119bbbb2 | 2025-11-14 13:34:23.498988 | MODIFIED
          17 |           3 | employees        | Constraint     | fk_emp_dept            | 89df7ceb79051c0169615bee6c678bbe | 2025-11-14 13:34:23.499048 | MODIFIED
          18 |           4 | departments      | Column         | department_name        | 5a841b9bbc928694255504765a33a956 | 2025-11-14 13:48:46.043964 | MODIFIED
          19 |           4 | employees        | Column         | department_id          | 23d773c000aa706b00d05e17a2b453dd | 2025-11-14 13:48:46.044021 | MODIFIED
          20 |           4 | employees        | Constraint     | fk_emp_dept            | 89df7ceb79051c0169615bee6c678bbe | 2025-11-14 13:48:46.044092 | DELETED
          21 |           4 | departments      | Constraint     | uq_department_name     | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 13:48:46.044107 | DELETED
          22 |           4 | departments      | Constraint     | chk_budget_code        | 752637a13b9c9f03a6b0ccdd119bbbb2 | 2025-11-14 13:48:46.044112 | DELETED

          23 |           5 | departments      | Column         | department_name        | fb2669e00867e9770987bdd9518553eb | 2025-11-14 14:46:58.994843 | MODIFIED
          24 |           5 | employees        | Column         | department_id          | 59fd7e65d7bb3798fcec7eef36dbe533 | 2025-11-14 14:46:58.994875 | MODIFIED
          25 |           5 | departments      | Constraint     | uq_department_title    | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 14:46:58.994922 | ADDED
          26 |           5 | employees        | Constraint     | fk_employee_department | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-14 14:46:58.994929 | ADDED
-- ======================================================
-- TEST RUN 5: MIXED constraint operations
-- ======================================================
-- Expected Metadata Change: MIXED (ADD, DELETE, MODIFY)
-- Add a new CHECK constraint on employees
ALTER TABLE analytics_schema.employees
    ADD CONSTRAINT chk_salary_positive CHECK (employee_id > 0);
-- Drop a constraint
ALTER TABLE analytics_schema.departments
    DROP CONSTRAINT uq_department_title;
-- Modify existing CHECK constraint logic
ALTER TABLE analytics_schema.employees
    DROP CONSTRAINT chk_salary_positive;
ALTER TABLE analytics_schema.employees
    ADD CONSTRAINT chk_salary_positive CHECK (employee_id >= 1);
    
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
test_db=# select metadata_id, snapshot_id, object_type_name, object_subtype, object_subtype_name, object_md5, processed_time, change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype |  object_subtype_name   |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+----------------+------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | Column         | department_id          | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-14 13:13:01.047559 | ADDED
           2 |           1 | departments      | Column         | department_name        | 5a841b9bbc928694255504765a33a956 | 2025-11-14 13:13:01.053514 | ADDED
           3 |           1 | departments      | Column         | budget_code            | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-14 13:13:01.053536 | ADDED
           4 |           1 | employees        | Column         | employee_id            | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-14 13:13:01.053542 | ADDED
           5 |           1 | employees        | Column         | first_name             | b537a75e4c2744b85e478bf937370ba9 | 2025-11-14 13:13:01.053546 | ADDED
           6 |           1 | employees        | Column         | last_name              | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-14 13:13:01.053551 | ADDED
           7 |           1 | employees        | Column         | department_id          | 23d773c000aa706b00d05e17a2b453dd | 2025-11-14 13:13:01.053555 | ADDED
           8 |           1 | departments      | Constraint     | departments_pkey       | 6f00aac61324af405b95307a19340808 | 2025-11-14 13:13:01.055954 | ADDED
           9 |           1 | employees        | Constraint     | employees_pkey         | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-14 13:13:01.055982 | ADDED
          10 |           2 | departments      | Column         | department_name        | 3f6424e5a67d5056eefa66dd02360a96 | 2025-11-14 13:13:55.925285 | MODIFIED
          11 |           2 | employees        | Column         | department_id          | 9867d232b4f9a5e95b8776ce0ef0c5c8 | 2025-11-14 13:13:55.925338 | MODIFIED
          12 |           2 | employees        | Column         | first_name             | c920113b40e4428ce69da7212c98ee46 | 2025-11-14 13:13:55.925344 | MODIFIED
          13 |           2 | departments      | Constraint     | chk_budget_code        | 07fd21fccdf78d5531b5fe1a6125b037 | 2025-11-14 13:13:55.925397 | ADDED
          14 |           2 | departments      | Constraint     | uq_department_name     | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 13:13:55.925402 | ADDED
          15 |           2 | employees        | Constraint     | fk_emp_dept            | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-14 13:13:55.925407 | ADDED
          16 |           3 | departments      | Constraint     | chk_budget_code        | 752637a13b9c9f03a6b0ccdd119bbbb2 | 2025-11-14 13:34:23.498988 | MODIFIED
          17 |           3 | employees        | Constraint     | fk_emp_dept            | 89df7ceb79051c0169615bee6c678bbe | 2025-11-14 13:34:23.499048 | MODIFIED
          18 |           4 | departments      | Column         | department_name        | 5a841b9bbc928694255504765a33a956 | 2025-11-14 13:48:46.043964 | MODIFIED
          19 |           4 | employees        | Column         | department_id          | 23d773c000aa706b00d05e17a2b453dd | 2025-11-14 13:48:46.044021 | MODIFIED
          20 |           4 | employees        | Constraint     | fk_emp_dept            | 89df7ceb79051c0169615bee6c678bbe | 2025-11-14 13:48:46.044092 | DELETED
          21 |           4 | departments      | Constraint     | uq_department_name     | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 13:48:46.044107 | DELETED
          22 |           4 | departments      | Constraint     | chk_budget_code        | 752637a13b9c9f03a6b0ccdd119bbbb2 | 2025-11-14 13:48:46.044112 | DELETED
          23 |           5 | departments      | Column         | department_name        | fb2669e00867e9770987bdd9518553eb | 2025-11-14 14:46:58.994843 | MODIFIED
          24 |           5 | employees        | Column         | department_id          | 59fd7e65d7bb3798fcec7eef36dbe533 | 2025-11-14 14:46:58.994875 | MODIFIED
          25 |           5 | departments      | Constraint     | uq_department_title    | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 14:46:58.994922 | ADDED
          26 |           5 | employees        | Constraint     | fk_employee_department | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-14 14:46:58.994929 | ADDED

          27 |           6 | departments      | Column         | department_name        | 5a841b9bbc928694255504765a33a956 | 2025-11-14 14:51:45.474365 | MODIFIED
          28 |           6 | employees        | Constraint     | chk_salary_positive    | d83b08bb558984ce6f2f61b2d4552e30 | 2025-11-14 14:51:45.474439 | ADDED
          29 |           6 | departments      | Constraint     | uq_department_title    | 248d00bcee18e82bd994a8063b86a94e | 2025-11-14 14:51:45.474475 | DELETED

-- ======================================================
-- VALIDATION QUERIES
-- ======================================================

-- List all constraints for current schema
SELECT 
    n.nspname AS schema_name,
    c.relname AS table_name,
    con.conname AS constraint_name,
    CASE con.contype
        WHEN 'p' THEN 'PRIMARY KEY'
        WHEN 'u' THEN 'UNIQUE'
        WHEN 'f' THEN 'FOREIGN KEY'
        WHEN 'c' THEN 'CHECK'
        ELSE con.contype::TEXT
    END AS constraint_type,
    pg_get_constraintdef(con.oid, true) AS definition
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'analytics_schema'
ORDER BY n.nspname, c.relname, con.conname;

-- Verify metadata table captures differences
-- (Run your function here if applicable)
-- SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl();

-- ======================================================
-- END OF SCRIPT
-- ======================================================
