drop function pdcd_schema.load_snapshot_tbl(TEXT[]);
drop function pdcd_schema.load_md5_metadata_tbl(TEXT[]);
drop function pdcd_schema.load_md5_metadata_staging_tbl(TEXT[]);

drop table pdcd_schema.snapshot_tbl;
drop table pdcd_schema.md5_metadata_tbl;
drop table pdcd_schema.md5_metadata_staging_tbl;

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;

--- =============================
--- ========= TEST CASES ========
--- =============================
DROP table IF EXISTS analytics_schema.employees;
DROP table IF EXISTS analytics_schema.departments;

CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    main_location VARCHAR(100),
    ternary_location VARCHAR(100),
    manager_id INT,
    budget_code VARCHAR(50)
);

CREATE TABLE analytics_schema.employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100),
    email VARCHAR(150) UNIQUE NOT NULL,
    phone_number VARCHAR(20),
    hire_date DATE NOT NULL DEFAULT CURRENT_DATE,
    salary NUMERIC(10,2),
    department_id INT NOT NULL REFERENCES analytics_schema.departments(department_id)
);

-- First Run
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    processed_time,    change_type
from pdcd_schema.md5_metadata_tbl; 
 snapshot_id |   schema_name    | object_type_name | object_subtype |     object_subtype_name      |            object_md5            |       processed_time       | change_type
-------------+------------------+------------------+----------------+------------------------------+----------------------------------+----------------------------+-------------
           1 | analytics_schema | departments      | Column         | department_id                | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-17 22:45:17.675502 | ADDED
           1 | analytics_schema | departments      | Column         | department_name              | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683473 | ADDED
           1 | analytics_schema | departments      | Column         | main_location                | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683498 | ADDED
           1 | analytics_schema | departments      | Column         | ternary_location             | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-17 22:45:17.683502 | ADDED
           1 | analytics_schema | departments      | Column         | manager_id                   | 840f65e90f5ad043ed195535e39dc868 | 2025-11-17 22:45:17.683506 | ADDED
           1 | analytics_schema | departments      | Column         | budget_code                  | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-17 22:45:17.68351  | ADDED
           1 | analytics_schema | employees        | Column         | employee_id                  | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-17 22:45:17.683514 | ADDED
           1 | analytics_schema | employees        | Column         | first_name                   | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683518 | ADDED
           1 | analytics_schema | employees        | Column         | last_name                    | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683521 | ADDED
           1 | analytics_schema | employees        | Column         | email                        | dbd03119c5e663a65d2215ea7cce415c | 2025-11-17 22:45:17.683952 | ADDED
           1 | analytics_schema | employees        | Column         | phone_number                 | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-17 22:45:17.683959 | ADDED
           1 | analytics_schema | employees        | Column         | hire_date                    | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-17 22:45:17.683963 | ADDED
           1 | analytics_schema | employees        | Column         | salary                       | 400aef400f3cb973b96caa66c27d549d | 2025-11-17 22:45:17.683967 | ADDED
           1 | analytics_schema | employees        | Column         | department_id                | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-17 22:45:17.683971 | ADDED
           1 | analytics_schema | departments      | Constraint     | departments_pkey             | 6f00aac61324af405b95307a19340808 | 2025-11-17 22:45:17.686418 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_department_id_fkey | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-17 22:45:17.686442 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_email_key          | 7366006ed8864803a9082211343f7a9d | 2025-11-17 22:45:17.686446 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_pkey               | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-17 22:45:17.686449 | ADDED
           1 | analytics_schema | departments      | Index          | departments_pkey             | b42e577e3d023d664d051edb062f8b89 | 2025-11-17 22:45:17.690131 | ADDED
           1 | analytics_schema | employees        | Index          | employees_email_key          | 46afff75141550028b030f851dff4823 | 2025-11-17 22:45:17.690165 | ADDED
           1 | analytics_schema | employees        | Index          | employees_pkey               | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-17 22:45:17.69017  | ADDED

--! Test Run 1 — Initial Additions & Modifications
-- 1. Create index on single column
CREATE INDEX idx_departments_main_location 
    ON analytics_schema.departments(main_location);
-- 2. Create unique index on budget_code
CREATE UNIQUE INDEX idx_departments_budget_code 
    ON analytics_schema.departments(budget_code);
-- 3. Create composite index
CREATE INDEX idx_employees_name 
    ON analytics_schema.employees(first_name, last_name);
CREATE INDEX idx_employees_lower_email 
    ON analytics_schema.employees(email);
-- Expected changes
-- 4 new entries with object_type = 'INDEX' and change_type = 'ADDED'.
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    processed_time,    change_type
from pdcd_schema.md5_metadata_tbl;
 snapshot_id |   schema_name    | object_type_name | object_subtype |      object_subtype_name      |            object_md5            |       processed_time       | change_type
-------------+------------------+------------------+----------------+-------------------------------+----------------------------------+----------------------------+-------------
           1 | analytics_schema | departments      | Column         | department_id                 | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-17 22:45:17.675502 | ADDED
           1 | analytics_schema | departments      | Column         | department_name               | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683473 | ADDED
           1 | analytics_schema | departments      | Column         | main_location                 | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683498 | ADDED
           1 | analytics_schema | departments      | Column         | ternary_location              | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-17 22:45:17.683502 | ADDED
           1 | analytics_schema | departments      | Column         | manager_id                    | 840f65e90f5ad043ed195535e39dc868 | 2025-11-17 22:45:17.683506 | ADDED
           1 | analytics_schema | departments      | Column         | budget_code                   | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-17 22:45:17.68351  | ADDED
           1 | analytics_schema | employees        | Column         | employee_id                   | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-17 22:45:17.683514 | ADDED
           1 | analytics_schema | employees        | Column         | first_name                    | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683518 | ADDED
           1 | analytics_schema | employees        | Column         | last_name                     | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683521 | ADDED
           1 | analytics_schema | employees        | Column         | email                         | dbd03119c5e663a65d2215ea7cce415c | 2025-11-17 22:45:17.683952 | ADDED
           1 | analytics_schema | employees        | Column         | phone_number                  | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-17 22:45:17.683959 | ADDED
           1 | analytics_schema | employees        | Column         | hire_date                     | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-17 22:45:17.683963 | ADDED
           1 | analytics_schema | employees        | Column         | salary                        | 400aef400f3cb973b96caa66c27d549d | 2025-11-17 22:45:17.683967 | ADDED
           1 | analytics_schema | employees        | Column         | department_id                 | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-17 22:45:17.683971 | ADDED
           1 | analytics_schema | departments      | Constraint     | departments_pkey              | 6f00aac61324af405b95307a19340808 | 2025-11-17 22:45:17.686418 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_department_id_fkey  | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-17 22:45:17.686442 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_email_key           | 7366006ed8864803a9082211343f7a9d | 2025-11-17 22:45:17.686446 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_pkey                | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-17 22:45:17.686449 | ADDED
           1 | analytics_schema | departments      | Index          | departments_pkey              | b42e577e3d023d664d051edb062f8b89 | 2025-11-17 22:45:17.690131 | ADDED
           1 | analytics_schema | employees        | Index          | employees_email_key           | 46afff75141550028b030f851dff4823 | 2025-11-17 22:45:17.690165 | ADDED
           1 | analytics_schema | employees        | Index          | employees_pkey                | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-17 22:45:17.69017  | ADDED
           2 | analytics_schema | departments      | Index          | idx_departments_budget_code   | 91036992bfa3cd533523940d8aa98116 | 2025-11-17 22:57:44.722996 | ADDED
           2 | analytics_schema | departments      | Index          | idx_departments_main_location | a00aa80d1a572c83f77edc6c8ea0bec4 | 2025-11-17 22:57:44.723047 | ADDED
           2 | analytics_schema | employees        | Index          | idx_employees_lower_email     | bfb32be21e32a409065bd56837323526 | 2025-11-17 22:57:44.723053 | ADDED
           2 | analytics_schema | employees        | Index          | idx_employees_name            | e2302f1fc86d6fbfd26e9bfbb5dd3847 | 2025-11-17 22:57:44.725016 | ADDED
(25 rows)


--! Test Run 2 — Rename & drop
-- 1. Rename an index
ALTER INDEX analytics_schema.idx_departments_main_location 
    RENAME TO idx_departments_main_loc;
-- 2. Drop a composite index
DROP INDEX analytics_schema.idx_employees_name;
-- 3. Drop unique index
DROP INDEX analytics_schema.idx_departments_budget_code;
-- 4. Rename another index
ALTER INDEX analytics_schema.idx_employees_lower_email 
    RENAME TO idx_employees_lower_mail;

-- Expected changes
-- 2 renames (RENAMED)
-- 2 deletions (DELETED)
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    processed_time,    change_type
from pdcd_schema.md5_metadata_tbl;
 snapshot_id |   schema_name    | object_type_name | object_subtype |      object_subtype_name      |            object_md5            |       processed_time       | change_type
-------------+------------------+------------------+----------------+-------------------------------+----------------------------------+----------------------------+-------------
           1 | analytics_schema | departments      | Column         | department_id                 | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-17 22:45:17.675502 | ADDED
           1 | analytics_schema | departments      | Column         | department_name               | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683473 | ADDED
           1 | analytics_schema | departments      | Column         | main_location                 | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683498 | ADDED
           1 | analytics_schema | departments      | Column         | ternary_location              | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-17 22:45:17.683502 | ADDED
           1 | analytics_schema | departments      | Column         | manager_id                    | 840f65e90f5ad043ed195535e39dc868 | 2025-11-17 22:45:17.683506 | ADDED
           1 | analytics_schema | departments      | Column         | budget_code                   | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-17 22:45:17.68351  | ADDED
           1 | analytics_schema | employees        | Column         | employee_id                   | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-17 22:45:17.683514 | ADDED
           1 | analytics_schema | employees        | Column         | first_name                    | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683518 | ADDED
           1 | analytics_schema | employees        | Column         | last_name                     | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683521 | ADDED
           1 | analytics_schema | employees        | Column         | email                         | dbd03119c5e663a65d2215ea7cce415c | 2025-11-17 22:45:17.683952 | ADDED
           1 | analytics_schema | employees        | Column         | phone_number                  | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-17 22:45:17.683959 | ADDED
           1 | analytics_schema | employees        | Column         | hire_date                     | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-17 22:45:17.683963 | ADDED
           1 | analytics_schema | employees        | Column         | salary                        | 400aef400f3cb973b96caa66c27d549d | 2025-11-17 22:45:17.683967 | ADDED
           1 | analytics_schema | employees        | Column         | department_id                 | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-17 22:45:17.683971 | ADDED
           1 | analytics_schema | departments      | Constraint     | departments_pkey              | 6f00aac61324af405b95307a19340808 | 2025-11-17 22:45:17.686418 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_department_id_fkey  | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-17 22:45:17.686442 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_email_key           | 7366006ed8864803a9082211343f7a9d | 2025-11-17 22:45:17.686446 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_pkey                | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-17 22:45:17.686449 | ADDED
           1 | analytics_schema | departments      | Index          | departments_pkey              | b42e577e3d023d664d051edb062f8b89 | 2025-11-17 22:45:17.690131 | ADDED
           1 | analytics_schema | employees        | Index          | employees_email_key           | 46afff75141550028b030f851dff4823 | 2025-11-17 22:45:17.690165 | ADDED
           1 | analytics_schema | employees        | Index          | employees_pkey                | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-17 22:45:17.69017  | ADDED
           2 | analytics_schema | departments      | Index          | idx_departments_budget_code   | 91036992bfa3cd533523940d8aa98116 | 2025-11-17 22:57:44.722996 | ADDED
           2 | analytics_schema | departments      | Index          | idx_departments_main_location | a00aa80d1a572c83f77edc6c8ea0bec4 | 2025-11-17 22:57:44.723047 | ADDED
           2 | analytics_schema | employees        | Index          | idx_employees_lower_email     | bfb32be21e32a409065bd56837323526 | 2025-11-17 22:57:44.723053 | ADDED
           2 | analytics_schema | employees        | Index          | idx_employees_name            | e2302f1fc86d6fbfd26e9bfbb5dd3847 | 2025-11-17 22:57:44.725016 | ADDED
         
           3 | analytics_schema | departments      | Index          | idx_departments_main_loc      | a00aa80d1a572c83f77edc6c8ea0bec4 | 2025-11-17 22:58:20.672987 | RENAMED
           3 | analytics_schema | employees        | Index          | idx_employees_lower_mail      | bfb32be21e32a409065bd56837323526 | 2025-11-17 22:58:20.673047 | RENAMED
           3 | analytics_schema | employees        | Index          | idx_employees_name            | e2302f1fc86d6fbfd26e9bfbb5dd3847 | 2025-11-17 22:58:20.673372 | DELETED
           3 | analytics_schema | departments      | Index          | idx_departments_budget_code   | 91036992bfa3cd533523940d8aa98116 | 2025-11-17 22:58:20.673395 | DELETED
(29 rows)

-- Test Run 3 — Add new advanced ones
    -- 1. Create partial index (filter condition)
CREATE INDEX idx_employees_high_salary 
    ON analytics_schema.employees(salary)
    WHERE salary > 100000;
-- 2. Create unique index with expression
CREATE UNIQUE INDEX idx_employees_unique_lower_email 
    ON analytics_schema.employees(LOWER(email));
-- 3. Create index on foreign key
CREATE INDEX idx_employees_department 
    ON analytics_schema.employees(department_id);
-- 4. Create index with DESC order
CREATE INDEX idx_departments_manager_desc 
    ON analytics_schema.departments(manager_id DESC);
-- Expected changes
-- 4 ADDED indexes with different characteristics (filtered, unique, expression, order).

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    processed_time,    change_type
from pdcd_schema.md5_metadata_tbl;
 snapshot_id |   schema_name    | object_type_name | object_subtype |       object_subtype_name        |            object_md5            |       processed_time       | change_type
-------------+------------------+------------------+----------------+----------------------------------+----------------------------------+----------------------------+-------------
           1 | analytics_schema | departments      | Column         | department_id                    | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-17 22:45:17.675502 | ADDED
           1 | analytics_schema | departments      | Column         | department_name                  | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683473 | ADDED
           1 | analytics_schema | departments      | Column         | main_location                    | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683498 | ADDED
           1 | analytics_schema | departments      | Column         | ternary_location                 | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-17 22:45:17.683502 | ADDED
           1 | analytics_schema | departments      | Column         | manager_id                       | 840f65e90f5ad043ed195535e39dc868 | 2025-11-17 22:45:17.683506 | ADDED
           1 | analytics_schema | departments      | Column         | budget_code                      | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-17 22:45:17.68351  | ADDED
           1 | analytics_schema | employees        | Column         | employee_id                      | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-17 22:45:17.683514 | ADDED
           1 | analytics_schema | employees        | Column         | first_name                       | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683518 | ADDED
           1 | analytics_schema | employees        | Column         | last_name                        | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683521 | ADDED
           1 | analytics_schema | employees        | Column         | email                            | dbd03119c5e663a65d2215ea7cce415c | 2025-11-17 22:45:17.683952 | ADDED
           1 | analytics_schema | employees        | Column         | phone_number                     | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-17 22:45:17.683959 | ADDED
           1 | analytics_schema | employees        | Column         | hire_date                        | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-17 22:45:17.683963 | ADDED
           1 | analytics_schema | employees        | Column         | salary                           | 400aef400f3cb973b96caa66c27d549d | 2025-11-17 22:45:17.683967 | ADDED
           1 | analytics_schema | employees        | Column         | department_id                    | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-17 22:45:17.683971 | ADDED
           1 | analytics_schema | departments      | Constraint     | departments_pkey                 | 6f00aac61324af405b95307a19340808 | 2025-11-17 22:45:17.686418 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_department_id_fkey     | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-17 22:45:17.686442 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_email_key              | 7366006ed8864803a9082211343f7a9d | 2025-11-17 22:45:17.686446 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_pkey                   | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-17 22:45:17.686449 | ADDED
           1 | analytics_schema | departments      | Index          | departments_pkey                 | b42e577e3d023d664d051edb062f8b89 | 2025-11-17 22:45:17.690131 | ADDED
           1 | analytics_schema | employees        | Index          | employees_email_key              | 46afff75141550028b030f851dff4823 | 2025-11-17 22:45:17.690165 | ADDED
           1 | analytics_schema | employees        | Index          | employees_pkey                   | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-17 22:45:17.69017  | ADDED
           2 | analytics_schema | departments      | Index          | idx_departments_budget_code      | 91036992bfa3cd533523940d8aa98116 | 2025-11-17 22:57:44.722996 | ADDED
           2 | analytics_schema | departments      | Index          | idx_departments_main_location    | a00aa80d1a572c83f77edc6c8ea0bec4 | 2025-11-17 22:57:44.723047 | ADDED
           2 | analytics_schema | employees        | Index          | idx_employees_lower_email        | bfb32be21e32a409065bd56837323526 | 2025-11-17 22:57:44.723053 | ADDED
           2 | analytics_schema | employees        | Index          | idx_employees_name               | e2302f1fc86d6fbfd26e9bfbb5dd3847 | 2025-11-17 22:57:44.725016 | ADDED
           3 | analytics_schema | departments      | Index          | idx_departments_main_loc         | a00aa80d1a572c83f77edc6c8ea0bec4 | 2025-11-17 22:58:20.672987 | RENAMED
           3 | analytics_schema | employees        | Index          | idx_employees_lower_mail         | bfb32be21e32a409065bd56837323526 | 2025-11-17 22:58:20.673047 | RENAMED
           3 | analytics_schema | employees        | Index          | idx_employees_name               | e2302f1fc86d6fbfd26e9bfbb5dd3847 | 2025-11-17 22:58:20.673372 | DELETED
           3 | analytics_schema | departments      | Index          | idx_departments_budget_code      | 91036992bfa3cd533523940d8aa98116 | 2025-11-17 22:58:20.673395 | DELETED
           4 | analytics_schema | departments      | Index          | idx_departments_manager_desc     | 144d6cbd70cc80783d9a8ac3f0a391e6 | 2025-11-17 22:59:41.204976 | ADDED
           4 | analytics_schema | employees        | Index          | idx_employees_department         | 45b82df16eeb6ee6a50d3c951000ea05 | 2025-11-17 22:59:41.205049 | ADDED
           4 | analytics_schema | employees        | Index          | idx_employees_high_salary        | 1d8039ae04bf65e367b917baf59b4e0a | 2025-11-17 22:59:41.205059 | ADDED
           4 | analytics_schema | employees        | Index          | idx_employees_unique_lower_email | d0ec151f8f25dc606c9737c7339ad039 | 2025-11-17 22:59:41.205067 | ADDED
(33 rows)

-- Test Run 4 — Modify and rename combo
-- 1. Drop partial index
DROP INDEX analytics_schema.idx_employees_high_salary;
-- 2. Rename foreign key index
ALTER INDEX analytics_schema.idx_employees_department 
    RENAME TO idx_emp_dept;
-- 3. Recreate same dropped index (modified form)
CREATE INDEX idx_employees_high_salary_new 
    ON analytics_schema.employees(salary)
    WHERE salary > 150000;
-- 4. Rename DESC index
ALTER INDEX analytics_schema.idx_departments_manager_desc 
    RENAME TO idx_departments_mgr_desc;
-- Expected changes
-- 2 RENAMED, 1 DELETED, 1 ADDED.
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    processed_time,    change_type
from pdcd_schema.md5_metadata_tbl;
 snapshot_id |   schema_name    | object_type_name | object_subtype |       object_subtype_name        |            object_md5            |       processed_time       | change_type
-------------+------------------+------------------+----------------+----------------------------------+----------------------------------+----------------------------+-------------
           1 | analytics_schema | departments      | Column         | department_id                    | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-17 22:45:17.675502 | ADDED
           1 | analytics_schema | departments      | Column         | department_name                  | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683473 | ADDED
           1 | analytics_schema | departments      | Column         | main_location                    | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683498 | ADDED
           1 | analytics_schema | departments      | Column         | ternary_location                 | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-17 22:45:17.683502 | ADDED
           1 | analytics_schema | departments      | Column         | manager_id                       | 840f65e90f5ad043ed195535e39dc868 | 2025-11-17 22:45:17.683506 | ADDED
           1 | analytics_schema | departments      | Column         | budget_code                      | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-17 22:45:17.68351  | ADDED
           1 | analytics_schema | employees        | Column         | employee_id                      | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-17 22:45:17.683514 | ADDED
           1 | analytics_schema | employees        | Column         | first_name                       | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683518 | ADDED
           1 | analytics_schema | employees        | Column         | last_name                        | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683521 | ADDED
           1 | analytics_schema | employees        | Column         | email                            | dbd03119c5e663a65d2215ea7cce415c | 2025-11-17 22:45:17.683952 | ADDED
           1 | analytics_schema | employees        | Column         | phone_number                     | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-17 22:45:17.683959 | ADDED
           1 | analytics_schema | employees        | Column         | hire_date                        | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-17 22:45:17.683963 | ADDED
           1 | analytics_schema | employees        | Column         | salary                           | 400aef400f3cb973b96caa66c27d549d | 2025-11-17 22:45:17.683967 | ADDED
           1 | analytics_schema | employees        | Column         | department_id                    | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-17 22:45:17.683971 | ADDED
           1 | analytics_schema | departments      | Constraint     | departments_pkey                 | 6f00aac61324af405b95307a19340808 | 2025-11-17 22:45:17.686418 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_department_id_fkey     | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-17 22:45:17.686442 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_email_key              | 7366006ed8864803a9082211343f7a9d | 2025-11-17 22:45:17.686446 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_pkey                   | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-17 22:45:17.686449 | ADDED
           1 | analytics_schema | departments      | Index          | departments_pkey                 | b42e577e3d023d664d051edb062f8b89 | 2025-11-17 22:45:17.690131 | ADDED
           1 | analytics_schema | employees        | Index          | employees_email_key              | 46afff75141550028b030f851dff4823 | 2025-11-17 22:45:17.690165 | ADDED
           1 | analytics_schema | employees        | Index          | employees_pkey                   | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-17 22:45:17.69017  | ADDED
           2 | analytics_schema | departments      | Index          | idx_departments_budget_code      | 91036992bfa3cd533523940d8aa98116 | 2025-11-17 22:57:44.722996 | ADDED
           2 | analytics_schema | departments      | Index          | idx_departments_main_location    | a00aa80d1a572c83f77edc6c8ea0bec4 | 2025-11-17 22:57:44.723047 | ADDED
           2 | analytics_schema | employees        | Index          | idx_employees_lower_email        | bfb32be21e32a409065bd56837323526 | 2025-11-17 22:57:44.723053 | ADDED
           2 | analytics_schema | employees        | Index          | idx_employees_name               | e2302f1fc86d6fbfd26e9bfbb5dd3847 | 2025-11-17 22:57:44.725016 | ADDED
           3 | analytics_schema | departments      | Index          | idx_departments_main_loc         | a00aa80d1a572c83f77edc6c8ea0bec4 | 2025-11-17 22:58:20.672987 | RENAMED
           3 | analytics_schema | employees        | Index          | idx_employees_lower_mail         | bfb32be21e32a409065bd56837323526 | 2025-11-17 22:58:20.673047 | RENAMED
           3 | analytics_schema | employees        | Index          | idx_employees_name               | e2302f1fc86d6fbfd26e9bfbb5dd3847 | 2025-11-17 22:58:20.673372 | DELETED
           3 | analytics_schema | departments      | Index          | idx_departments_budget_code      | 91036992bfa3cd533523940d8aa98116 | 2025-11-17 22:58:20.673395 | DELETED
           4 | analytics_schema | departments      | Index          | idx_departments_manager_desc     | 144d6cbd70cc80783d9a8ac3f0a391e6 | 2025-11-17 22:59:41.204976 | ADDED
           4 | analytics_schema | employees        | Index          | idx_employees_department         | 45b82df16eeb6ee6a50d3c951000ea05 | 2025-11-17 22:59:41.205049 | ADDED
           4 | analytics_schema | employees        | Index          | idx_employees_high_salary        | 1d8039ae04bf65e367b917baf59b4e0a | 2025-11-17 22:59:41.205059 | ADDED
           4 | analytics_schema | employees        | Index          | idx_employees_unique_lower_email | d0ec151f8f25dc606c9737c7339ad039 | 2025-11-17 22:59:41.205067 | ADDED
           5 | analytics_schema | departments      | Index          | idx_departments_mgr_desc         | 144d6cbd70cc80783d9a8ac3f0a391e6 | 2025-11-17 23:01:32.253662 | RENAMED
           5 | analytics_schema | employees        | Index          | idx_emp_dept                     | 45b82df16eeb6ee6a50d3c951000ea05 | 2025-11-17 23:01:32.25369  | RENAMED
           5 | analytics_schema | employees        | Index          | idx_employees_high_salary_new    | 1c278c7c2d9e04bb7665e964e9328e78 | 2025-11-17 23:01:32.253745 | ADDED
           5 | analytics_schema | employees        | Index          | idx_employees_high_salary        | 1d8039ae04bf65e367b917baf59b4e0a | 2025-11-17 23:01:32.253772 | DELETED
(37 rows)

-- Test Run 5 — Cleanup and recreate
-- 1. Drop all employee indexes
DROP INDEX IF EXISTS analytics_schema.idx_employees_high_salary_new;
DROP INDEX IF EXISTS analytics_schema.idx_employees_unique_lower_email;
DROP INDEX IF EXISTS analytics_schema.idx_emp_dept;
DROP INDEX IF EXISTS analytics_schema.idx_employees_lower_mail;
-- 2. Drop department index
DROP INDEX IF EXISTS analytics_schema.idx_departments_mgr_desc;
-- 3. Recreate a new one for each table
CREATE INDEX idx_dept_budget_mgr 
    ON analytics_schema.departments(budget_code, manager_id);
CREATE INDEX idx_emp_hire_salary 
    ON analytics_schema.employees(hire_date, salary);
-- 4. Create unique multi-column index
CREATE UNIQUE INDEX idx_emp_name_email 
    ON analytics_schema.employees(first_name, email);
-- Expected changes
-- Multiple DELETED followed by new ADDED indexes.
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    processed_time,    change_type
from pdcd_schema.md5_metadata_tbl;
 snapshot_id |   schema_name    | object_type_name | object_subtype |       object_subtype_name        |            object_md5            |       processed_time       | change_type
-------------+------------------+------------------+----------------+----------------------------------+----------------------------------+----------------------------+-------------
           1 | analytics_schema | departments      | Column         | department_id                    | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-17 22:45:17.675502 | ADDED
           1 | analytics_schema | departments      | Column         | department_name                  | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683473 | ADDED
           1 | analytics_schema | departments      | Column         | main_location                    | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683498 | ADDED
           1 | analytics_schema | departments      | Column         | ternary_location                 | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-17 22:45:17.683502 | ADDED
           1 | analytics_schema | departments      | Column         | manager_id                       | 840f65e90f5ad043ed195535e39dc868 | 2025-11-17 22:45:17.683506 | ADDED
           1 | analytics_schema | departments      | Column         | budget_code                      | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-17 22:45:17.68351  | ADDED
           1 | analytics_schema | employees        | Column         | employee_id                      | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-17 22:45:17.683514 | ADDED
           1 | analytics_schema | employees        | Column         | first_name                       | 5a841b9bbc928694255504765a33a956 | 2025-11-17 22:45:17.683518 | ADDED
           1 | analytics_schema | employees        | Column         | last_name                        | d6e08099dee6077445cfbdf123772ea9 | 2025-11-17 22:45:17.683521 | ADDED
           1 | analytics_schema | employees        | Column         | email                            | dbd03119c5e663a65d2215ea7cce415c | 2025-11-17 22:45:17.683952 | ADDED
           1 | analytics_schema | employees        | Column         | phone_number                     | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-17 22:45:17.683959 | ADDED
           1 | analytics_schema | employees        | Column         | hire_date                        | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-17 22:45:17.683963 | ADDED
           1 | analytics_schema | employees        | Column         | salary                           | 400aef400f3cb973b96caa66c27d549d | 2025-11-17 22:45:17.683967 | ADDED
           1 | analytics_schema | employees        | Column         | department_id                    | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-17 22:45:17.683971 | ADDED
           1 | analytics_schema | departments      | Constraint     | departments_pkey                 | 6f00aac61324af405b95307a19340808 | 2025-11-17 22:45:17.686418 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_department_id_fkey     | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-17 22:45:17.686442 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_email_key              | 7366006ed8864803a9082211343f7a9d | 2025-11-17 22:45:17.686446 | ADDED
           1 | analytics_schema | employees        | Constraint     | employees_pkey                   | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-17 22:45:17.686449 | ADDED
           1 | analytics_schema | departments      | Index          | departments_pkey                 | b42e577e3d023d664d051edb062f8b89 | 2025-11-17 22:45:17.690131 | ADDED
           1 | analytics_schema | employees        | Index          | employees_email_key              | 46afff75141550028b030f851dff4823 | 2025-11-17 22:45:17.690165 | ADDED
           1 | analytics_schema | employees        | Index          | employees_pkey                   | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-17 22:45:17.69017  | ADDED
           2 | analytics_schema | departments      | Index          | idx_departments_budget_code      | 91036992bfa3cd533523940d8aa98116 | 2025-11-17 22:57:44.722996 | ADDED
           2 | analytics_schema | departments      | Index          | idx_departments_main_location    | a00aa80d1a572c83f77edc6c8ea0bec4 | 2025-11-17 22:57:44.723047 | ADDED
           2 | analytics_schema | employees        | Index          | idx_employees_lower_email        | bfb32be21e32a409065bd56837323526 | 2025-11-17 22:57:44.723053 | ADDED
           2 | analytics_schema | employees        | Index          | idx_employees_name               | e2302f1fc86d6fbfd26e9bfbb5dd3847 | 2025-11-17 22:57:44.725016 | ADDED
           3 | analytics_schema | departments      | Index          | idx_departments_main_loc         | a00aa80d1a572c83f77edc6c8ea0bec4 | 2025-11-17 22:58:20.672987 | RENAMED
           3 | analytics_schema | employees        | Index          | idx_employees_lower_mail         | bfb32be21e32a409065bd56837323526 | 2025-11-17 22:58:20.673047 | RENAMED
           3 | analytics_schema | employees        | Index          | idx_employees_name               | e2302f1fc86d6fbfd26e9bfbb5dd3847 | 2025-11-17 22:58:20.673372 | DELETED
           3 | analytics_schema | departments      | Index          | idx_departments_budget_code      | 91036992bfa3cd533523940d8aa98116 | 2025-11-17 22:58:20.673395 | DELETED
           4 | analytics_schema | departments      | Index          | idx_departments_manager_desc     | 144d6cbd70cc80783d9a8ac3f0a391e6 | 2025-11-17 22:59:41.204976 | ADDED
           4 | analytics_schema | employees        | Index          | idx_employees_department         | 45b82df16eeb6ee6a50d3c951000ea05 | 2025-11-17 22:59:41.205049 | ADDED
           4 | analytics_schema | employees        | Index          | idx_employees_high_salary        | 1d8039ae04bf65e367b917baf59b4e0a | 2025-11-17 22:59:41.205059 | ADDED
           4 | analytics_schema | employees        | Index          | idx_employees_unique_lower_email | d0ec151f8f25dc606c9737c7339ad039 | 2025-11-17 22:59:41.205067 | ADDED
           5 | analytics_schema | departments      | Index          | idx_departments_mgr_desc         | 144d6cbd70cc80783d9a8ac3f0a391e6 | 2025-11-17 23:01:32.253662 | RENAMED
           5 | analytics_schema | employees        | Index          | idx_emp_dept                     | 45b82df16eeb6ee6a50d3c951000ea05 | 2025-11-17 23:01:32.25369  | RENAMED
           5 | analytics_schema | employees        | Index          | idx_employees_high_salary_new    | 1c278c7c2d9e04bb7665e964e9328e78 | 2025-11-17 23:01:32.253745 | ADDED
           5 | analytics_schema | employees        | Index          | idx_employees_high_salary        | 1d8039ae04bf65e367b917baf59b4e0a | 2025-11-17 23:01:32.253772 | DELETED
         
           6 | analytics_schema | departments      | Index          | idx_dept_budget_mgr              | aae0097057690a395653d5d71e7a6aaf | 2025-11-17 23:03:09.273987 | ADDED
           6 | analytics_schema | employees        | Index          | idx_emp_hire_salary              | 68d7df4a897bb6d46efb658fe5f7a1eb | 2025-11-17 23:03:09.274066 | ADDED
           6 | analytics_schema | employees        | Index          | idx_emp_name_email               | 17cc28a30d1f065a4d87eca132b98874 | 2025-11-17 23:03:09.274071 | ADDED
           6 | analytics_schema | employees        | Index          | idx_employees_unique_lower_email | d0ec151f8f25dc606c9737c7339ad039 | 2025-11-17 23:03:09.274122 | DELETED
           6 | analytics_schema | employees        | Index          | idx_employees_high_salary_new    | 1c278c7c2d9e04bb7665e964e9328e78 | 2025-11-17 23:03:09.274137 | DELETED
           6 | analytics_schema | departments      | Index          | idx_departments_mgr_desc         | 144d6cbd70cc80783d9a8ac3f0a391e6 | 2025-11-17 23:03:09.274141 | DELETED
           6 | analytics_schema | employees        | Index          | idx_employees_lower_mail         | bfb32be21e32a409065bd56837323526 | 2025-11-17 23:03:09.274145 | DELETED
           6 | analytics_schema | employees        | Index          | idx_emp_dept                     | 45b82df16eeb6ee6a50d3c951000ea05 | 2025-11-17 23:03:09.274148 | DELETED