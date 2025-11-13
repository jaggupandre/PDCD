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
DROP table IF EXISTS analytics_schema.departments;
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    main_location VARCHAR(100),
    ternary_location VARCHAR(100),
    manager_id INT,
    budget_code VARCHAR(50)
);
DROP table IF EXISTS analytics_schema.employees;
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

select metadata_id, snapshot_id, object_type_name, object_subtype_name, object_md5, processed_time, change_type FROM pdcd_schema.md5_metadata_tbl;

-- Test Run 1 — Initial Additions & Modifications
-- 1. Create index on single column
CREATE INDEX idx_departments_main_location 
    ON analytics_schema.departments(main_location);
-- 2. Create unique index on budget_code
CREATE UNIQUE INDEX idx_departments_budget_code 
    ON analytics_schema.departments(budget_code);
-- 3. Create composite index
CREATE INDEX idx_employees_name 
    ON analytics_schema.employees(first_name, last_name);
-- 4. Create index with expression
CREATE INDEX idx_employees_lower_email 
    ON analytics_schema.employees(LOWER(email));

-- Expected changes
-- 4 new entries with object_type = 'INDEX' and change_type = 'ADDED'.
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

-- 🧪 Test Run 2 — Rename & drop
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
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

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
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

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
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

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
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
test_db=# select * from pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id |   schema_name    | object_type | object_type_name | object_subtype | object_subtype_name |                                                                                                                            object_subtype_details                                                                                                                             |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+-------------+------------------+----------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | analytics_schema | Table       | departments      | Column         | department_id       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1 | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-12 18:12:50.064149 | ADDED
           2 |           1 | analytics_schema | Table       | departments      | Column         | department_name     | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2                                                                          | 5a841b9bbc928694255504765a33a956 | 2025-11-12 18:12:50.070223 | ADDED
           3 |           1 | analytics_schema | Table       | departments      | Column         | main_location       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3                                                                         | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 18:12:50.070249 | ADDED
           4 |           1 | analytics_schema | Table       | departments      | Column         | ternary_location    | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4                                                                         | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-12 18:12:50.070253 | ADDED
           5 |           1 | analytics_schema | Table       | departments      | Column         | manager_id          | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5                                                                                   | 840f65e90f5ad043ed195535e39dc868 | 2025-11-12 18:12:50.070295 | ADDED
           6 |           1 | analytics_schema | Table       | departments      | Column         | budget_code         | data_type:character varying,max_length:50,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6                                                                          | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-12 18:12:50.070322 | ADDED
           7 |           2 | analytics_schema | Table       | departments      | Column         | department_name     | data_type:character varying,max_length:150,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2                                                                          | 2b9f21bc6704c84937a2dd0ca518c14c | 2025-11-12 18:13:39.279529 | MODIFIED
           8 |           2 | analytics_schema | Table       | departments      | Column         | main_location       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:'Head Office'::character varying,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3                                         | 3af8bad4fa96a3a485e23550581e6d8b | 2025-11-12 18:13:39.279585 | MODIFIED
           9 |           2 | analytics_schema | Table       | departments      | Column         | region              | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7                                                                                         | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 18:13:39.279653 | ADDED
          10 |           2 | analytics_schema | Table       | departments      | Column         | established_year    | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:8                                                                                   | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-12 18:13:39.279662 | ADDED
          11 |           3 | analytics_schema | Table       | departments      | Column         | department_region   | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7                                                                                         | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 18:16:52.682361 | RENAMED
          12 |           3 | analytics_schema | Table       | departments      | Column         | founded_year        | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:8                                                                                   | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-12 18:16:52.682897 | RENAMED
          13 |           3 | analytics_schema | Table       | departments      | Column         | last_updated_by     | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9                                                                                         | a110ecc03220cb8113204e96e1f9cfba | 2025-11-12 18:16:52.683044 | ADDED
          14 |           3 | analytics_schema | Table       | departments      | Column         | budget_allocated    | data_type:numeric,max_length:,numeric_precision:12,numeric_scale:2,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:10                                                                                  | 9fcafc65da07baa20e36cbd0d8d92c72 | 2025-11-12 18:16:52.683055 | ADDED
          15 |           4 | analytics_schema | Table       | departments      | Column         | budget_allocated    | data_type:double precision,max_length:,numeric_precision:53,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:10                                                                          | 4303381b7b875f4b5f782798e2564a36 | 2025-11-12 18:17:55.671346 | MODIFIED
          16 |           4 | analytics_schema | Table       | departments      | Column         | founded_year        | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:EXTRACT(year FROM now()),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:8                                                           | fe2a8229c2f4a36a65c346c2a8ec4e92 | 2025-11-12 18:17:55.671405 | MODIFIED
          17 |           4 | analytics_schema | Table       | departments      | Column         | updated_by          | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:CURRENT_USER,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9                                                                             | 88965135e4291d50cd045e493557bb97 | 2025-11-12 18:17:55.671466 | ADDED
          18 |           4 | analytics_schema | Table       | departments      | Column         | last_updated_by     | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9                                                                                         | a110ecc03220cb8113204e96e1f9cfba | 2025-11-12 18:17:55.671513 | DELETED
          19 |           5 | analytics_schema | Table       | departments      | Column         | total_employees     | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:11                                                                                  | b07395d20d52dbcb38f516077a00500d | 2025-11-12 18:19:30.783148 | ADDED
          20 |           5 | analytics_schema | Table       | departments      | Column         | active_status       | data_type:boolean,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:true,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:12                                                                                 | 32725f541bfbd0de9ea4ac4817444b96 | 2025-11-12 18:19:30.783193 | ADDED
          21 |           5 | analytics_schema | Table       | departments      | Column         | budget_allocated    | data_type:double precision,max_length:,numeric_precision:53,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:10                                                                          | 4303381b7b875f4b5f782798e2564a36 | 2025-11-12 18:19:30.78324  | DELETED
          22 |           5 | analytics_schema | Table       | departments      | Column         | updated_by          | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:CURRENT_USER,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9

-- Test Run 5 — Final Mixed Changes
ALTER TABLE analytics_schema.departments RENAME COLUMN total_employees TO headcount;
ALTER TABLE analytics_schema.departments ALTER COLUMN headcount TYPE BIGINT;
ALTER TABLE analytics_schema.departments DROP COLUMN active_status;
ALTER TABLE analytics_schema.departments ADD COLUMN remarks TEXT;
--  Purpose: simulate rename + modify + drop + add together — final full-cycle test.
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);


test_db=# select * from pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id |   schema_name    | object_type | object_type_name | object_subtype | object_subtype_name |                                                                                                                            object_subtype_details                                                                                                                             |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+-------------+------------------+----------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | analytics_schema | Table       | departments      | Column         | department_id       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1 | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-12 18:12:50.064149 | ADDED
           2 |           1 | analytics_schema | Table       | departments      | Column         | department_name     | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2                                                                          | 5a841b9bbc928694255504765a33a956 | 2025-11-12 18:12:50.070223 | ADDED
           3 |           1 | analytics_schema | Table       | departments      | Column         | main_location       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3                                                                         | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 18:12:50.070249 | ADDED
           4 |           1 | analytics_schema | Table       | departments      | Column         | ternary_location    | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4                                                                         | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-12 18:12:50.070253 | ADDED
           5 |           1 | analytics_schema | Table       | departments      | Column         | manager_id          | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5                                                                                   | 840f65e90f5ad043ed195535e39dc868 | 2025-11-12 18:12:50.070295 | ADDED
           6 |           1 | analytics_schema | Table       | departments      | Column         | budget_code         | data_type:character varying,max_length:50,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6                                                                          | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-12 18:12:50.070322 | ADDED
        --    
           7 |           2 | analytics_schema | Table       | departments      | Column         | department_name     | data_type:character varying,max_length:150,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2                                                                          | 2b9f21bc6704c84937a2dd0ca518c14c | 2025-11-12 18:13:39.279529 | MODIFIED
           8 |           2 | analytics_schema | Table       | departments      | Column         | main_location       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:'Head Office'::character varying,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3                                         | 3af8bad4fa96a3a485e23550581e6d8b | 2025-11-12 18:13:39.279585 | MODIFIED
           9 |           2 | analytics_schema | Table       | departments      | Column         | region              | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7                                                                                         | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 18:13:39.279653 | ADDED
          10 |           2 | analytics_schema | Table       | departments      | Column         | established_year    | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:8                                                                                   | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-12 18:13:39.279662 | ADDED
-- 
          11 |           3 | analytics_schema | Table       | departments      | Column         | department_region   | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7                                                                                         | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 18:16:52.682361 | RENAMED
          12 |           3 | analytics_schema | Table       | departments      | Column         | founded_year        | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:8                                                                                   | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-12 18:16:52.682897 | RENAMED
          13 |           3 | analytics_schema | Table       | departments      | Column         | last_updated_by     | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9                                                                                         | a110ecc03220cb8113204e96e1f9cfba | 2025-11-12 18:16:52.683044 | ADDED
          14 |           3 | analytics_schema | Table       | departments      | Column         | budget_allocated    | data_type:numeric,max_length:,numeric_precision:12,numeric_scale:2,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:10                                                                                  | 9fcafc65da07baa20e36cbd0d8d92c72 | 2025-11-12 18:16:52.683055 | ADDED
-- 
          15 |           4 | analytics_schema | Table       | departments      | Column         | budget_allocated    | data_type:double precision,max_length:,numeric_precision:53,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:10                                                                          | 4303381b7b875f4b5f782798e2564a36 | 2025-11-12 18:17:55.671346 | MODIFIED
          16 |           4 | analytics_schema | Table       | departments      | Column         | founded_year        | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:EXTRACT(year FROM now()),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:8                                                           | fe2a8229c2f4a36a65c346c2a8ec4e92 | 2025-11-12 18:17:55.671405 | MODIFIED
          17 |           4 | analytics_schema | Table       | departments      | Column         | updated_by          | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:CURRENT_USER,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9                                                                             | 88965135e4291d50cd045e493557bb97 | 2025-11-12 18:17:55.671466 | ADDED
          18 |           4 | analytics_schema | Table       | departments      | Column         | last_updated_by     | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9                                                                                         | a110ecc03220cb8113204e96e1f9cfba | 2025-11-12 18:17:55.671513 | DELETED
-- 
          19 |           5 | analytics_schema | Table       | departments      | Column         | total_employees     | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:11                                                                                  | b07395d20d52dbcb38f516077a00500d | 2025-11-12 18:19:30.783148 | ADDED
          20 |           5 | analytics_schema | Table       | departments      | Column         | active_status       | data_type:boolean,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:true,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:12                                                                                 | 32725f541bfbd0de9ea4ac4817444b96 | 2025-11-12 18:19:30.783193 | ADDED
          21 |           5 | analytics_schema | Table       | departments      | Column         | budget_allocated    | data_type:double precision,max_length:,numeric_precision:53,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:10                                                                          | 4303381b7b875f4b5f782798e2564a36 | 2025-11-12 18:19:30.78324  | DELETED
          22 |           5 | analytics_schema | Table       | departments      | Column         | updated_by          | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:CURRENT_USER,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9                                                                             | 88965135e4291d50cd045e493557bb97 | 2025-11-12 18:19:30.78325  | DELETED
-- 
          23 |           6 | analytics_schema | Table       | departments      | Column         | remarks             | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:13                                                                                        | 3b04b5ccde27a819b92b5f1db6e5a3ee | 2025-11-12 18:22:12.361383 | ADDED
          24 |           6 | analytics_schema | Table       | departments      | Column         | active_status       | data_type:boolean,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:true,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:12                                                                                 | 32725f541bfbd0de9ea4ac4817444b96 | 2025-11-12 18:22:12.361448 | DELETED
-- 
          25 |           7 | analytics_schema | Table       | departments      | Column         | headcount           | data_type:bigint,max_length:,numeric_precision:64,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:11                                                                                   | ae84db7cd478eaeb1426f3f97e751cba | 2025-11-12 18:24:02.075893 | ADDED
          26 |           7 | analytics_schema | Table       | departments      | Column         | remarks             | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:13                                                                                        | 3b04b5ccde27a819b92b5f1db6e5a3ee | 2025-11-12 18:24:02.083129 | ADDED
          27 |           7 | analytics_schema | Table       | departments      | Column         | total_employees     | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:11                                                                                  | b07395d20d52dbcb38f516077a00500d | 2025-11-12 18:24:02.083222 | DELETED
          28 |           7 | analytics_schema | Table       | departments      | Column         | active_status       | data_type:boolean,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:true,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:12                                                                                 | 32725f541bfbd0de9ea4ac4817444b96 | 2025-11-12 18:24:02.083235 | DELETED


test_db=# 
select
    metadata_id,
    snapshot_id,
    object_type_name,
    object_subtype_name,
    object_md5,
    processed_time,
    change_type
FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-12 18:12:50.064149 | ADDED
           2 |           1 | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-12 18:12:50.070223 | ADDED
           3 |           1 | main_location       | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 18:12:50.070249 | ADDED
           4 |           1 | ternary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-12 18:12:50.070253 | ADDED
           5 |           1 | manager_id          | 840f65e90f5ad043ed195535e39dc868 | 2025-11-12 18:12:50.070295 | ADDED
           6 |           1 | budget_code         | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-12 18:12:50.070322 | ADDED

           7 |           2 | department_name     | 2b9f21bc6704c84937a2dd0ca518c14c | 2025-11-12 18:13:39.279529 | MODIFIED
           8 |           2 | main_location       | 3af8bad4fa96a3a485e23550581e6d8b | 2025-11-12 18:13:39.279585 | MODIFIED
           9 |           2 | region              | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 18:13:39.279653 | ADDED
          10 |           2 | established_year    | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-12 18:13:39.279662 | ADDED

          11 |           3 | department_region   | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 18:16:52.682361 | RENAMED
          12 |           3 | founded_year        | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-12 18:16:52.682897 | RENAMED
          13 |           3 | last_updated_by     | a110ecc03220cb8113204e96e1f9cfba | 2025-11-12 18:16:52.683044 | ADDED
          14 |           3 | budget_allocated    | 9fcafc65da07baa20e36cbd0d8d92c72 | 2025-11-12 18:16:52.683055 | ADDED

          15 |           4 | budget_allocated    | 4303381b7b875f4b5f782798e2564a36 | 2025-11-12 18:17:55.671346 | MODIFIED
          16 |           4 | founded_year        | fe2a8229c2f4a36a65c346c2a8ec4e92 | 2025-11-12 18:17:55.671405 | MODIFIED
          17 |           4 | updated_by          | 88965135e4291d50cd045e493557bb97 | 2025-11-12 18:17:55.671466 | ADDED
          18 |           4 | last_updated_by     | a110ecc03220cb8113204e96e1f9cfba | 2025-11-12 18:17:55.671513 | DELETED

          19 |           5 | total_employees     | b07395d20d52dbcb38f516077a00500d | 2025-11-12 18:19:30.783148 | ADDED
          20 |           5 | active_status       | 32725f541bfbd0de9ea4ac4817444b96 | 2025-11-12 18:19:30.783193 | ADDED
          21 |           5 | budget_allocated    | 4303381b7b875f4b5f782798e2564a36 | 2025-11-12 18:19:30.78324  | DELETED
          22 |           5 | updated_by          | 88965135e4291d50cd045e493557bb97 | 2025-11-12 18:19:30.78325  | DELETED

          23 |           6 | headcount           | ae84db7cd478eaeb1426f3f97e751cba | 2025-11-12 18:24:02.075893 | ADDED
          24 |           6 | remarks             | 3b04b5ccde27a819b92b5f1db6e5a3ee | 2025-11-12 18:24:02.083129 | ADDED
          25 |           6 | total_employees     | b07395d20d52dbcb38f516077a00500d | 2025-11-12 18:24:02.083222 | DELETED
          26 |           6 | active_status       | 32725f541bfbd0de9ea4ac4817444b96 | 2025-11-12 18:24:02.083235 | DELETED

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