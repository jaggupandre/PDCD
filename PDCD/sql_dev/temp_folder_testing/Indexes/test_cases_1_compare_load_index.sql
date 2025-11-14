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

select metadata_id, snapshot_id, object_type_name, object_subtype_name, object_subtype_details, object_md5, processed_time, change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name |     object_subtype_name      |                                                               object_subtype_details                                                                |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | departments_pkey             | constraint_type:PRIMARY KEY,column_name:department_id,definition:PRIMARY KEY (department_id)                                                        | 6f00aac61324af405b95307a19340808 | 2025-11-13 21:29:15.268683 | ADDED
           2 |           1 | employees        | employees_department_id_fkey | constraint_type:FOREIGN KEY,column_name:department_id,definition:FOREIGN KEY (department_id) REFERENCES analytics_schema.departments(department_id) | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-13 21:29:15.270923 | ADDED
           3 |           1 | employees        | employees_email_key          | constraint_type:UNIQUE,column_name:email,definition:UNIQUE (email)                                                                                  | 7366006ed8864803a9082211343f7a9d | 2025-11-13 21:29:15.270936 | ADDED
           4 |           1 | employees        | employees_pkey               | constraint_type:PRIMARY KEY,column_name:employee_id,definition:PRIMARY KEY (employee_id)                                                            | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-13 21:29:15.270939 | ADDED

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
-- 4. Create index with expression
CREATE INDEX idx_employees_lower_email 
    ON analytics_schema.employees(LOWER(email));
-- Expected changes
-- 4 new entries with object_type = 'INDEX' and change_type = 'ADDED'.
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    
            SELECT 
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        -- FROM pdcd_schema.get_table_columns_md5(p_table_list)
        FROM pdcd_schema.get_table_constraints_md5(ARRAY['analytics_schema']);

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_constraint_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

   schema_name    | object_type | object_type_name | object_subtype |     object_subtype_name      |                                                               object_subtype_details                                                                |            object_md5
------------------+-------------+------------------+----------------+------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------
 analytics_schema | Table       | departments      | Constraint     | departments_pkey             | constraint_type:PRIMARY KEY,column_name:department_id,definition:PRIMARY KEY (department_id)                                                        | 6f00aac61324af405b95307a19340808
 analytics_schema | Table       | employees        | Constraint     | employees_department_id_fkey | constraint_type:FOREIGN KEY,column_name:department_id,definition:FOREIGN KEY (department_id) REFERENCES analytics_schema.departments(department_id) | d15c5860c9bccaa7101d3622732d6aa7
 analytics_schema | Table       | employees        | Constraint     | employees_email_key          | constraint_type:UNIQUE,column_name:email,definition:UNIQUE (email)                                                                                  | 7366006ed8864803a9082211343f7a9d
 analytics_schema | Table       | employees        | Constraint     | employees_pkey               | constraint_type:PRIMARY KEY,column_name:employee_id,definition:PRIMARY KEY (employee_id)                                                            | 46e4d7c138cb7c7937bfd192b807b708

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