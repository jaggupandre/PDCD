
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;

DROP SCHEMA IF EXISTS analytics_schema CASCADE;
CREATE SCHEMA IF NOT EXISTS analytics_schema;

--==============================
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    main_location VARCHAR(100),
    ternary_location VARCHAR(100),
    manager_id INT,
    budget_code VARCHAR(50)
);
--==============================
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

-- =================== =================== ===================
-- *================== Run 1
-- =================== =================== ===================
-- Test Run 1 — Create Initial Functions
-- Purpose: Detect newly added functions.
-- TEST RUN 1
-- Function 001
CREATE OR REPLACE FUNCTION analytics_schema.add_values(a INT, b INT)
RETURNS INT
LANGUAGE SQL
AS $$
    SELECT a + b;
$$;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);


-- =================== =================== ===================
-- *================== Run 2
-- =================== =================== ===================

-- Function 002
CREATE OR REPLACE FUNCTION analytics_schema.get_department_count()
RETURNS BIGINT
LANGUAGE SQL
AS $$
    SELECT COUNT(*) FROM analytics_schema.departments;
$$;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
        SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
        SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);


-- =================== =================== ===================
-- *================== Run 3
-- =================== =================== ===================

-- ✅ Test Function #1 — Pure SQL Function
CREATE OR REPLACE FUNCTION analytics_schema.test_sql_fn()
RETURNS INTEGER
LANGUAGE SQL
AS $$
    SELECT COUNT(*) FROM analytics_schema.employees;
$$;

-- ✅ Test Function #2 — PL/pgSQL Function (with DECLARE, RETURN, etc.)
CREATE OR REPLACE FUNCTION analytics_schema.test_plpgsql_fn()
RETURNS TEXT
LANGUAGE plpgsql
AS $function$
DECLARE
    msg TEXT;
BEGIN
    msg := 'Hello from test_plpgsql_fn';
    RETURN msg;
END;
$function$;

-- ✅ Test Function #3 — PL/pgSQL With Multiple Statements
CREATE OR REPLACE FUNCTION analytics_schema.test_plpgsql_multi()
RETURNS INTEGER
LANGUAGE plpgsql
AS $function$
DECLARE
    a INT := 10;
    b INT := 20;
BEGIN
    a := a + b;
    a := a * 2;
    RETURN a;
END;
$function$;
-- ✅ Test Function #4 — Function With Arguments
CREATE OR REPLACE FUNCTION analytics_schema.test_fn_args(x INT, y INT)
RETURNS INT
LANGUAGE SQL
AS $$
    SELECT x + y;
$$;


    SELECT * FROM pdcd_schema.load_snapshot_tbl();
        SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
        SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);


-- =================== =================== ===================
-- *================== Run 4
-- =================== =================== ===================

-- Test #1 — Altering the logic and added function variable
CREATE OR REPLACE FUNCTION analytics_schema.test_sql_fn(x INT)
RETURNS INTEGER
LANGUAGE SQL
AS $$
    SELECT (COUNT(*) + x) AS employee_count FROM analytics_schema.employees;
$$;

-- Test #2 - RENAME
ALTER FUNCTION analytics_schema.test_fn_args
RENAME TO test_fn_arguments;


    SELECT * FROM pdcd_schema.load_snapshot_tbl();
        SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
        SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);

 snapshot_id |   schema_name    | object_type_name  | object_subtype | object_subtype_name |            object_md5            | change_type |                                                                                                                             object_subtype_details
-------------+------------------+-------------------+----------------+---------------------+----------------------------------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           4 | analytics_schema | test_fn_arguments |                |                     | 3e9c1477d76c622fe29b6f5fcd469a74 | RENAMED     | argument_types:x integer, y integer,argument_modes:x integer, y integer,return_type:integer,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:169497,function_body: SELECT x + y;
           4 | analytics_schema | test_sql_fn       |                |                     | d205aba30b2c2526bf2fa2715ab12e17 | MODIFIED    | argument_types:x integer,argument_modes:x integer,return_type:integer,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:169497,function_body: SELECT (COUNT(*) + x) AS employee_count FROM analytics_schema.employees;
(2 rows)

-- =================== =================== ===================
-- *================== Run 5
-- =================== =================== ===================


------------------------------------------------------------
--todo RESET METADATA TABLES
------------------------------------------------------------
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;

DROP SCHEMA IF EXISTS analytics_schema CASCADE;
CREATE SCHEMA analytics_schema;

------------------------------------------------------------
-- CREATE BASE TABLES
------------------------------------------------------------
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

-- =================================================================
-- RUN 1 — ADD FUNCTIONS
-- =================================================================
-- Function 1
CREATE OR REPLACE FUNCTION analytics_schema.fn_add(a INT, b INT)
RETURNS INT
LANGUAGE SQL AS $$ SELECT a + b; $$;

-- Function 2
CREATE OR REPLACE FUNCTION analytics_schema.fn_count_emp()
RETURNS BIGINT
LANGUAGE SQL AS $$ SELECT COUNT(*) FROM analytics_schema.employees; $$;

-- Snapshot + MD5 Load
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type, object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
 snapshot_id |   schema_name    | object_type |       object_type_name       | object_subtype |      object_subtype_name      |            object_md5            | change_type |                                                                                                                            object_subtype_details
-------------+------------------+-------------+------------------------------+----------------+-------------------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           1 | analytics_schema | Function    | fn_add(a integer, b integer) |                |                               | 6871ce764ecfdda7ad4a9efbee1254eb | ADDED       | argument_types:a integer, b integer,argument_modes:a integer, b integer,return_type:integer,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170547,function_body:SELECT a + b;
           1 | analytics_schema | Function    | fn_count_emp()               |                |                               | b3879dbbf997a569c956d933ae59f6a7 | ADDED       | argument_types:,argument_modes:,return_type:bigint,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170547,function_body:SELECT COUNT(*) FROM analytics_schema.employees;
(26 rows)

-- =================================================================
-- RUN 2 — MODIFY RETURN TYPE + BODY
-- =================================================================
DROP FUNCTION analytics_schema.fn_add(integer,integer);

CREATE OR REPLACE FUNCTION analytics_schema.fn_add(a INT, b INT)
RETURNS NUMERIC
LANGUAGE SQL AS $$ SELECT (a + b)::NUMERIC + 1; $$;


-- Snapshot compare
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type, object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
 snapshot_id |   schema_name    | object_type |       object_type_name       | object_subtype | object_subtype_name |            object_md5            | change_type |                                                                                                                 object_subtype_details
-------------+------------------+-------------+------------------------------+----------------+---------------------+----------------------------------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           2 | analytics_schema | Function    | fn_add(a integer, b integer) |                |                     | 0b615ffc85cf7455307cfaf66896935a | MODIFIED    | argument_types:a integer, b integer,argument_modes:a integer, b integer,return_type:numeric,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170547,function_body:SELECT (a + b)::NUMERIC + 1;
(1 row)

-- =================================================================
-- RUN 3 — ADD PLPGSQL, DEFAULT ARGUMENTS, OVERLOADED VERSION
-- =================================================================
CREATE OR REPLACE FUNCTION analytics_schema.fn_greet(name TEXT DEFAULT 'User')
RETURNS TEXT
LANGUAGE plpgsql
AS $fn$
DECLARE
    msg TEXT := 'Hello ' || name;
BEGIN
    RETURN msg;
END;
$fn$;

--todo Overloaded fn_add
CREATE OR REPLACE FUNCTION analytics_schema.fn_add(a NUMERIC, b NUMERIC)
RETURNS NUMERIC
LANGUAGE SQL AS $$ SELECT a + b + 100; $$;

-- Snapshot compare
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type, object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
 snapshot_id |   schema_name    | object_type |       object_type_name       | object_subtype | object_subtype_name |            object_md5            | change_type |                                                                                                                           object_subtype_details
-------------+------------------+-------------+------------------------------+----------------+---------------------+----------------------------------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           3 | analytics_schema | Function    | fn_add(a numeric, b numeric) |                |                     | 5c8df2fd0742ecb8ac61f79578178b93 | ADDED       | argument_types:a numeric, b numeric,argument_modes:a numeric, b numeric,return_type:numeric,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170547,function_body:SELECT a + b + 100;
           3 | analytics_schema | Function    | fn_greet(name text)          |                |                     | 4c056c7af7307730f685af80b83d8a41 | ADDED       | argument_types:name text DEFAULT 'User'::text,argument_modes:name text,return_type:text,language:plpgsql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170547,13906,function_body:DECLARE msg TEXT := 'Hello ' || name; BEGIN
(2 rows)

-- =================================================================
-- RUN 4 — RENAME + MODIFICATION
-- =================================================================
ALTER FUNCTION analytics_schema.fn_count_emp()
RENAME TO fn_count_employees;

CREATE OR REPLACE FUNCTION analytics_schema.fn_greet(name TEXT DEFAULT 'User')
RETURNS TEXT
LANGUAGE plpgsql
AS $fn$
DECLARE
    msg TEXT := 'Welcome ' || name;
BEGIN
    RETURN msg;
END;
$fn$;

-- Snapshot compare
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type, object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);

 snapshot_id |   schema_name    | object_type |   object_type_name   | object_subtype | object_subtype_name |            object_md5            | change_type |                                                                                                                            object_subtype_details
-------------+------------------+-------------+----------------------+----------------+---------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           4 | analytics_schema | Function    | fn_count_employees() |                |                     | b3879dbbf997a569c956d933ae59f6a7 | RENAMED     | argument_types:,argument_modes:,return_type:bigint,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170547,function_body:SELECT COUNT(*) FROM analytics_schema.employees;
           4 | analytics_schema | Function    | fn_greet(name text)  |                |                     | f1eb0061bc156bdcde3a9370c6276d81 | MODIFIED    | argument_types:name text DEFAULT 'User'::text,argument_modes:name text,return_type:text,language:plpgsql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170547,13906,function_body:DECLARE msg TEXT := 'Welcome ' || name; BEGIN
(2 rows)
-- =================================================================
-- RUN 5 — DELETE + SECURITY DEFINER + COST
-- =================================================================
DROP FUNCTION analytics_schema.fn_add(NUMERIC, NUMERIC);

CREATE OR REPLACE FUNCTION analytics_schema.fn_add(a INT, b INT)
RETURNS NUMERIC
LANGUAGE SQL
SECURITY DEFINER
COST 50
AS $$ SELECT (a + b)::NUMERIC + 1; $$;

-- Snapshot compare
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type, object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);


 snapshot_id |   schema_name    | object_type |       object_type_name       | object_subtype | object_subtype_name |            object_md5            | change_type |                                                                                                            object_subtype_details
-------------+------------------+-------------+------------------------------+----------------+---------------------+----------------------------------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           5 | analytics_schema | Function    | fn_add(a numeric, b numeric) |                |                     | 5c8df2fd0742ecb8ac61f79578178b93 | DELETED     | argument_types:a numeric, b numeric,argument_modes:a numeric, b numeric,return_type:numeric,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170547,function_body:SELECT a + b + 100;
(1 row)

-- =================================================================
-- RUN 6 — PRIVILEGE CHANGE
-- =================================================================
REVOKE ALL ON FUNCTION analytics_schema.fn_greet(TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION analytics_schema.fn_greet(TEXT) TO read_only_role;

-- Snapshot compare
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);


-- =================================================================
-- RUN 7 — VOLATILITY + PARALLEL SAFE MODIFICATION
-- =================================================================
CREATE OR REPLACE FUNCTION analytics_schema.fn_count_employees()
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$ SELECT COUNT(*) FROM analytics_schema.employees; $$;

-- Snapshot compare
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);


-- =================================================================
-- RUN 8 — COMMENT CHANGE (Optional Drift Case)
-- =================================================================
COMMENT ON FUNCTION analytics_schema.fn_add(INT, INT)
IS 'sample comment for testing drift';

-- Snapshot compare
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

------------------------------------------------------------
-- FINAL RESULT
------------------------------------------------------------
SELECT snapshot_id,
       schema_name,
       object_type_name,
       object_subtype,
       object_subtype_name,
       object_md5,
       change_type,
       object_subtype_details
FROM pdcd_schema.md5_metadata_tbl
WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl)
ORDER BY object_type_name;
