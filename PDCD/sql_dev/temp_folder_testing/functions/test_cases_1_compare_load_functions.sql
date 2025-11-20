
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;

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
--==============================

CREATE SCHEMA IF NOT EXISTS analytics_schema;
--==============================
-- Test Run 1 — Create Initial Functions
-- Purpose: Detect newly added functions.
-- TEST RUN 1
CREATE OR REPLACE FUNCTION analytics_schema.add_values(a INT, b INT)
RETURNS INT
LANGUAGE SQL
AS $$
    SELECT a + b;
$$;

CREATE OR REPLACE FUNCTION analytics_schema.get_department_count()
RETURNS BIGINT
LANGUAGE SQL
AS $$
    SELECT COUNT(*) FROM analytics_schema.departments;
$$;

--============================== ============================== ============================== 
--* UNDERSTANDING
--============================== ============================== ============================== 

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


--*----SOMETHING ELSE
-- CREATE
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



-- DROP
DROP FUNCTION analytics_schema.get_employee_count();

-- MODIFY (Alter its logic)
    -- DROP AND RECREATE with changed logic

-- RENAME
ALTER FUNCTION analytics_schema.get_employee_count()
RENAME TO get_total_employee_COUNT;


-- First Run
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

-- Second Run and subsequent runs to compare 
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);