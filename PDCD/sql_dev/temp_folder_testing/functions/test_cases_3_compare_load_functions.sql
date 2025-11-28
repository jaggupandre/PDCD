-- ======================================================================
-- CLEANUP
-- ======================================================================
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;

DROP SCHEMA IF EXISTS analytics_schema CASCADE;
CREATE SCHEMA analytics_schema;

-- ======================================================================
-- BASIC TABLE (NEEDED FOR TESTS)
-- ======================================================================
CREATE TABLE analytics_schema.employees (
    employee_id SERIAL PRIMARY KEY,
    first_name  TEXT,
    last_name   TEXT
);

-- ======================================================================
-- ======================= TEST 1 — BASIC DEFINER ========================
-- ======================================================================
CREATE OR REPLACE FUNCTION analytics_schema.secure_fn_basic(a INT)
RETURNS INT
LANGUAGE SQL
SECURITY DEFINER
AS $$
    SELECT a + 100;
$$;

--SELECT * FROM pdcd_schema.load_snapshot_tbl();
--SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ======================================================================
-- ===================== TEST 2 — UNQUALIFIED TABLE ======================
-- ======================================================================
-- CREATE TABLE IF NOT EXISTS employees(employee_id int);
CREATE OR REPLACE FUNCTION analytics_schema.secure_fn_unqualified()
RETURNS INT
LANGUAGE SQL
SECURITY DEFINER
AS $$
    SELECT COUNT(*) FROM employees;
$$;

--SELECT * FROM pdcd_schema.load_snapshot_tbl();
--SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ======================================================================
-- ================== TEST 3 — SAFE search_path DEFINER ==================
-- ======================================================================

CREATE OR REPLACE FUNCTION analytics_schema.secure_fn_safe()
RETURNS INT
LANGUAGE SQL
SECURITY DEFINER
SET search_path = analytics_schema, pg_catalog
AS $$
    SELECT COUNT(*) FROM analytics_schema.employees;
$$;

--SELECT * FROM pdcd_schema.load_snapshot_tbl();
--SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ======================================================================
-- ================== TEST 4 — INDIRECT UNQUALIFIED CALL =================
-- ======================================================================
-- CREATE OR REPLACE FUNCTION analytics_schema.helper_fn(x INT)
-- RETURNS INT
-- LANGUAGE SQL
-- AS $$ SELECT x * 2; $$;

-- CREATE OR REPLACE FUNCTION analytics_schema.secure_fn_indirect(x INT)
-- RETURNS INT
-- LANGUAGE SQL
-- SECURITY DEFINER
-- AS $$
--     SELECT helper_fn(x);
-- $$;

--SELECT * FROM pdcd_schema.load_snapshot_tbl();
--SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ======================================================================
-- ===================== TEST 5 — PLPGSQL DEFINER ========================
-- ======================================================================
CREATE OR REPLACE FUNCTION analytics_schema.secure_fn_plpgsql(emp_id INT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    emp_name TEXT;
BEGIN
    SELECT first_name INTO emp_name
    FROM analytics_schema.employees
    WHERE employee_id = emp_id;

    RETURN 'Employee: ' || emp_name;
END;
$$;

--SELECT * FROM pdcd_schema.load_snapshot_tbl();
--SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ======================================================================
-- ================= TEST 6 — PUBLIC FIRST IN search_path =================
-- ======================================================================
CREATE OR REPLACE FUNCTION analytics_schema.secure_fn_unsafe_path()
RETURNS INT
LANGUAGE SQL
SECURITY DEFINER
SET search_path = public, analytics_schema, pg_catalog
AS $$
    SELECT 1;
$$;

--SELECT * FROM pdcd_schema.load_snapshot_tbl();
--SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ======================================================================
-- ================= TEST 7 — search_path MISSING pg_catalog ==============
-- ======================================================================
CREATE OR REPLACE FUNCTION analytics_schema.secure_fn_missing_pg_catalog()
RETURNS INT
LANGUAGE SQL
SECURITY DEFINER
SET search_path = analytics_schema
AS $$
    SELECT 1;
$$;

--SELECT * FROM pdcd_schema.load_snapshot_tbl();
--SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ======================================================================
-- ==================== TEST 8 — WEIRD search_path =======================
-- ======================================================================
CREATE OR REPLACE FUNCTION analytics_schema.secure_fn_weird()
RETURNS INT
LANGUAGE SQL
SECURITY DEFINER
SET search_path = "Analytics_Schema"  ,pg_catalog
AS $$
    SELECT 10;
$$;

--SELECT * FROM pdcd_schema.load_snapshot_tbl();
--SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ======================================================================
-- ===================== TEST 9 — pg_temp USE ============================
-- ======================================================================
CREATE TABLE IF NOT EXISTS pg_temp.temp_tbl(temp_id int);
CREATE OR REPLACE FUNCTION analytics_schema.secure_fn_temp()
RETURNS INT
LANGUAGE SQL
SECURITY DEFINER
AS $$
    SELECT COUNT(*) FROM pg_temp.temp_tbl;
$$;

--SELECT * FROM pdcd_schema.load_snapshot_tbl();
--SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ======================================================================
-- ==================== TEST 10 — DYNAMIC SQL ============================
-- ======================================================================
CREATE OR REPLACE FUNCTION analytics_schema.secure_fn_dynamic(tab TEXT)
RETURNS INT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    result INT;
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM ' || tab INTO result;
    RETURN result;
END;
$$;

--SELECT * FROM pdcd_schema.load_snapshot_tbl();
--SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ======================================================================
-- ======================= FINAL RESULT CHECK ============================
-- ======================================================================
SELECT
    schema_name,
    function_name,
    is_security_definer,
    search_path_value,
    search_path_is_safe,
    config_settings_text,
    function_body
FROM pdcd_schema.md5_metadata_staging_tbl
WHERE object_type = 'Function'
ORDER BY function_name;


test_db=# SELECT * FROM pdcd_schema.get_functions_details(ARRAY['analytics_schema']);
   schema_name    |           function_name           | argument_types | argument_modes | return_type | language | volatility | parallel_safe |   owner_role   | privileges | dependencies | is_security_definer |                   config_settings                    |               config_settings_text               |          search_path_value           | search_path_is_safe |                                                      function_body
------------------+-----------------------------------+----------------+----------------+-------------+----------+------------+---------------+----------------+------------+--------------+---------------------+------------------------------------------------------+--------------------------------------------------+--------------------------------------+---------------------+--------------------------------------------------------------------------------------------------------------------------
 analytics_schema | secure_fn_basic(a integer)        | a integer      | a integer      | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   |                                                      |                                                  |                                      | f                   |  SELECT a + 100;
 analytics_schema | secure_fn_unqualified()           |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   |                                                      |                                                  |                                      | f                   |  SELECT COUNT(*) FROM employees;
 analytics_schema | secure_fn_safe()                  |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   | {"search_path=analytics_schema, pg_catalog"}         | search_path=analytics_schema, pg_catalog         | analytics_schema, pg_catalog         | t                   |  SELECT COUNT(*) FROM analytics_schema.employees;
 analytics_schema | secure_fn_plpgsql(emp_id integer) | emp_id integer | emp_id integer | text        | plpgsql  | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941,13906 | t                   |                                                      |                                                  |                                      | f                   | DECLARE emp_name TEXT; BEGIN SELECT first_name INTO emp_name FROM analytics_schema.employees WHERE employee_id = emp_id;
 analytics_schema | secure_fn_unsafe_path()           |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   | {"search_path=public, analytics_schema, pg_catalog"} | search_path=public, analytics_schema, pg_catalog | public, analytics_schema, pg_catalog | f                   |  SELECT 1;
 analytics_schema | secure_fn_missing_pg_catalog()    |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   | {search_path=analytics_schema}                       | search_path=analytics_schema                     | analytics_schema                     | f                   |  SELECT 1;
 analytics_schema | secure_fn_weird()                 |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   | {"search_path=\"Analytics_Schema\", pg_catalog"}     | search_path="Analytics_Schema", pg_catalog       | "Analytics_Schema", pg_catalog       | f                   |  SELECT 10;
 analytics_schema | secure_fn_temp()                  |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   |                                                      |                                                  |                                      | f                   |  SELECT COUNT(*) FROM pg_temp.temp_tbl;
 analytics_schema | secure_fn_dynamic(tab text)       | tab text       | tab text       | integer     | plpgsql  | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941,13906 | t                   |                                                      |                                                  |                                      | f                   | DECLARE result INT; BEGIN EXECUTE 'SELECT COUNT(*) FROM ' || tab INTO result;
(9 rows)

   schema_name    |           function_name           | argument_types | argument_modes | return_type | language | volatility | parallel_safe |   owner_role   | privileges | dependencies | is_security_definer |               config_settings_text               |                                                      function_body
------------------+-----------------------------------+----------------+----------------+-------------+----------+------------+---------------+----------------+------------+--------------+---------------------+--------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------
 analytics_schema | secure_fn_basic(a integer)        | a integer      | a integer      | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   |                                                  |  SELECT a + 100;
 analytics_schema | secure_fn_unqualified()           |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   |                                                  |  SELECT COUNT(*) FROM employees;
 analytics_schema | secure_fn_safe()                  |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   | search_path=analytics_schema, pg_catalog         |  SELECT COUNT(*) FROM analytics_schema.employees;
 analytics_schema | secure_fn_plpgsql(emp_id integer) | emp_id integer | emp_id integer | text        | plpgsql  | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941,13906 | t                   |                                                  | DECLARE emp_name TEXT; BEGIN SELECT first_name INTO emp_name FROM analytics_schema.employees WHERE employee_id = emp_id;
 analytics_schema | secure_fn_unsafe_path()           |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   | search_path=public, analytics_schema, pg_catalog |  SELECT 1;
 analytics_schema | secure_fn_missing_pg_catalog()    |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   | search_path=analytics_schema                     |  SELECT 1;
 analytics_schema | secure_fn_weird()                 |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   | search_path="Analytics_Schema", pg_catalog       |  SELECT 10;
 analytics_schema | secure_fn_temp()                  |                |                | integer     | sql      | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941       | t                   |                                                  |  SELECT COUNT(*) FROM pg_temp.temp_tbl;
 analytics_schema | secure_fn_dynamic(tab text)       | tab text       | tab text       | integer     | plpgsql  | VOLATILE   | UNSAFE        | jagdish_pandre |            | 170941,13906 | t                   |                                                  | DECLARE result INT; BEGIN EXECUTE 'SELECT COUNT(*) FROM ' || tab INTO result;
(9 rows)