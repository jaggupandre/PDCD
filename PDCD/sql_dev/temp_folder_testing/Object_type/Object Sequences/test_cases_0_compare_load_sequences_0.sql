
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
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
 snapshot_id |   schema_name    | object_type_name | object_subtype |      object_subtype_name      |            object_md5            | change_type |                                                                                                                            object_subtype_details
-------------+------------------+------------------+----------------+-------------------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           1 | analytics_schema | departments      | Column         | department_id                 | 57cdd3e718f6f0349c77a716434d09f8 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1
           1 | analytics_schema | departments      | Column         | department_name               | 5a841b9bbc928694255504765a33a956 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2
           1 | analytics_schema | departments      | Column         | main_location                 | d6e08099dee6077445cfbdf123772ea9 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3
           1 | analytics_schema | departments      | Column         | ternary_location              | a4e83f1f87ade648d39ec2e7fbdce011 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4
           1 | analytics_schema | departments      | Column         | manager_id                    | 840f65e90f5ad043ed195535e39dc868 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           1 | analytics_schema | departments      | Column         | budget_code                   | 4c9087b4da4db15878fc5dffc5f1482a | ADDED       | data_type:character varying,max_length:50,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6
           1 | analytics_schema | employees        | Column         | employee_id                   | 6b1b0723fc761b4f80b3ebd7347a2adc | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.employees_employee_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_pkey,ordinal_position:1
           1 | analytics_schema | employees        | Column         | first_name                    | 5a841b9bbc928694255504765a33a956 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2
           1 | analytics_schema | employees        | Column         | last_name                     | d6e08099dee6077445cfbdf123772ea9 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3
           1 | analytics_schema | employees        | Column         | email                         | dbd03119c5e663a65d2215ea7cce415c | ADDED       | data_type:character varying,max_length:150,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_email_key,ordinal_position:4
           1 | analytics_schema | employees        | Column         | phone_number                  | 8556e666adc57bad9ca0a20dd335cf50 | ADDED       | data_type:character varying,max_length:20,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           1 | analytics_schema | employees        | Column         | hire_date                     | 2802a1f37505eb5f6c985529b7f852ed | ADDED       | data_type:date,max_length:,numeric_precision:,numeric_scale:,nullable:NO,default_value:CURRENT_DATE,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6
           1 | analytics_schema | employees        | Column         | salary                        | 400aef400f3cb973b96caa66c27d549d | ADDED       | data_type:numeric,max_length:,numeric_precision:10,numeric_scale:2,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7
           1 | analytics_schema | employees        | Column         | department_id                 | 7f736d83e67a0fad0f24d0fcab4566b3 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_department_id_fkey,ordinal_position:8
           1 | analytics_schema | departments      | Constraint     | departments_pkey              | 6f00aac61324af405b95307a19340808 | ADDED       | constraint_type:PRIMARY KEY,column_name:department_id,definition:PRIMARY KEY (department_id)
           1 | analytics_schema | employees        | Constraint     | employees_department_id_fkey  | d15c5860c9bccaa7101d3622732d6aa7 | ADDED       | constraint_type:FOREIGN KEY,column_name:department_id,definition:FOREIGN KEY (department_id) REFERENCES analytics_schema.departments(department_id)
           1 | analytics_schema | employees        | Constraint     | employees_email_key           | 7366006ed8864803a9082211343f7a9d | ADDED       | constraint_type:UNIQUE,column_name:email,definition:UNIQUE (email)
           1 | analytics_schema | employees        | Constraint     | employees_pkey                | 46e4d7c138cb7c7937bfd192b807b708 | ADDED       | constraint_type:PRIMARY KEY,column_name:employee_id,definition:PRIMARY KEY (employee_id)
           1 | analytics_schema | departments      | Index          | departments_pkey              | b42e577e3d023d664d051edb062f8b89 | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX departments_pkey ON analytics_schema.departments USING btree (department_id),is_unique:true,is_primary:true,index_columns:department_id,index_predicate:,access_method:btree
           1 | analytics_schema | employees        | Index          | employees_email_key           | 46afff75141550028b030f851dff4823 | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX employees_email_key ON analytics_schema.employees USING btree (email),is_unique:true,is_primary:false,index_columns:email,index_predicate:,access_method:btree
           1 | analytics_schema | employees        | Index          | employees_pkey                | 2ead719c29bb313dcd2fb7888d34134b | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX employees_pkey ON analytics_schema.employees USING btree (employee_id),is_unique:true,is_primary:true,index_columns:employee_id,index_predicate:,access_method:btree
           1 | analytics_schema | employees        | Reference      | employees_department_id_fkey  | c46dee26800ff09f71c42b9e2c940139 | ADDED       | source_column:department_id,target_schema:analytics_schema,target_table:departments,target_column:department_id,constraint_name:employees_department_id_fkey
           1 | analytics_schema | departments      | Sequence       | departments_department_id_seq | b66daad0e2adfd2f5532848abbc732cb | ADDED       | owned_by:analytics_schema.departments.department_id,sequence_type:SERIAL,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
           1 | analytics_schema | employees        | Sequence       | employees_employee_id_seq     | d32484bda74d320e26998f12cae3fbce | ADDED       | owned_by:analytics_schema.employees.employee_id,sequence_type:SERIAL,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
(24 rows)

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type, object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);

 snapshot_id |   schema_name    |    object_type_name     | object_subtype | object_subtype_name |            object_md5            | change_type |                                                                                                                                                                object_subtype_details
-------------+------------------+-------------------------+----------------+---------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           2 | analytics_schema | generate_snowflake_id() |                |                     | bf9c3bad7b5be7f13b55cacd84f11ed5 | ADDED       | argument_types:,argument_modes:,return_type:bigint,language:plpgsql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:171099,13906,function_body:DECLARE seq_id BIGINT; ts BIGINT; BEGIN ts := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT; seq_id := nextval('analytics_schema.snowflake_seq');
           2 | analytics_schema | get_daily_id()          |                |                     | 966bba2953556bf2191c8ff813693a86 | ADDED       | argument_types:,argument_modes:,return_type:text,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:171099,function_body: SELECT to_char(CURRENT_DATE, 'YYYYMMDD') || LPAD(nextval('analytics_schema.daily_seq')::text, 5, '0');
           2 | analytics_schema | cyclic_seq              |                |                     | 3e8b854f55d254596634efa118fc7b4f | ADDED       | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:1000,increment_by:1,cycle_option:YES,cache_size:10
           2 | analytics_schema | daily_seq               |                |                     | 641db9cc55a313ec034a4306255f22fb | ADDED       | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:99999,increment_by:1,cycle_option:YES,cache_size:1
           2 | analytics_schema | global_id_seq           |                |                     | 120312b0a9f0fe77b5a5fed9f0daedac | ADDED       | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1000000000,minimum_value:1,maximum_value:9223372036854775807,increment_by:10,cycle_option:NO,cache_size:200
           2 | analytics_schema | owned_seq               |                |                     | f2b79aa0dfe0539cd62778f0d5656f16 | ADDED       | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:9223372036854775807,increment_by:1,cycle_option:NO,cache_size:1
           2 | analytics_schema | reverse_seq             |                |                     | 9e08ba87139bd405be2bd842f36a3c6e | ADDED       | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:0,minimum_value:-999999,maximum_value:0,increment_by:-1,cycle_option:NO,cache_size:20
           2 | analytics_schema | secured_seq             |                |                     | 803da5c594068414d9ccdff1b57a7e4a | ADDED       | owned_by:,sequence_type:MANUAL,privileges:{jagdish_pandre=rwU/jagdish_pandre,app_read_role=rU/jagdish_pandre,app_write_role=w/jagdish_pandre},data_type:bigint,start_value:1,minimum_value:1,maximum_value:9223372036854775807,increment_by:1,cycle_option:NO,cache_size:1
           2 | analytics_schema | small_seq               |                |                     | f7a431e802bf06c1254b87131e8d47c0 | ADDED       | owned_by:,sequence_type:MANUAL,privileges:,data_type:smallint,start_value:1,minimum_value:1,maximum_value:32767,increment_by:1,cycle_option:NO,cache_size:1
           2 | analytics_schema | snowflake_seq           |                |                     | e20de0482ad0786e197a13786d9b2a49 | ADDED       | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:4095,increment_by:1,cycle_option:YES,cache_size:50
           2 | analytics_schema | std_seq                 |                |                     | 4420007627af413a40bfc098156f7fbc | ADDED       | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:9223372036854775807,increment_by:1,cycle_option:NO,cache_size:50
(11 rows)
--todo--------------------------------------------------------------------------
-- TEST CASES FOR OBJECT TYPE: SEQUENCES
-- ==========================================
SElECT * FROM pdcd_schema.get_sequence_details(ARRAY['analytics_schema']);

SELECT * FROM pdcd_schema.get_table_sequences_md5(ARRAY['analytics_schema']);
SELECT * FROM pdcd_schema.get_object_sequences_md5(ARRAY['analytics_schema']);
-- ==========================================

-- ============================================================
--  SCHEMA PREPARATION (optional)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS analytics_schema;
CREATE SCHEMA IF NOT EXISTS core_ids;

-- ============================================================
-- 1. STANDARD STANDALONE SEQUENCE
-- ============================================================
CREATE SEQUENCE analytics_schema.std_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 50
    NO CYCLE;


-- ============================================================
-- 2. CYCLIC SEQUENCE (auto-wrap)
-- ============================================================
CREATE SEQUENCE analytics_schema.cyclic_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 1000
    CYCLE
    CACHE 10;



-- ============================================================
-- 3. REVERSE / DECREMENTING SEQUENCE
-- ============================================================
CREATE SEQUENCE analytics_schema.reverse_seq
    START WITH 0
    INCREMENT BY -1
    MINVALUE -999999
    MAXVALUE 0
    CACHE 20
    NO CYCLE;



-- ============================================================
-- 4. SMALLINT RANGE SEQUENCE
-- ============================================================
CREATE SEQUENCE analytics_schema.small_seq
    AS SMALLINT
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 32767
    NO CYCLE;


-- ============================================================
-- 6. SEQUENCE WITH EXPLICIT OWNER ROLE
-- ============================================================
CREATE ROLE data_engineer_role LOGIN;

CREATE SEQUENCE analytics_schema.owned_seq
    START WITH 1
    INCREMENT BY 1
    NO CYCLE
    OWNED BY NONE;

ALTER SEQUENCE analytics_schema.owned_seq OWNER TO data_engineer_role;

DROP SEQUENCE analytics_schema.owned_seq;
DROP ROLE data_engineer_role;




-- ============================================================
-- 7. SEQUENCE WITH PRIVILEGES (GRANTS / REVOKE)
-- ============================================================
CREATE ROLE app_read_role NOLOGIN;
CREATE ROLE app_write_role NOLOGIN;

CREATE SEQUENCE analytics_schema.secured_seq
    START WITH 1
    INCREMENT BY 1
    NO CYCLE;

-- Remove permissions from the public
REVOKE ALL ON SEQUENCE analytics_schema.secured_seq FROM PUBLIC;

-- Restrictive access
GRANT USAGE, SELECT ON SEQUENCE analytics_schema.secured_seq TO app_read_role;
GRANT UPDATE ON SEQUENCE analytics_schema.secured_seq TO app_write_role;

DROP SEQUENCE analytics_schema.secured_seq;
DROP ROLE app_read_role;
DROP ROLE app_write_role;

-- ============================================================
-- 8. CUSTOM SCHEMA SEQUENCE
-- ============================================================
CREATE SEQUENCE core_ids.customer_id_seq
    START WITH 5000
    INCREMENT BY 1
    NO CYCLE
    CACHE 100;



-- ============================================================
-- 9. SECURITY DEFINER WRAPPER FOR NEXTVAL
-- ============================================================
CREATE OR REPLACE FUNCTION core_ids.get_customer_id()
RETURNS BIGINT
LANGUAGE SQL
SECURITY DEFINER
AS $$
    SELECT nextval('core_ids.customer_id_seq');
$$;

REVOKE ALL ON FUNCTION core_ids.get_customer_id() FROM PUBLIC;



-- ============================================================
-- 10. GLOBAL DISTRIBUTED-ID SEQUENCE
-- ============================================================
CREATE SEQUENCE analytics_schema.global_id_seq
    START WITH 1000000000
    INCREMENT BY 10
    CACHE 200
    NO CYCLE;



-- ============================================================
-- 11. SNOWFLAKE-STYLE HYBRID SEQUENCE
-- ============================================================
CREATE SEQUENCE analytics_schema.snowflake_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 4095
    CYCLE
    CACHE 50;

CREATE OR REPLACE FUNCTION analytics_schema.generate_snowflake_id()
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    seq_id BIGINT;
    ts BIGINT;
BEGIN
    ts := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
    seq_id := nextval('analytics_schema.snowflake_seq');

    RETURN (ts << 12) | seq_id;
END;
$$;



-- ============================================================
-- 12. DAILY RESET SEQUENCE + DATE PREFIX FUNCTION
-- ============================================================
CREATE SEQUENCE analytics_schema.daily_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 99999
    CYCLE;

CREATE OR REPLACE FUNCTION analytics_schema.get_daily_id()
RETURNS TEXT
LANGUAGE SQL
AS $$
SELECT 
    to_char(CURRENT_DATE, 'YYYYMMDD') || 
    LPAD(nextval('analytics_schema.daily_seq')::text, 5, '0');
$$;



-- ============================================================
-- END OF SCRIPT
-- ============================================================


test_db=# SElECT * FROM pdcd_schema.get_sequence_details(ARRAY['analytics_schema']);
   schema_name    |         sequence_name         | table_name  |                  owned_by                  | sequence_type |                                             privileges                                              | data_type | start_value | minimum_value |    maximum_value    | increment_by | cycle_option | cache_size
------------------+-------------------------------+-------------+--------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------+-----------+-------------+---------------+---------------------+--------------+--------------+------------
 analytics_schema | cyclic_seq                    |             |                                            | MANUAL        |                                                                                                     | bigint    |           1 |             1 |                1000 |            1 | YES          |         10
 analytics_schema | daily_seq                     |             |                                            | MANUAL        |                                                                                                     | bigint    |           1 |             1 |               99999 |            1 | YES          |          1
 analytics_schema | departments_department_id_seq | departments | analytics_schema.departments.department_id | SERIAL        |                                                                                                     | integer   |           1 |             1 |          2147483647 |            1 | NO           |          1
 analytics_schema | employees_employee_id_seq     | employees   | analytics_schema.employees.employee_id     | SERIAL        |                                                                                                     | integer   |           1 |             1 |          2147483647 |            1 | NO           |          1
 analytics_schema | global_id_seq                 |             |                                            | MANUAL        |                                                                                                     | bigint    |  1000000000 |             1 | 9223372036854775807 |           10 | NO           |        200
 analytics_schema | owned_seq                     |             |                                            | MANUAL        |                                                                                                     | bigint    |           1 |             1 | 9223372036854775807 |            1 | NO           |          1
 analytics_schema | reverse_seq                   |             |                                            | MANUAL        |                                                                                                     | bigint    |           0 |       -999999 |                   0 |           -1 | NO           |         20
 analytics_schema | secured_seq                   |             |                                            | MANUAL        | {jagdish_pandre=rwU/jagdish_pandre,app_read_role=rU/jagdish_pandre,app_write_role=w/jagdish_pandre} | bigint    |           1 |             1 | 9223372036854775807 |            1 | NO           |          1
 analytics_schema | small_seq                     |             |                                            | MANUAL        |                                                                                                     | smallint  |           1 |             1 |               32767 |            1 | NO           |          1
 analytics_schema | snowflake_seq                 |             |                                            | MANUAL        |                                                                                                     | bigint    |           1 |             1 |                4095 |            1 | YES          |         50
 analytics_schema | std_seq                       |             |                                            | MANUAL        |                                                                                                     | bigint    |           1 |             1 | 9223372036854775807 |            1 | NO           |         50
(11 rows)

test_db=# SELECT * FROM pdcd_schema.get_table_sequences_md5(ARRAY['analytics_schema']);
   schema_name    | object_type | object_type_name | object_subtype |      object_subtype_name      |                                                                                          object_subtype_details                                                                                           |            object_md5
------------------+-------------+------------------+----------------+-------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------
 analytics_schema | Table       | departments      | Sequence       | departments_department_id_seq | owned_by:analytics_schema.departments.department_id,sequence_type:SERIAL,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1 | b66daad0e2adfd2f5532848abbc732cb
 analytics_schema | Table       | employees        | Sequence       | employees_employee_id_seq     | owned_by:analytics_schema.employees.employee_id,sequence_type:SERIAL,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1     | d32484bda74d320e26998f12cae3fbce
(2 rows)

test_db=# SELECT * FROM pdcd_schema.get_object_sequences_md5(ARRAY['analytics_schema']);
   schema_name    | object_type | object_type_name | object_subtype | object_subtype_name |                                                                                                                           object_subtype_details                                                                                                                           |            object_md5
------------------+-------------+------------------+----------------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------
 analytics_schema | Sequence    | cyclic_seq       |                |                     | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:1000,increment_by:1,cycle_option:YES,cache_size:10                                                                                                                 | 3e8b854f55d254596634efa118fc7b4f
 analytics_schema | Sequence    | daily_seq        |                |                     | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:99999,increment_by:1,cycle_option:YES,cache_size:1                                                                                                                 | 641db9cc55a313ec034a4306255f22fb
 analytics_schema | Sequence    | global_id_seq    |                |                     | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1000000000,minimum_value:1,maximum_value:9223372036854775807,increment_by:10,cycle_option:NO,cache_size:200                                                                                        | 120312b0a9f0fe77b5a5fed9f0daedac
 analytics_schema | Sequence    | owned_seq        |                |                     | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:9223372036854775807,increment_by:1,cycle_option:NO,cache_size:1                                                                                                    | f2b79aa0dfe0539cd62778f0d5656f16
 analytics_schema | Sequence    | reverse_seq      |                |                     | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:0,minimum_value:-999999,maximum_value:0,increment_by:-1,cycle_option:NO,cache_size:20                                                                                                              | 9e08ba87139bd405be2bd842f36a3c6e
 analytics_schema | Sequence    | secured_seq      |                |                     | owned_by:,sequence_type:MANUAL,privileges:{jagdish_pandre=rwU/jagdish_pandre,app_read_role=rU/jagdish_pandre,app_write_role=w/jagdish_pandre},data_type:bigint,start_value:1,minimum_value:1,maximum_value:9223372036854775807,increment_by:1,cycle_option:NO,cache_size:1 | 803da5c594068414d9ccdff1b57a7e4a
 analytics_schema | Sequence    | small_seq        |                |                     | owned_by:,sequence_type:MANUAL,privileges:,data_type:smallint,start_value:1,minimum_value:1,maximum_value:32767,increment_by:1,cycle_option:NO,cache_size:1                                                                                                                | f7a431e802bf06c1254b87131e8d47c0
 analytics_schema | Sequence    | snowflake_seq    |                |                     | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:4095,increment_by:1,cycle_option:YES,cache_size:50                                                                                                                 | e20de0482ad0786e197a13786d9b2a49
 analytics_schema | Sequence    | std_seq          |                |                     | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:9223372036854775807,increment_by:1,cycle_option:NO,cache_size:50                                                                                                   | 4420007627af413a40bfc098156f7fbc
(9 rows)