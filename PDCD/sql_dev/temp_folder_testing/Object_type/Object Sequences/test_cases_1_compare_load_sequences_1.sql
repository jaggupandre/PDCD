
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


-- ==========================================
-- 1. ADDITION
-- ==========================================
CREATE SEQUENCE analytics_schema.test_add_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 999999999
    CACHE 1
    NO CYCLE;


    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype, object_subtype_name, object_md5, change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);


-- ==========================================
-- 2. DELETION
-- ==========================================
DROP SEQUENCE analytics_schema.test_add_seq;



    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype, object_subtype_name, object_md5, change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);


-- ==========================================
-- 3. MODIFICATION
-- ==========================================
CREATE SEQUENCE analytics_schema.test_modify_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 999999
    CACHE 1
    NO CYCLE;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype, object_subtype_name, object_md5, change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);


ALTER SEQUENCE analytics_schema.test_modify_seq
    INCREMENT BY 5
    MINVALUE 1
    MAXVALUE 1000000
    RESTART WITH 1
    CACHE 20
    NO CYCLE;

 snapshot_id |   schema_name    | object_type_name | object_subtype | object_subtype_name |            object_md5            | change_type |                                                                    object_subtype_details
-------------+------------------+------------------+----------------+---------------------+----------------------------------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------
           5 | analytics_schema | test_modify_seq  |                |                     | 761c128aff4ccd451f1959a8d3c7489d | MODIFIED    | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:1000000,increment_by:5,cycle_option:NO,cache_size:20
(1 row)

----

DROP SEQUENCE analytics_schema.test_modify_seq;

CREATE SEQUENCE analytics_schema.test_modify_seq
    INCREMENT BY 5
    MINVALUE 10
    MAXVALUE 1000000
    RESTART WITH 10
    CACHE 20
    NO CYCLE;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_functions(ARRAY['analytics_schema']);

    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

-- ==========================================
-- 4. RENAMED
-- ==========================================
CREATE SEQUENCE analytics_schema.test_rename_seq
    START WITH 1;

ALTER SEQUENCE analytics_schema.test_rename_seq
    RENAME TO test_rename_seq_new;



SELECT snapshot_id, schema_name,object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
-- ==========================================


ALTER SEQUENCE analytics_schema.test_rename_seq_new
    RENAME TO test_rename_seq_new_0;

test_db=# SELECT snapshot_id, schema_name,object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
 snapshot_id |   schema_name    |   object_type_name    | object_subtype | object_subtype_name |            object_md5            | change_type |                                                                         object_subtype_details
-------------+------------------+-----------------------+----------------+---------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           9 | analytics_schema | test_rename_seq_new_0 |                |                     | f2b79aa0dfe0539cd62778f0d5656f16 | RENAMED     | owned_by:,sequence_type:MANUAL,privileges:,data_type:bigint,start_value:1,minimum_value:1,maximum_value:9223372036854775807,increment_by:1,cycle_option:NO,cache_size:1
(1 row)