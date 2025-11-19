-- =========================================================
-- TEST CASES
-- =========================================================

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;

DROP schema IF EXISTS analytics_schema CASCADE;
CREATE SCHEMA analytics_schema;
-- ========================================================================================
-- Test Run 1 — Initial creation with SERIAL (auto sequence creation)
-- ========================================================================================
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100),
    location VARCHAR(100)
);

CREATE TABLE analytics_schema.employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    department_id INT REFERENCES analytics_schema.departments(department_id)
);
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
 snapshot_id |   schema_name    | object_type_name | object_subtype |      object_subtype_name      |            object_md5            | change_type |                                                                                                                            object_subtype_details
-------------+------------------+------------------+----------------+-------------------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           1 | analytics_schema | departments      | Column         | department_id                 | 57cdd3e718f6f0349c77a716434d09f8 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1
           1 | analytics_schema | departments      | Column         | department_name               | 75b7eaf502c34cd9264507f6cf2a9cde | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2
           1 | analytics_schema | departments      | Column         | location                      | d6e08099dee6077445cfbdf123772ea9 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3
           1 | analytics_schema | employees        | Column         | employee_id                   | 6b1b0723fc761b4f80b3ebd7347a2adc | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.employees_employee_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_pkey,ordinal_position:1
           1 | analytics_schema | employees        | Column         | first_name                    | 75b7eaf502c34cd9264507f6cf2a9cde | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2
           1 | analytics_schema | employees        | Column         | last_name                     | d6e08099dee6077445cfbdf123772ea9 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3
           1 | analytics_schema | employees        | Column         | department_id                 | b678dc10fb93850eb0cf441f84f0f853 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_department_id_fkey,ordinal_position:4
           1 | analytics_schema | departments      | Constraint     | departments_pkey              | 6f00aac61324af405b95307a19340808 | ADDED       | constraint_type:PRIMARY KEY,column_name:department_id,definition:PRIMARY KEY (department_id)
           1 | analytics_schema | employees        | Constraint     | employees_department_id_fkey  | d15c5860c9bccaa7101d3622732d6aa7 | ADDED       | constraint_type:FOREIGN KEY,column_name:department_id,definition:FOREIGN KEY (department_id) REFERENCES analytics_schema.departments(department_id)
           1 | analytics_schema | employees        | Constraint     | employees_pkey                | 46e4d7c138cb7c7937bfd192b807b708 | ADDED       | constraint_type:PRIMARY KEY,column_name:employee_id,definition:PRIMARY KEY (employee_id)
           1 | analytics_schema | departments      | Index          | departments_pkey              | b42e577e3d023d664d051edb062f8b89 | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX departments_pkey ON analytics_schema.departments USING btree (department_id),is_unique:true,is_primary:true,index_columns:department_id,index_predicate:,access_method:btree
           1 | analytics_schema | employees        | Index          | employees_pkey                | 2ead719c29bb313dcd2fb7888d34134b | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX employees_pkey ON analytics_schema.employees USING btree (employee_id),is_unique:true,is_primary:true,index_columns:employee_id,index_predicate:,access_method:btree
           1 | analytics_schema | employees        | Reference      | employees_department_id_fkey  | c46dee26800ff09f71c42b9e2c940139 | ADDED       | source_column:department_id,target_schema:analytics_schema,target_table:departments,target_column:department_id,constraint_name:employees_department_id_fkey
           1 | analytics_schema | departments      | Sequence       | departments_department_id_seq | b66daad0e2adfd2f5532848abbc732cb | ADDED       | owned_by:analytics_schema.departments.department_id,sequence_type:SERIAL,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
           1 | analytics_schema | employees        | Sequence       | employees_employee_id_seq     | d32484bda74d320e26998f12cae3fbce | ADDED       | owned_by:analytics_schema.employees.employee_id,sequence_type:SERIAL,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
(15 rows)

-- Purpose:
-- Detect SERIAL sequences: departments_department_id_seq, employees_employee_id_seq
-- Detect OWNED BY column
-- Detect sequence type = SERIAL
-- Detect table + column link through pg_depend

-- ========================================================================================
-- Test Run 2 — Convert SERIAL to IDENTITY + add new identity-backed columns
-- ========================================================================================
-- ! dropping and recreating a sequence on the same column will term sequence and column as modified, SERIAL -> IDENTITY

ALTER TABLE analytics_schema.departments ALTER COLUMN department_id DROP DEFAULT;
DROP SEQUENCE IF EXISTS analytics_schema.departments_department_id_seq;
ALTER TABLE analytics_schema.departments ALTER COLUMN department_id ADD GENERATED ALWAYS AS IDENTITY;

ALTER TABLE analytics_schema.departments
    ADD COLUMN legacy_code SERIAL;  -- creates a new sequence
ALTER TABLE analytics_schema.employees
    ADD COLUMN payroll_id INT GENERATED BY DEFAULT AS IDENTITY;

-- JOB
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
 snapshot_id |   schema_name    | object_type_name | object_subtype |      object_subtype_name      |            object_md5            | change_type |                                                                                                                   object_subtype_details
-------------+------------------+------------------+----------------+-------------------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           2 | analytics_schema | departments      | Column         | department_id                 | a88c60f162e673b265756bba67091413 | MODIFIED    | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:,is_identity:YES,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1
           2 | analytics_schema | departments      | Sequence       | departments_department_id_seq | b234cb737bbc25171ab06f2b6cd8e11b | MODIFIED    | owned_by:analytics_schema.departments.department_id,sequence_type:IDENTITY,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
           2 | analytics_schema | departments      | Column         | legacy_code                   | 8db120fdc078d0d08c6b45c08c4bcd7c | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_legacy_code_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4
           2 | analytics_schema | employees        | Column         | payroll_id                    | f0ee692ba91193d8525a16073448c802 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:,is_identity:YES,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           2 | analytics_schema | departments      | Sequence       | departments_legacy_code_seq   | 2a1d64810b499a88a7755011bbc5d2db | ADDED       | owned_by:analytics_schema.departments.legacy_code,sequence_type:SERIAL,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
           2 | analytics_schema | employees        | Sequence       | employees_payroll_id_seq      | 62c79d5dbe1bc860b184e8c9031a9977 | ADDED       | owned_by:analytics_schema.employees.payroll_id,sequence_type:IDENTITY,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
(6 rows)

-- Purpose:
-- Detect change of type SERIAL → IDENTITY
-- Detect new SERIAL-created sequence (legacy_code)
-- Detect new IDENTITY column (payroll_id)
-- Verify dependency type:
--     'a' = serial
--     'i' = identity


-- ========================================================================================
-- Test Run 3 — Renaming identity & serial columns + renaming table
-- ========================================================================================
ALTER TABLE analytics_schema.departments
    RENAME COLUMN legacy_code TO aux_code;

ALTER TABLE analytics_schema.employees
    RENAME COLUMN payroll_id TO comp_code;


-- JOB
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
 snapshot_id |   schema_name    | object_type_name | object_subtype |     object_subtype_name     |            object_md5            | change_type |                                                                                                                   object_subtype_details
-------------+------------------+------------------+----------------+-----------------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           3 | analytics_schema | departments      | Column         | aux_code                    | 8db120fdc078d0d08c6b45c08c4bcd7c | RENAMED     | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_legacy_code_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4
           3 | analytics_schema | employees        | Column         | comp_code                   | f0ee692ba91193d8525a16073448c802 | RENAMED     | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:,is_identity:YES,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           3 | analytics_schema | departments      | Sequence       | departments_legacy_code_seq | 5259a9694a81509e4e1f7b9655586f39 | MODIFIED    | owned_by:analytics_schema.departments.aux_code,sequence_type:SERIAL,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
           3 | analytics_schema | employees        | Sequence       | employees_payroll_id_seq    | 0cb7d67d62894dcb262448a0bb18b2b4 | MODIFIED    | owned_by:analytics_schema.employees.comp_code,sequence_type:IDENTITY,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
(4 rows)
-- PURPOSE:
-- Metadata must now show:
-- departments_aux_code_seq still exists but column name changed → function detects new column_name
-- sequence name does NOT change (intentional PG behavior)
--  Table rename must update table_name in sequence mapping
--  Sequence type must remain correct (SERIAL / IDENTITY)

-- ========================================================================================
-- Test Run 4 — Drop columns → sequences dropped automatically
-- ========================================================================================
ALTER TABLE analytics_schema.departments
    DROP COLUMN aux_code;

ALTER TABLE analytics_schema.employees
    DROP COLUMN comp_code;

-- Add a manual sequence for testing manual ownership
CREATE SEQUENCE analytics_schema.audit_seq
    START WITH 500
    INCREMENT BY 5;

ALTER TABLE analytics_schema.employees
    ADD COLUMN audit_id INT DEFAULT nextval('analytics_schema.audit_seq');

ALTER SEQUENCE analytics_schema.audit_seq
    OWNED BY analytics_schema.employees.audit_id;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
 snapshot_id |   schema_name    | object_type_name | object_subtype |     object_subtype_name     |            object_md5            | change_type |                                                                                                                   object_subtype_details
-------------+------------------+------------------+----------------+-----------------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           4 | analytics_schema | employees        | Column         | audit_id                    | cf83f8526f67c9ce6a42186ec614c9ac | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:nextval('analytics_schema.audit_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6
           4 | analytics_schema | employees        | Sequence       | audit_seq                   | d1d4e8f86c81585101e5e904ed30d891 | ADDED       | owned_by:analytics_schema.employees.audit_id,sequence_type:SERIAL,privileges:,data_type:bigint,start_value:500,minimum_value:1,maximum_value:9223372036854775807,increment_by:5,cycle_option:NO,cache_size:1
           4 | analytics_schema | departments      | Column         | aux_code                    | 8db120fdc078d0d08c6b45c08c4bcd7c | DELETED     | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_legacy_code_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4
           4 | analytics_schema | departments      | Sequence       | departments_legacy_code_seq | 5259a9694a81509e4e1f7b9655586f39 | DELETED     | owned_by:analytics_schema.departments.aux_code,sequence_type:SERIAL,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
           4 | analytics_schema | employees        | Column         | comp_code                   | f0ee692ba91193d8525a16073448c802 | DELETED     | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:,is_identity:YES,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           4 | analytics_schema | employees        | Sequence       | employees_payroll_id_seq    | 0cb7d67d62894dcb262448a0bb18b2b4 | DELETED     | owned_by:analytics_schema.employees.comp_code,sequence_type:IDENTITY,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
(6 rows)
-- ! GOT A BUG,
    -- That when manual sequence is not created and column does not own it that then object_type_name will be NULL and there will be an error loading and compare_load as well

-- Purpose:
-- Validate that dropping sequence-backed columns removes sequences
-- Detect manually created sequences
-- Detect OWNED BY correctly
-- Detect manual sequence type = MANUAL

-- ========================================================================================
-- Test Run 5 — Recreate sequence-backed columns with new types
-- ========================================================================================
ALTER TABLE analytics_schema.departments
    ADD COLUMN dept_code INT GENERATED ALWAYS AS IDENTITY;

ALTER TABLE analytics_schema.employees
    ADD COLUMN temp_serial SERIAL;

ALTER TABLE analytics_schema.employees
    ADD COLUMN fixed_id INT DEFAULT nextval('analytics_schema.audit_seq');  -- using old manual sequence again


    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type_name, object_subtype,    object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);
 snapshot_id |   schema_name    | object_type_name | object_subtype |    object_subtype_name    |            object_md5            | change_type |                                                                                                                  object_subtype_details
-------------+------------------+------------------+----------------+---------------------------+----------------------------------+-------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           5 | analytics_schema | departments      | Column         | dept_code                 | f0ee692ba91193d8525a16073448c802 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:,is_identity:YES,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           5 | analytics_schema | employees        | Column         | temp_serial               | ad3f1706e4bb528fe652146dadfe950d | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.employees_temp_serial_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7
           5 | analytics_schema | employees        | Column         | fixed_id                  | 0f310bbce72ba4d6a623c391ca1b3e04 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:nextval('analytics_schema.audit_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:8
           5 | analytics_schema | departments      | Sequence       | departments_dept_code_seq | 69ed0f21e1fab636b824ff2ee64c0695 | ADDED       | owned_by:analytics_schema.departments.dept_code,sequence_type:IDENTITY,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
           5 | analytics_schema | employees        | Sequence       | employees_temp_serial_seq | 8eb3821fd8b08e7cd313df3e7aa5d40a | ADDED       | owned_by:analytics_schema.employees.temp_serial,sequence_type:SERIAL,privileges:,data_type:integer,start_value:1,minimum_value:1,maximum_value:2147483647,increment_by:1,cycle_option:NO,cache_size:1
(5 rows)

-- Purpose:
-- Detect newly created identity sequence (dept_code)
-- Detect newly created serial sequence (temp_serial)
-- Detect manual sequence reused for a new column (audit_seq)
-- Ensure correct classification:
--     dept_code → IDENTITY
--     temp_serial → SERIAL
--     fixed_id → MANUAL