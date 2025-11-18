--- =============================
--- ========= TEST CASES ========
--- =============================

TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;

DROP schema IF EXISTS analytics_schema CASCADE;
CREATE SCHEMA analytics_schema;

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

-- =============================================================
-- First Run
-- =============================================================
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
SELECT snapshot_id,object_type_name, object_subtype,    object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl;
 snapshot_id | object_type_name | object_subtype |     object_subtype_name      |            object_md5            | change_type |                                                                                                                            object_subtype_details
-------------+------------------+----------------+------------------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           1 | departments      | Column         | department_id                | 57cdd3e718f6f0349c77a716434d09f8 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1
           1 | departments      | Column         | department_name              | 5a841b9bbc928694255504765a33a956 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2
           1 | departments      | Column         | main_location                | d6e08099dee6077445cfbdf123772ea9 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3
           1 | departments      | Column         | ternary_location             | a4e83f1f87ade648d39ec2e7fbdce011 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4
           1 | departments      | Column         | manager_id                   | 840f65e90f5ad043ed195535e39dc868 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           1 | departments      | Column         | budget_code                  | 4c9087b4da4db15878fc5dffc5f1482a | ADDED       | data_type:character varying,max_length:50,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6
           1 | employees        | Column         | employee_id                  | 6b1b0723fc761b4f80b3ebd7347a2adc | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.employees_employee_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_pkey,ordinal_position:1
           1 | employees        | Column         | first_name                   | 5a841b9bbc928694255504765a33a956 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2
           1 | employees        | Column         | last_name                    | d6e08099dee6077445cfbdf123772ea9 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3
           1 | employees        | Column         | email                        | dbd03119c5e663a65d2215ea7cce415c | ADDED       | data_type:character varying,max_length:150,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_email_key,ordinal_position:4
           1 | employees        | Column         | phone_number                 | 8556e666adc57bad9ca0a20dd335cf50 | ADDED       | data_type:character varying,max_length:20,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           1 | employees        | Column         | hire_date                    | 2802a1f37505eb5f6c985529b7f852ed | ADDED       | data_type:date,max_length:,numeric_precision:,numeric_scale:,nullable:NO,default_value:CURRENT_DATE,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6
           1 | employees        | Column         | salary                       | 400aef400f3cb973b96caa66c27d549d | ADDED       | data_type:numeric,max_length:,numeric_precision:10,numeric_scale:2,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7
           1 | employees        | Column         | department_id                | 7f736d83e67a0fad0f24d0fcab4566b3 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_department_id_fkey,ordinal_position:8
           1 | departments      | Constraint     | departments_pkey             | 6f00aac61324af405b95307a19340808 | ADDED       | constraint_type:PRIMARY KEY,column_name:department_id,definition:PRIMARY KEY (department_id)
           1 | employees        | Constraint     | employees_department_id_fkey | d15c5860c9bccaa7101d3622732d6aa7 | ADDED       | constraint_type:FOREIGN KEY,column_name:department_id,definition:FOREIGN KEY (department_id) REFERENCES analytics_schema.departments(department_id)
           1 | employees        | Constraint     | employees_email_key          | 7366006ed8864803a9082211343f7a9d | ADDED       | constraint_type:UNIQUE,column_name:email,definition:UNIQUE (email)
           1 | employees        | Constraint     | employees_pkey               | 46e4d7c138cb7c7937bfd192b807b708 | ADDED       | constraint_type:PRIMARY KEY,column_name:employee_id,definition:PRIMARY KEY (employee_id)
           1 | departments      | Index          | departments_pkey             | b42e577e3d023d664d051edb062f8b89 | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX departments_pkey ON analytics_schema.departments USING btree (department_id),is_unique:true,is_primary:true,index_columns:department_id,index_predicate:,access_method:btree
           1 | employees        | Index          | employees_email_key          | 46afff75141550028b030f851dff4823 | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX employees_email_key ON analytics_schema.employees USING btree (email),is_unique:true,is_primary:false,index_columns:email,index_predicate:,access_method:btree
           1 | employees        | Index          | employees_pkey               | 2ead719c29bb313dcd2fb7888d34134b | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX employees_pkey ON analytics_schema.employees USING btree (employee_id),is_unique:true,is_primary:true,index_columns:employee_id,index_predicate:,access_method:btree
           1 | employees        | Reference      | employees_department_id_fkey | c46dee26800ff09f71c42b9e2c940139 | ADDED       | source_column:department_id,target_schema:analytics_schema,target_table:departments,target_column:department_id,constraint_name:employees_department_id_fkey
(22 rows)
--! EXCEPTION
-- ! what if we rename the column we are referencing, like department_id to dept_id?
-- Then 

ALTER TABLE analytics_schema.departments RENAME COLUMN department_id TO dept_id;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

SELECT snapshot_id,object_type_name, object_subtype,    object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl;


 snapshot_id | object_type_name | object_subtype |     object_subtype_name      |            object_md5            | change_type |                                                                                                                            object_subtype_details
-------------+------------------+----------------+------------------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           1 | departments      | Column         | department_id                | 57cdd3e718f6f0349c77a716434d09f8 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1
           1 | departments      | Column         | department_name              | 5a841b9bbc928694255504765a33a956 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2
           1 | departments      | Column         | main_location                | d6e08099dee6077445cfbdf123772ea9 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3
           1 | departments      | Column         | ternary_location             | a4e83f1f87ade648d39ec2e7fbdce011 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4
           1 | departments      | Column         | manager_id                   | 840f65e90f5ad043ed195535e39dc868 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           1 | departments      | Column         | budget_code                  | 4c9087b4da4db15878fc5dffc5f1482a | ADDED       | data_type:character varying,max_length:50,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6
           1 | employees        | Column         | employee_id                  | 6b1b0723fc761b4f80b3ebd7347a2adc | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.employees_employee_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_pkey,ordinal_position:1
           1 | employees        | Column         | first_name                   | 5a841b9bbc928694255504765a33a956 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2
           1 | employees        | Column         | last_name                    | d6e08099dee6077445cfbdf123772ea9 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3
           1 | employees        | Column         | email                        | dbd03119c5e663a65d2215ea7cce415c | ADDED       | data_type:character varying,max_length:150,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_email_key,ordinal_position:4
           1 | employees        | Column         | phone_number                 | 8556e666adc57bad9ca0a20dd335cf50 | ADDED       | data_type:character varying,max_length:20,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           1 | employees        | Column         | hire_date                    | 2802a1f37505eb5f6c985529b7f852ed | ADDED       | data_type:date,max_length:,numeric_precision:,numeric_scale:,nullable:NO,default_value:CURRENT_DATE,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6
           1 | employees        | Column         | salary                       | 400aef400f3cb973b96caa66c27d549d | ADDED       | data_type:numeric,max_length:,numeric_precision:10,numeric_scale:2,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7
           1 | employees        | Column         | department_id                | 7f736d83e67a0fad0f24d0fcab4566b3 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_department_id_fkey,ordinal_position:8
           1 | departments      | Constraint     | departments_pkey             | 6f00aac61324af405b95307a19340808 | ADDED       | constraint_type:PRIMARY KEY,column_name:department_id,definition:PRIMARY KEY (department_id)
           1 | employees        | Constraint     | employees_department_id_fkey | d15c5860c9bccaa7101d3622732d6aa7 | ADDED       | constraint_type:FOREIGN KEY,column_name:department_id,definition:FOREIGN KEY (department_id) REFERENCES analytics_schema.departments(department_id)
           1 | employees        | Constraint     | employees_email_key          | 7366006ed8864803a9082211343f7a9d | ADDED       | constraint_type:UNIQUE,column_name:email,definition:UNIQUE (email)
           1 | employees        | Constraint     | employees_pkey               | 46e4d7c138cb7c7937bfd192b807b708 | ADDED       | constraint_type:PRIMARY KEY,column_name:employee_id,definition:PRIMARY KEY (employee_id)
           1 | departments      | Index          | departments_pkey             | b42e577e3d023d664d051edb062f8b89 | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX departments_pkey ON analytics_schema.departments USING btree (department_id),is_unique:true,is_primary:true,index_columns:department_id,index_predicate:,access_method:btree
           1 | employees        | Index          | employees_email_key          | 46afff75141550028b030f851dff4823 | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX employees_email_key ON analytics_schema.employees USING btree (email),is_unique:true,is_primary:false,index_columns:email,index_predicate:,access_method:btree
           1 | employees        | Index          | employees_pkey               | 2ead719c29bb313dcd2fb7888d34134b | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX employees_pkey ON analytics_schema.employees USING btree (employee_id),is_unique:true,is_primary:true,index_columns:employee_id,index_predicate:,access_method:btree
           1 | employees        | Reference      | employees_department_id_fkey | c46dee26800ff09f71c42b9e2c940139 | ADDED       | source_column:department_id,target_schema:analytics_schema,target_table:departments,target_column:department_id,constraint_name:employees_department_id_fkey
           2 | employees        | Column         | dept_id                      | 7f736d83e67a0fad0f24d0fcab4566b3 | RENAMED     | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_department_id_fkey,ordinal_position:8
           2 | employees        | Constraint     | employees_department_id_fkey | b00ca1ec8f6a40ea15ed663d463fd544 | MODIFIED    | constraint_type:FOREIGN KEY,column_name:dept_id,definition:FOREIGN KEY (dept_id) REFERENCES analytics_schema.departments(department_id)
           2 | employees        | Reference      | employees_department_id_fkey | ed8cb39f967935b85b4ca70cc6cd5c31 | MODIFIED    | source_column:dept_id,target_schema:analytics_schema,target_table:departments,target_column:department_id,constraint_name:employees_department_id_fkey
(25 rows)

-- ! what if we rename the table we are referencing, like department_tbl to dept_tbl
ALTER TABLE analytics_schema.departments
    RENAME TO dept_master;
-- ! Every object related to that table will be Show as newly 
 snapshot_id | object_type_name | object_subtype |     object_subtype_name      |            object_md5            | change_type |                                                                                                                            object_subtype_details
-------------+------------------+----------------+------------------------------+----------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
           1 | departments      | Column         | department_id                | 57cdd3e718f6f0349c77a716434d09f8 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1
           1 | departments      | Column         | department_name              | 5a841b9bbc928694255504765a33a956 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2
           1 | departments      | Column         | main_location                | d6e08099dee6077445cfbdf123772ea9 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3
           1 | departments      | Column         | ternary_location             | a4e83f1f87ade648d39ec2e7fbdce011 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4
           1 | departments      | Column         | manager_id                   | 840f65e90f5ad043ed195535e39dc868 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           1 | departments      | Column         | budget_code                  | 4c9087b4da4db15878fc5dffc5f1482a | ADDED       | data_type:character varying,max_length:50,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6
           1 | employees        | Column         | employee_id                  | 6b1b0723fc761b4f80b3ebd7347a2adc | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.employees_employee_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_pkey,ordinal_position:1
           1 | employees        | Column         | first_name                   | 5a841b9bbc928694255504765a33a956 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:2
           1 | employees        | Column         | last_name                    | d6e08099dee6077445cfbdf123772ea9 | ADDED       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3
           1 | employees        | Column         | email                        | dbd03119c5e663a65d2215ea7cce415c | ADDED       | data_type:character varying,max_length:150,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_email_key,ordinal_position:4
           1 | employees        | Column         | phone_number                 | 8556e666adc57bad9ca0a20dd335cf50 | ADDED       | data_type:character varying,max_length:20,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5
           1 | employees        | Column         | hire_date                    | 2802a1f37505eb5f6c985529b7f852ed | ADDED       | data_type:date,max_length:,numeric_precision:,numeric_scale:,nullable:NO,default_value:CURRENT_DATE,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6
           1 | employees        | Column         | salary                       | 400aef400f3cb973b96caa66c27d549d | ADDED       | data_type:numeric,max_length:,numeric_precision:10,numeric_scale:2,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7
           1 | employees        | Column         | department_id                | 7f736d83e67a0fad0f24d0fcab4566b3 | ADDED       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:employees_department_id_fkey,ordinal_position:8
           
           1 | departments      | Constraint     | departments_pkey             | 6f00aac61324af405b95307a19340808 | ADDED       | constraint_type:PRIMARY KEY,column_name:department_id,definition:PRIMARY KEY (department_id)
           1 | employees        | Constraint     | employees_department_id_fkey | d15c5860c9bccaa7101d3622732d6aa7 | ADDED       | constraint_type:FOREIGN KEY,column_name:department_id,definition:FOREIGN KEY (department_id) REFERENCES analytics_schema.departments(department_id)
           1 | employees        | Constraint     | employees_email_key          | 7366006ed8864803a9082211343f7a9d | ADDED       | constraint_type:UNIQUE,column_name:email,definition:UNIQUE (email)
           1 | employees        | Constraint     | employees_pkey               | 46e4d7c138cb7c7937bfd192b807b708 | ADDED       | constraint_type:PRIMARY KEY,column_name:employee_id,definition:PRIMARY KEY (employee_id)
           
           1 | departments      | Index          | departments_pkey             | b42e577e3d023d664d051edb062f8b89 | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX departments_pkey ON analytics_schema.departments USING btree (department_id),is_unique:true,is_primary:true,index_columns:department_id,index_predicate:,access_method:btree
           1 | employees        | Index          | employees_email_key          | 46afff75141550028b030f851dff4823 | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX employees_email_key ON analytics_schema.employees USING btree (email),is_unique:true,is_primary:false,index_columns:email,index_predicate:,access_method:btree
           1 | employees        | Index          | employees_pkey               | 2ead719c29bb313dcd2fb7888d34134b | ADDED       | tablespace:,indexdef:CREATE UNIQUE INDEX employees_pkey ON analytics_schema.employees USING btree (employee_id),is_unique:true,is_primary:true,index_columns:employee_id,index_predicate:,access_method:btree
           
           1 | employees        | Reference      | employees_department_id_fkey | c46dee26800ff09f71c42b9e2c940139 | ADDED       | source_column:department_id,target_schema:analytics_schema,target_table:departments,target_column:department_id,constraint_name:employees_department_id_fkey
           
           2 | departments      | Column         | dept_id                      | 57cdd3e718f6f0349c77a716434d09f8 | RENAMED     | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1
           2 | departments      | Constraint     | departments_pkey             | 9e71d83424d14d7af950aa5eab35d0a1 | MODIFIED    | constraint_type:PRIMARY KEY,column_name:dept_id,definition:PRIMARY KEY (dept_id)
           2 | departments      | Index          | departments_pkey             | 2b27c7ee86642bc7e5ae5b193f5da326 | MODIFIED    | tablespace:,indexdef:CREATE UNIQUE INDEX departments_pkey ON analytics_schema.departments USING btree (dept_id),is_unique:true,is_primary:true,index_columns:dept_id,index_predicate:,access_method:btree
           
           2 | employees        | Constraint     | employees_department_id_fkey | 45e37a7e7262c340cbf1151fae22205f | MODIFIED    | constraint_type:FOREIGN KEY,column_name:department_id,definition:FOREIGN KEY (department_id) REFERENCES analytics_schema.departments(dept_id)
           2 | employees        | Reference      | employees_department_id_fkey | 8e98045c5ccda6e9b0690916bc4fde2b | MODIFIED    | source_column:department_id,target_schema:analytics_schema,target_table:departments,target_column:dept_id,constraint_name:employees_department_id_fkey
(27 rows)
