TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;

--- =============================
--- ========= TEST CASES ========
--- =============================


DROP table IF EXISTS analytics_schema.employees;
DROP table IF EXISTS analytics_schema.departments;

DROP SCHEMA IF EXISTS analytics_schema CASCADE;
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


-- First Run
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

-- Main Table MD5 Metadata changes
select    metadata_id,    snapshot_id,    object_type_name,    object_subtype,    object_subtype_name,  object_md5,    processed_time,  change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype |     object_subtype_name      |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+----------------+------------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | Column         | department_id                | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-18 18:14:29.872481 | ADDED
           2 |           1 | departments      | Column         | department_name              | 5a841b9bbc928694255504765a33a956 | 2025-11-18 18:14:29.875372 | ADDED
           3 |           1 | departments      | Column         | main_location                | d6e08099dee6077445cfbdf123772ea9 | 2025-11-18 18:14:29.875383 | ADDED
           4 |           1 | departments      | Column         | ternary_location             | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-18 18:14:29.87539  | ADDED
           5 |           1 | departments      | Column         | manager_id                   | 840f65e90f5ad043ed195535e39dc868 | 2025-11-18 18:14:29.875397 | ADDED
           6 |           1 | departments      | Column         | budget_code                  | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-18 18:14:29.875404 | ADDED
           7 |           1 | employees        | Column         | employee_id                  | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-18 18:14:29.875409 | ADDED
           8 |           1 | employees        | Column         | first_name                   | 5a841b9bbc928694255504765a33a956 | 2025-11-18 18:14:29.875414 | ADDED
           9 |           1 | employees        | Column         | last_name                    | d6e08099dee6077445cfbdf123772ea9 | 2025-11-18 18:14:29.87542  | ADDED
          10 |           1 | employees        | Column         | email                        | dbd03119c5e663a65d2215ea7cce415c | 2025-11-18 18:14:29.875426 | ADDED
          11 |           1 | employees        | Column         | phone_number                 | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-18 18:14:29.875432 | ADDED
          12 |           1 | employees        | Column         | hire_date                    | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-18 18:14:29.875437 | ADDED
          13 |           1 | employees        | Column         | salary                       | 400aef400f3cb973b96caa66c27d549d | 2025-11-18 18:14:29.875441 | ADDED
          14 |           1 | employees        | Column         | department_id                | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-18 18:14:29.875448 | ADDED
          15 |           1 | departments      | Constraint     | departments_pkey             | 6f00aac61324af405b95307a19340808 | 2025-11-18 18:14:29.877437 | ADDED
          16 |           1 | employees        | Constraint     | employees_department_id_fkey | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-18 18:14:29.877451 | ADDED
          17 |           1 | employees        | Constraint     | employees_email_key          | 7366006ed8864803a9082211343f7a9d | 2025-11-18 18:14:29.877456 | ADDED
          18 |           1 | employees        | Constraint     | employees_pkey               | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-18 18:14:29.877461 | ADDED
          19 |           1 | departments      | Index          | departments_pkey             | b42e577e3d023d664d051edb062f8b89 | 2025-11-18 18:14:29.880772 | ADDED
          20 |           1 | employees        | Index          | employees_email_key          | 46afff75141550028b030f851dff4823 | 2025-11-18 18:14:29.880805 | ADDED
          21 |           1 | employees        | Index          | employees_pkey               | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-18 18:14:29.880811 | ADDED
          22 |           1 | employees        | Reference      | employees_department_id_fkey | c46dee26800ff09f71c42b9e2c940139 | 2025-11-18 18:14:29.886075 | ADDED
          23 |           1 | departments      | Trigger        | trg_department_update_audit  | 3d8c39c09f64c264ca8b223ec8fb7773 | 2025-11-18 18:14:29.88838  | ADDED
          24 |           1 | employees        | Trigger        | trg_check_salary             | c4534188d54ab4ac1103ec1088d4e890 | 2025-11-18 18:14:29.890952 | ADDED
          25 |           1 | employees        | Trigger        | trg_employee_delete_cleanup  | a864b553fcaa0a3edc24e6cc9d57b1a8 | 2025-11-18 18:14:29.890959 | ADDED
          26 |           1 | employees        | Trigger        | trg_employee_insert_audit    | 82e54f0c8b98900e9ad56097a80ebd4c | 2025-11-18 18:14:29.890964 | ADDED
          27 |           1 | employees        | Trigger        | trg_employees_stmt_audit     | ad5a4c46f2169d47bc7aa23759ed76dc | 2025-11-18 18:14:29.890969 | ADDED
(27 rows)

-- Staging Table MD5 Metadata changes
-- select    metadata_id,    snapshot_id,    object_type_name,    object_subtype,    object_subtype_name,  object_md5,    processed_time FROM pdcd_schema.md5_metadata_staging_tbl;
metadata_id | snapshot_id | object_type_name | object_subtype |     object_subtype_name      |            object_md5            |       processed_time
-------------+-------------+------------------+----------------+------------------------------+----------------------------------+----------------------------
           1 |           1 | employees        | Column         | hire_date                    | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-18 18:14:31.177067
           2 |           1 | departments      | Column         | department_name              | 5a841b9bbc928694255504765a33a956 | 2025-11-18 18:14:31.179804
           3 |           1 | departments      | Column         | manager_id                   | 840f65e90f5ad043ed195535e39dc868 | 2025-11-18 18:14:31.179824
           4 |           1 | employees        | Column         | first_name                   | 5a841b9bbc928694255504765a33a956 | 2025-11-18 18:14:31.179831
           5 |           1 | employees        | Column         | last_name                    | d6e08099dee6077445cfbdf123772ea9 | 2025-11-18 18:14:31.179837
           6 |           1 | departments      | Column         | ternary_location             | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-18 18:14:31.179843
           7 |           1 | employees        | Column         | department_id                | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-18 18:14:31.17985
           8 |           1 | departments      | Column         | budget_code                  | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-18 18:14:31.179857
           9 |           1 | employees        | Column         | email                        | dbd03119c5e663a65d2215ea7cce415c | 2025-11-18 18:14:31.179863
          10 |           1 | departments      | Column         | department_id                | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-18 18:14:31.179869
          11 |           1 | employees        | Column         | phone_number                 | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-18 18:14:31.179876
          12 |           1 | employees        | Column         | salary                       | 400aef400f3cb973b96caa66c27d549d | 2025-11-18 18:14:31.179883
          13 |           1 | employees        | Column         | employee_id                  | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-18 18:14:31.179889
          14 |           1 | departments      | Column         | main_location                | d6e08099dee6077445cfbdf123772ea9 | 2025-11-18 18:14:31.179895
          15 |           1 | employees        | Constraint     | employees_pkey               | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-18 18:14:31.182294
          16 |           1 | employees        | Constraint     | employees_email_key          | 7366006ed8864803a9082211343f7a9d | 2025-11-18 18:14:31.182316
          17 |           1 | employees        | Constraint     | employees_department_id_fkey | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-18 18:14:31.182322
          18 |           1 | departments      | Constraint     | departments_pkey             | 6f00aac61324af405b95307a19340808 | 2025-11-18 18:14:31.182328
          19 |           1 | departments      | Index          | departments_pkey             | b42e577e3d023d664d051edb062f8b89 | 2025-11-18 18:14:31.18521
          20 |           1 | employees        | Index          | employees_email_key          | 46afff75141550028b030f851dff4823 | 2025-11-18 18:14:31.185232
          21 |           1 | employees        | Index          | employees_pkey               | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-18 18:14:31.18524
          22 |           1 | employees        | Reference      | employees_department_id_fkey | c46dee26800ff09f71c42b9e2c940139 | 2025-11-18 18:14:31.189383
          23 |           1 | departments      | Trigger        | trg_department_update_audit  | 3d8c39c09f64c264ca8b223ec8fb7773 | 2025-11-18 18:14:31.189714
          24 |           1 | employees        | Trigger        | trg_check_salary             | c4534188d54ab4ac1103ec1088d4e890 | 2025-11-18 18:14:31.189719
          25 |           1 | employees        | Trigger        | trg_employee_delete_cleanup  | a864b553fcaa0a3edc24e6cc9d57b1a8 | 2025-11-18 18:14:31.190759
          26 |           1 | employees        | Trigger        | trg_employees_stmt_audit     | ad5a4c46f2169d47bc7aa23759ed76dc | 2025-11-18 18:14:31.190761
          27 |           1 | employees        | Trigger        | trg_employee_insert_audit    | 82e54f0c8b98900e9ad56097a80ebd4c | 2025-11-18 18:14:31.190763
(27 rows)

---======== TEST CASES ========---
--=================================

-- Test Run 1 — Initial Additions & Modifications

-- MODIFY trigger function (content change)
CREATE OR REPLACE FUNCTION analytics_schema.fn_check_salary()
RETURNS trigger AS $$
BEGIN
    IF NEW.salary < 100 THEN   -- CHANGED from <0
        RAISE EXCEPTION 'Salary cannot be less than 100';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ADD new trigger to departments
CREATE TRIGGER trg_department_insert_audit
AFTER INSERT ON analytics_schema.departments
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_department_update_audit();   -- reusing function

 -- purpose : to check modification and addition of triggers

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

select    metadata_id,    snapshot_id,    object_type_name,    object_subtype,    object_subtype_name,  object_md5,    processed_time,  change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype |     object_subtype_name      |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+----------------+------------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | Column         | department_id                | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-18 18:14:29.872481 | ADDED
           2 |           1 | departments      | Column         | department_name              | 5a841b9bbc928694255504765a33a956 | 2025-11-18 18:14:29.875372 | ADDED
           3 |           1 | departments      | Column         | main_location                | d6e08099dee6077445cfbdf123772ea9 | 2025-11-18 18:14:29.875383 | ADDED
           4 |           1 | departments      | Column         | ternary_location             | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-18 18:14:29.87539  | ADDED
           5 |           1 | departments      | Column         | manager_id                   | 840f65e90f5ad043ed195535e39dc868 | 2025-11-18 18:14:29.875397 | ADDED
           6 |           1 | departments      | Column         | budget_code                  | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-18 18:14:29.875404 | ADDED
           7 |           1 | employees        | Column         | employee_id                  | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-18 18:14:29.875409 | ADDED
           8 |           1 | employees        | Column         | first_name                   | 5a841b9bbc928694255504765a33a956 | 2025-11-18 18:14:29.875414 | ADDED
           9 |           1 | employees        | Column         | last_name                    | d6e08099dee6077445cfbdf123772ea9 | 2025-11-18 18:14:29.87542  | ADDED
          10 |           1 | employees        | Column         | email                        | dbd03119c5e663a65d2215ea7cce415c | 2025-11-18 18:14:29.875426 | ADDED
          11 |           1 | employees        | Column         | phone_number                 | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-18 18:14:29.875432 | ADDED
          12 |           1 | employees        | Column         | hire_date                    | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-18 18:14:29.875437 | ADDED
          13 |           1 | employees        | Column         | salary                       | 400aef400f3cb973b96caa66c27d549d | 2025-11-18 18:14:29.875441 | ADDED
          14 |           1 | employees        | Column         | department_id                | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-18 18:14:29.875448 | ADDED
          15 |           1 | departments      | Constraint     | departments_pkey             | 6f00aac61324af405b95307a19340808 | 2025-11-18 18:14:29.877437 | ADDED
          16 |           1 | employees        | Constraint     | employees_department_id_fkey | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-18 18:14:29.877451 | ADDED
          17 |           1 | employees        | Constraint     | employees_email_key          | 7366006ed8864803a9082211343f7a9d | 2025-11-18 18:14:29.877456 | ADDED
          18 |           1 | employees        | Constraint     | employees_pkey               | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-18 18:14:29.877461 | ADDED
          19 |           1 | departments      | Index          | departments_pkey             | b42e577e3d023d664d051edb062f8b89 | 2025-11-18 18:14:29.880772 | ADDED
          20 |           1 | employees        | Index          | employees_email_key          | 46afff75141550028b030f851dff4823 | 2025-11-18 18:14:29.880805 | ADDED
          21 |           1 | employees        | Index          | employees_pkey               | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-18 18:14:29.880811 | ADDED
          22 |           1 | employees        | Reference      | employees_department_id_fkey | c46dee26800ff09f71c42b9e2c940139 | 2025-11-18 18:14:29.886075 | ADDED
          23 |           1 | departments      | Trigger        | trg_department_update_audit  | 3d8c39c09f64c264ca8b223ec8fb7773 | 2025-11-18 18:14:29.88838  | ADDED
          24 |           1 | employees        | Trigger        | trg_check_salary             | c4534188d54ab4ac1103ec1088d4e890 | 2025-11-18 18:14:29.890952 | ADDED
          25 |           1 | employees        | Trigger        | trg_employee_delete_cleanup  | a864b553fcaa0a3edc24e6cc9d57b1a8 | 2025-11-18 18:14:29.890959 | ADDED
          26 |           1 | employees        | Trigger        | trg_employee_insert_audit    | 82e54f0c8b98900e9ad56097a80ebd4c | 2025-11-18 18:14:29.890964 | ADDED
          27 |           1 | employees        | Trigger        | trg_employees_stmt_audit     | ad5a4c46f2169d47bc7aa23759ed76dc | 2025-11-18 18:14:29.890969 | ADDED
 
          28 |           2 | employees        | Trigger        | trg_check_salary             | e5fdfaa3559c42448fa2ad8f8705bb58 | 2025-11-18 18:16:35.328715 | MODIFIED
          29 |           2 | departments      | Trigger        | trg_department_insert_audit  | c2b7bfc45e5cb485cc9c7a23e6b6b4a1 | 2025-11-18 18:16:35.338004 | ADDED
(29 rows)

-- Test Run 2 — Renaming and Adding

-- RENAME trigger
ALTER TRIGGER trg_employee_insert_audit
ON analytics_schema.employees
RENAME TO trg_emp_insert_audit_v2;

-- ADD another new trigger
CREATE TRIGGER trg_employee_update_audit
AFTER UPDATE ON analytics_schema.employees
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_employee_insert_audit();
--  Purpose: check detection of renames and new trigger additions in same run.

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

select    metadata_id,    snapshot_id,    object_type_name,    object_subtype,    object_subtype_name,  object_md5,    processed_time,  change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype |     object_subtype_name      |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+----------------+------------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | Column         | department_id                | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-18 18:14:29.872481 | ADDED
           2 |           1 | departments      | Column         | department_name              | 5a841b9bbc928694255504765a33a956 | 2025-11-18 18:14:29.875372 | ADDED
           3 |           1 | departments      | Column         | main_location                | d6e08099dee6077445cfbdf123772ea9 | 2025-11-18 18:14:29.875383 | ADDED
           4 |           1 | departments      | Column         | ternary_location             | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-18 18:14:29.87539  | ADDED
           5 |           1 | departments      | Column         | manager_id                   | 840f65e90f5ad043ed195535e39dc868 | 2025-11-18 18:14:29.875397 | ADDED
           6 |           1 | departments      | Column         | budget_code                  | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-18 18:14:29.875404 | ADDED
           7 |           1 | employees        | Column         | employee_id                  | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-18 18:14:29.875409 | ADDED
           8 |           1 | employees        | Column         | first_name                   | 5a841b9bbc928694255504765a33a956 | 2025-11-18 18:14:29.875414 | ADDED
           9 |           1 | employees        | Column         | last_name                    | d6e08099dee6077445cfbdf123772ea9 | 2025-11-18 18:14:29.87542  | ADDED
          10 |           1 | employees        | Column         | email                        | dbd03119c5e663a65d2215ea7cce415c | 2025-11-18 18:14:29.875426 | ADDED
          11 |           1 | employees        | Column         | phone_number                 | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-18 18:14:29.875432 | ADDED
          12 |           1 | employees        | Column         | hire_date                    | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-18 18:14:29.875437 | ADDED
          13 |           1 | employees        | Column         | salary                       | 400aef400f3cb973b96caa66c27d549d | 2025-11-18 18:14:29.875441 | ADDED
          14 |           1 | employees        | Column         | department_id                | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-18 18:14:29.875448 | ADDED
          15 |           1 | departments      | Constraint     | departments_pkey             | 6f00aac61324af405b95307a19340808 | 2025-11-18 18:14:29.877437 | ADDED
          16 |           1 | employees        | Constraint     | employees_department_id_fkey | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-18 18:14:29.877451 | ADDED
          17 |           1 | employees        | Constraint     | employees_email_key          | 7366006ed8864803a9082211343f7a9d | 2025-11-18 18:14:29.877456 | ADDED
          18 |           1 | employees        | Constraint     | employees_pkey               | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-18 18:14:29.877461 | ADDED
          19 |           1 | departments      | Index          | departments_pkey             | b42e577e3d023d664d051edb062f8b89 | 2025-11-18 18:14:29.880772 | ADDED
          20 |           1 | employees        | Index          | employees_email_key          | 46afff75141550028b030f851dff4823 | 2025-11-18 18:14:29.880805 | ADDED
          21 |           1 | employees        | Index          | employees_pkey               | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-18 18:14:29.880811 | ADDED
          22 |           1 | employees        | Reference      | employees_department_id_fkey | c46dee26800ff09f71c42b9e2c940139 | 2025-11-18 18:14:29.886075 | ADDED
          23 |           1 | departments      | Trigger        | trg_department_update_audit  | 3d8c39c09f64c264ca8b223ec8fb7773 | 2025-11-18 18:14:29.88838  | ADDED
          24 |           1 | employees        | Trigger        | trg_check_salary             | c4534188d54ab4ac1103ec1088d4e890 | 2025-11-18 18:14:29.890952 | ADDED
          25 |           1 | employees        | Trigger        | trg_employee_delete_cleanup  | a864b553fcaa0a3edc24e6cc9d57b1a8 | 2025-11-18 18:14:29.890959 | ADDED
          26 |           1 | employees        | Trigger        | trg_employee_insert_audit    | 82e54f0c8b98900e9ad56097a80ebd4c | 2025-11-18 18:14:29.890964 | ADDED
          27 |           1 | employees        | Trigger        | trg_employees_stmt_audit     | ad5a4c46f2169d47bc7aa23759ed76dc | 2025-11-18 18:14:29.890969 | ADDED
 
          28 |           2 | employees        | Trigger        | trg_check_salary             | e5fdfaa3559c42448fa2ad8f8705bb58 | 2025-11-18 18:16:35.328715 | MODIFIED
          29 |           2 | departments      | Trigger        | trg_department_insert_audit  | c2b7bfc45e5cb485cc9c7a23e6b6b4a1 | 2025-11-18 18:16:35.338004 | ADDED
  
          30 |           3 | employees        | Trigger        | trg_emp_insert_audit_v2      | 82e54f0c8b98900e9ad56097a80ebd4c | 2025-11-18 18:24:24.63362  | RENAMED
          31 |           3 | employees        | Trigger        | trg_employee_update_audit    | 8f62a22248034869b48ce407e7abad24 | 2025-11-18 18:24:24.63386  | ADDED
(31 rows)

-- Test Run 3 — Type, Timing, Event, Level Changes
-- MODIFY event + timing
DROP TRIGGER trg_check_salary1 ON analytics_schema.employees;

CREATE TRIGGER trg_check_salary1
AFTER INSERT OR UPDATE ON analytics_schema.employees   -- CHANGED BEFORE → AFTER and removed update (event changed)
FOR EACH ROW                                    
EXECUTE FUNCTION analytics_schema.fn_check_salary();

-- MODIFY function body again
CREATE OR REPLACE FUNCTION analytics_schema.fn_employee_delete_cleanup()
RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Cleanup for employee %, deleted at %', OLD.employee_id, now();  -- modified message
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- purpose: to check detection of changes in trigger event and timing, and function body changes
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

select    metadata_id,    snapshot_id,    object_type_name,    object_subtype,    object_subtype_name,  object_md5,    processed_time,  change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype |     object_subtype_name      |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+----------------+------------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | Column         | department_id                | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-18 18:14:29.872481 | ADDED
           2 |           1 | departments      | Column         | department_name              | 5a841b9bbc928694255504765a33a956 | 2025-11-18 18:14:29.875372 | ADDED
           3 |           1 | departments      | Column         | main_location                | d6e08099dee6077445cfbdf123772ea9 | 2025-11-18 18:14:29.875383 | ADDED
           4 |           1 | departments      | Column         | ternary_location             | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-18 18:14:29.87539  | ADDED
           5 |           1 | departments      | Column         | manager_id                   | 840f65e90f5ad043ed195535e39dc868 | 2025-11-18 18:14:29.875397 | ADDED
           6 |           1 | departments      | Column         | budget_code                  | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-18 18:14:29.875404 | ADDED
           7 |           1 | employees        | Column         | employee_id                  | 6b1b0723fc761b4f80b3ebd7347a2adc | 2025-11-18 18:14:29.875409 | ADDED
           8 |           1 | employees        | Column         | first_name                   | 5a841b9bbc928694255504765a33a956 | 2025-11-18 18:14:29.875414 | ADDED
           9 |           1 | employees        | Column         | last_name                    | d6e08099dee6077445cfbdf123772ea9 | 2025-11-18 18:14:29.87542  | ADDED
          10 |           1 | employees        | Column         | email                        | dbd03119c5e663a65d2215ea7cce415c | 2025-11-18 18:14:29.875426 | ADDED
          11 |           1 | employees        | Column         | phone_number                 | 8556e666adc57bad9ca0a20dd335cf50 | 2025-11-18 18:14:29.875432 | ADDED
          12 |           1 | employees        | Column         | hire_date                    | 2802a1f37505eb5f6c985529b7f852ed | 2025-11-18 18:14:29.875437 | ADDED
          13 |           1 | employees        | Column         | salary                       | 400aef400f3cb973b96caa66c27d549d | 2025-11-18 18:14:29.875441 | ADDED
          14 |           1 | employees        | Column         | department_id                | 7f736d83e67a0fad0f24d0fcab4566b3 | 2025-11-18 18:14:29.875448 | ADDED
          15 |           1 | departments      | Constraint     | departments_pkey             | 6f00aac61324af405b95307a19340808 | 2025-11-18 18:14:29.877437 | ADDED
          16 |           1 | employees        | Constraint     | employees_department_id_fkey | d15c5860c9bccaa7101d3622732d6aa7 | 2025-11-18 18:14:29.877451 | ADDED
          17 |           1 | employees        | Constraint     | employees_email_key          | 7366006ed8864803a9082211343f7a9d | 2025-11-18 18:14:29.877456 | ADDED
          18 |           1 | employees        | Constraint     | employees_pkey               | 46e4d7c138cb7c7937bfd192b807b708 | 2025-11-18 18:14:29.877461 | ADDED
          19 |           1 | departments      | Index          | departments_pkey             | b42e577e3d023d664d051edb062f8b89 | 2025-11-18 18:14:29.880772 | ADDED
          20 |           1 | employees        | Index          | employees_email_key          | 46afff75141550028b030f851dff4823 | 2025-11-18 18:14:29.880805 | ADDED
          21 |           1 | employees        | Index          | employees_pkey               | 2ead719c29bb313dcd2fb7888d34134b | 2025-11-18 18:14:29.880811 | ADDED
          22 |           1 | employees        | Reference      | employees_department_id_fkey | c46dee26800ff09f71c42b9e2c940139 | 2025-11-18 18:14:29.886075 | ADDED
          23 |           1 | departments      | Trigger        | trg_department_update_audit  | 3d8c39c09f64c264ca8b223ec8fb7773 | 2025-11-18 18:14:29.88838  | ADDED
          24 |           1 | employees        | Trigger        | trg_check_salary             | c4534188d54ab4ac1103ec1088d4e890 | 2025-11-18 18:14:29.890952 | ADDED
          25 |           1 | employees        | Trigger        | trg_employee_delete_cleanup  | a864b553fcaa0a3edc24e6cc9d57b1a8 | 2025-11-18 18:14:29.890959 | ADDED
          26 |           1 | employees        | Trigger        | trg_employee_insert_audit    | 82e54f0c8b98900e9ad56097a80ebd4c | 2025-11-18 18:14:29.890964 | ADDED
          27 |           1 | employees        | Trigger        | trg_employees_stmt_audit     | ad5a4c46f2169d47bc7aa23759ed76dc | 2025-11-18 18:14:29.890969 | ADDED
      
          28 |           2 | employees        | Trigger        | trg_check_salary             | e5fdfaa3559c42448fa2ad8f8705bb58 | 2025-11-18 18:16:35.328715 | MODIFIED
          29 |           2 | departments      | Trigger        | trg_department_insert_audit  | c2b7bfc45e5cb485cc9c7a23e6b6b4a1 | 2025-11-18 18:16:35.338004 | ADDED
      
          30 |           3 | employees        | Trigger        | trg_emp_insert_audit_v2      | 82e54f0c8b98900e9ad56097a80ebd4c | 2025-11-18 18:24:24.63362  | RENAMED
          31 |           3 | employees        | Trigger        | trg_employee_update_audit    | 8f62a22248034869b48ce407e7abad24 | 2025-11-18 18:24:24.63386  | ADDED
      
          32 |           4 | employees        | Trigger        | trg_employee_delete_cleanup  | 50020d043338f7c49bd2b2d3fc53f300 | 2025-11-18 18:35:04.018709 | MODIFIED
     
          33 |           5 | employees        | Trigger        | trg_check_salary1            | e5fdfaa3559c42448fa2ad8f8705bb58 | 2025-11-18 18:42:18.597521 | RENAMED
          34 |           5 | employees        | Trigger        | trg_employee_delete_cleanup  | 22d39428776740d3f8c808d98d2ca54f | 2025-11-18 18:42:18.597764 | MODIFIED

-- Test Run 4 — Dropping and Adding Back
-- DROP trigger
DROP TRIGGER trg_department_update_audit
ON analytics_schema.departments;

-- ADD with different definition → ADDED (new MD5)
CREATE TRIGGER trg_department_update_audit
BEFORE UPDATE ON analytics_schema.departments     -- previously AFTER
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_department_update_audit();

-- purpose: to check detection of dropped and re-added triggers
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);






-- Test Run 5 — Final Mixed Changes

ALTER TRIGGER trg_emp_delete_log
ON analytics_schema.employees
RENAME TO trg_emp_delete_log_v2;

--
-- MODIFIED TEST
CREATE OR REPLACE FUNCTION analytics_schema.fn_employee_insert_audit()
RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Employee inserted %, updated logic', now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--
-- ADDED TEST
CREATE TRIGGER trg_employee_salary_check_extra
AFTER UPDATE ON analytics_schema.employees
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_check_salary();

--
-- DELETED TEST
DROP TRIGGER trg_department_update_audit ON analytics_schema.departments;


-- purpose: to check detection of renames, function body changes, status changes, and statement vs row level changes
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

-- select    metadata_id,    snapshot_id,    object_type_name,    object_subtype,    object_subtype_name,  object_md5,    processed_time,  change_type FROM pdcd_schema.md5_metadata_tbl;
-- select    metadata_id,    snapshot_id,    object_type_name,    object_subtype,    object_subtype_name,  object_md5,    processed_time FROM pdcd_schema.md5_metadata_staging_tbl;
       
          64 |          28 | employees        | Trigger        | trg_emp_delete_cleanup_v2       | 545ee212b13e73f7ba86a5dd9b6787c0 | 2025-11-18 21:27:48.455702 | ADDED
          65 |          28 | employees        | Trigger        | trg_employee_delete_cleanup     | a17825df8229e3f79b443fcb06b0b5b9 | 2025-11-18 21:27:48.455763 | DELETED
          66 |          29 | employees        | Trigger        | trg_employees_stmt_audit_v2     | 32d3a6019129deb690613de98693ae3f | 2025-11-18 21:32:51.652125 | RENAMED
          67 |          29 | departments      | Trigger        | trg_department_update_audit     | 57cbfddaf8caebc633bbd8a1b16c1894 | 2025-11-18 21:32:51.652235 | MODIFIED
          68 |          29 | employees        | Trigger        | trg_emp_delete_cleanup_v2       | 834693087fbf7123f9f9b38ef0a4963b | 2025-11-18 21:32:51.652261 | MODIFIED
          69 |          29 | employees        | Trigger        | trg_check_salary                | 3277b9a0b364872702a264a7ffee44fc | 2025-11-18 21:32:51.65234  | ADDED
          70 |          29 | employees        | Trigger        | trg_emp_delete_log              | de878fcea4573d5bea0ba036a8a5630f | 2025-11-18 21:32:51.652349 | ADDED
          71 |          30 | employees        | Trigger        | trg_emp_delete_log_v2           | de878fcea4573d5bea0ba036a8a5630f | 2025-11-18 21:36:37.705546 | RENAMED
          72 |          30 | employees        | Trigger        | trg_emp_insert_audit_v2         | eeb6f223a64ad26277c83981039136f3 | 2025-11-18 21:36:37.705662 | MODIFIED
          73 |          30 | employees        | Trigger        | trg_employee_update_audit       | e32b21f366b1db7b925fca1105eae893 | 2025-11-18 21:36:37.705674 | MODIFIED
          74 |          30 | employees        | Trigger        | trg_employee_salary_check_extra | 4b9ccd697eb9a5214f3adc899d43afbd | 2025-11-18 21:36:37.705984 | ADDED
          75 |          30 | departments      | Trigger        | trg_department_update_audit     | 57cbfddaf8caebc633bbd8a1b16c1894 | 2025-11-18 21:36:37.706059 | DELETED
(75 rows)
