drop function pdcd_schema.load_snapshot_tbl(TEXT[]);
drop function pdcd_schema.load_md5_metadata_tbl(TEXT[]);
drop function pdcd_schema.load_md5_metadata_staging_tbl(TEXT[]);

drop table pdcd_schema.snapshot_tbl;
drop table pdcd_schema.md5_metadata_tbl;
drop table pdcd_schema.md5_metadata_staging_tbl;


TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;

DROP table IF EXISTS analytics_schema.departments;
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    main_location VARCHAR(100),
    ternary_location VARCHAR(100),
    manager_id INT,
    budget_code VARCHAR(50)
);

-- First Run
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
select    metadata_id,    snapshot_id,    object_type_name,    object_subtype_name,    object_md5,    processed_time,    change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-13 17:38:41.254066 | ADDED
           2 |           1 | departments      | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-13 17:38:41.260605 | ADDED
           3 |           1 | departments      | main_location       | d6e08099dee6077445cfbdf123772ea9 | 2025-11-13 17:38:41.260635 | ADDED
           4 |           1 | departments      | ternary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-13 17:38:41.260644 | ADDED
           5 |           1 | departments      | manager_id          | 840f65e90f5ad043ed195535e39dc868 | 2025-11-13 17:38:41.260654 | ADDED
           6 |           1 | departments      | budget_code         | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-13 17:38:41.260662 | ADDED

-- Test Run 1 — Initial Additions & Modifications
ALTER TABLE analytics_schema.departments ADD COLUMN region TEXT;
ALTER TABLE analytics_schema.departments ADD COLUMN established_year INTEGER;
ALTER TABLE analytics_schema.departments ALTER COLUMN department_name TYPE VARCHAR(150);
ALTER TABLE analytics_schema.departments ALTER COLUMN main_location SET DEFAULT 'Head Office';
--  Purpose: initial schema growth + default and data type changes.
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
select    metadata_id,    snapshot_id,    object_type_name,    object_subtype_name,    object_md5,    processed_time,    change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-13 17:38:41.254066 | ADDED
           2 |           1 | departments      | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-13 17:38:41.260605 | ADDED
           3 |           1 | departments      | main_location       | d6e08099dee6077445cfbdf123772ea9 | 2025-11-13 17:38:41.260635 | ADDED
           4 |           1 | departments      | ternary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-13 17:38:41.260644 | ADDED
           5 |           1 | departments      | manager_id          | 840f65e90f5ad043ed195535e39dc868 | 2025-11-13 17:38:41.260654 | ADDED
           6 |           1 | departments      | budget_code         | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-13 17:38:41.260662 | ADDED

           7 |           2 | departments      | department_name     | 2b9f21bc6704c84937a2dd0ca518c14c | 2025-11-13 17:39:09.719905 | MODIFIED
           8 |           2 | departments      | main_location       | 3af8bad4fa96a3a485e23550581e6d8b | 2025-11-13 17:39:09.719967 | MODIFIED
           9 |           2 | departments      | region              | 194c7795dfa2987255dfc248e379b863 | 2025-11-13 17:39:09.720018 | ADDED
          10 |           2 | departments      | established_year    | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-13 17:39:09.720028 | ADDED

-- Test Run 2 — Renaming and Adding
ALTER TABLE analytics_schema.departments RENAME COLUMN region TO department_region;
ALTER TABLE analytics_schema.departments RENAME COLUMN established_year TO founded_year;
ALTER TABLE analytics_schema.departments ADD COLUMN last_updated_by TEXT;
ALTER TABLE analytics_schema.departments ADD COLUMN budget_allocated NUMERIC(12,2);
--  Purpose: check detection of renames and new column additions in same run.
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
select    metadata_id,    snapshot_id,    object_type_name,    object_subtype_name,    object_md5,    processed_time,    change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-13 17:38:41.254066 | ADDED
           2 |           1 | departments      | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-13 17:38:41.260605 | ADDED
           3 |           1 | departments      | main_location       | d6e08099dee6077445cfbdf123772ea9 | 2025-11-13 17:38:41.260635 | ADDED
           4 |           1 | departments      | ternary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-13 17:38:41.260644 | ADDED
           5 |           1 | departments      | manager_id          | 840f65e90f5ad043ed195535e39dc868 | 2025-11-13 17:38:41.260654 | ADDED
           6 |           1 | departments      | budget_code         | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-13 17:38:41.260662 | ADDED

           7 |           2 | departments      | department_name     | 2b9f21bc6704c84937a2dd0ca518c14c | 2025-11-13 17:39:09.719905 | MODIFIED
           8 |           2 | departments      | main_location       | 3af8bad4fa96a3a485e23550581e6d8b | 2025-11-13 17:39:09.719967 | MODIFIED
           9 |           2 | departments      | region              | 194c7795dfa2987255dfc248e379b863 | 2025-11-13 17:39:09.720018 | ADDED
          10 |           2 | departments      | established_year    | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-13 17:39:09.720028 | ADDED

          11 |           3 | departments      | department_region   | 194c7795dfa2987255dfc248e379b863 | 2025-11-13 17:40:31.677879 | RENAMED
          12 |           3 | departments      | founded_year        | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-13 17:40:31.677933 | RENAMED
          13 |           3 | departments      | budget_allocated    | 9fcafc65da07baa20e36cbd0d8d92c72 | 2025-11-13 17:40:31.678017 | ADDED
          14 |           3 | departments      | last_updated_by     | a110ecc03220cb8113204e96e1f9cfba | 2025-11-13 17:40:31.678023 | ADDED

--! Test Run 3 — Type, Default, and Rename Updates !-- SPecial CASE
ALTER TABLE analytics_schema.departments ALTER COLUMN budget_allocated TYPE FLOAT;
ALTER TABLE analytics_schema.departments ALTER COLUMN founded_year SET DEFAULT EXTRACT(YEAR FROM NOW());
ALTER TABLE analytics_schema.departments RENAME COLUMN last_updated_by TO updated_by;
ALTER TABLE analytics_schema.departments ALTER COLUMN updated_by SET DEFAULT CURRENT_USER;
--  Purpose: test multiple attribute updates (type, default, rename).
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
select    metadata_id,    snapshot_id,    object_type_name,    object_subtype_name,    object_md5,    processed_time,    change_type FROM pdcd_schema.md5_metadata_tbl;
     metadata_id | snapshot_id | object_type_name | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-13 17:38:41.254066 | ADDED
           2 |           1 | departments      | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-13 17:38:41.260605 | ADDED
           3 |           1 | departments      | main_location       | d6e08099dee6077445cfbdf123772ea9 | 2025-11-13 17:38:41.260635 | ADDED
           4 |           1 | departments      | ternary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-13 17:38:41.260644 | ADDED
           5 |           1 | departments      | manager_id          | 840f65e90f5ad043ed195535e39dc868 | 2025-11-13 17:38:41.260654 | ADDED
           6 |           1 | departments      | budget_code         | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-13 17:38:41.260662 | ADDED

           7 |           2 | departments      | department_name     | 2b9f21bc6704c84937a2dd0ca518c14c | 2025-11-13 17:39:09.719905 | MODIFIED
           8 |           2 | departments      | main_location       | 3af8bad4fa96a3a485e23550581e6d8b | 2025-11-13 17:39:09.719967 | MODIFIED
           9 |           2 | departments      | region              | 194c7795dfa2987255dfc248e379b863 | 2025-11-13 17:39:09.720018 | ADDED
          10 |           2 | departments      | established_year    | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-13 17:39:09.720028 | ADDED

          11 |           3 | departments      | department_region   | 194c7795dfa2987255dfc248e379b863 | 2025-11-13 17:40:31.677879 | RENAMED
          12 |           3 | departments      | founded_year        | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-13 17:40:31.677933 | RENAMED
          13 |           3 | departments      | budget_allocated    | 9fcafc65da07baa20e36cbd0d8d92c72 | 2025-11-13 17:40:31.678017 | ADDED
          14 |           3 | departments      | last_updated_by     | a110ecc03220cb8113204e96e1f9cfba | 2025-11-13 17:40:31.678023 | ADDED

          15 |           4 | departments      | founded_year        | fe2a8229c2f4a36a65c346c2a8ec4e92 | 2025-11-13 17:42:27.929675 | MODIFIED
          16 |           4 | departments      | budget_allocated    | 4303381b7b875f4b5f782798e2564a36 | 2025-11-13 17:42:27.929738 | MODIFIED
          17 |           4 | departments      | updated_by          | 88965135e4291d50cd045e493557bb97 | 2025-11-13 17:42:27.9298   | ADDED
          18 |           4 | departments      | last_updated_by     | a110ecc03220cb8113204e96e1f9cfba | 2025-11-13 17:42:27.929865 | DELETED

-- Test Run 4 — Dropping and Adding Back
ALTER TABLE analytics_schema.departments DROP COLUMN updated_by;
ALTER TABLE analytics_schema.departments DROP COLUMN budget_allocated;
ALTER TABLE analytics_schema.departments ADD COLUMN total_employees INTEGER;
ALTER TABLE analytics_schema.departments ADD COLUMN active_status BOOLEAN DEFAULT TRUE;
--  Purpose: test column removal + re-creation patterns — very common in evolution.
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
select    metadata_id,    snapshot_id,    object_type_name,    object_subtype_name,    object_md5,    processed_time,    change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-13 17:38:41.254066 | ADDED
           2 |           1 | departments      | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-13 17:38:41.260605 | ADDED
           3 |           1 | departments      | main_location       | d6e08099dee6077445cfbdf123772ea9 | 2025-11-13 17:38:41.260635 | ADDED
           4 |           1 | departments      | ternary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-13 17:38:41.260644 | ADDED
           5 |           1 | departments      | manager_id          | 840f65e90f5ad043ed195535e39dc868 | 2025-11-13 17:38:41.260654 | ADDED
           6 |           1 | departments      | budget_code         | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-13 17:38:41.260662 | ADDED
           7 |           2 | departments      | department_name     | 2b9f21bc6704c84937a2dd0ca518c14c | 2025-11-13 17:39:09.719905 | MODIFIED
           8 |           2 | departments      | main_location       | 3af8bad4fa96a3a485e23550581e6d8b | 2025-11-13 17:39:09.719967 | MODIFIED
           9 |           2 | departments      | region              | 194c7795dfa2987255dfc248e379b863 | 2025-11-13 17:39:09.720018 | ADDED
          10 |           2 | departments      | established_year    | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-13 17:39:09.720028 | ADDED
          11 |           3 | departments      | department_region   | 194c7795dfa2987255dfc248e379b863 | 2025-11-13 17:40:31.677879 | RENAMED
          12 |           3 | departments      | founded_year        | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-13 17:40:31.677933 | RENAMED
          13 |           3 | departments      | budget_allocated    | 9fcafc65da07baa20e36cbd0d8d92c72 | 2025-11-13 17:40:31.678017 | ADDED
          14 |           3 | departments      | last_updated_by     | a110ecc03220cb8113204e96e1f9cfba | 2025-11-13 17:40:31.678023 | ADDED
          15 |           4 | departments      | founded_year        | fe2a8229c2f4a36a65c346c2a8ec4e92 | 2025-11-13 17:42:27.929675 | MODIFIED
          16 |           4 | departments      | budget_allocated    | 4303381b7b875f4b5f782798e2564a36 | 2025-11-13 17:42:27.929738 | MODIFIED
          17 |           4 | departments      | updated_by          | 88965135e4291d50cd045e493557bb97 | 2025-11-13 17:42:27.9298   | ADDED
          18 |           4 | departments      | last_updated_by     | a110ecc03220cb8113204e96e1f9cfba | 2025-11-13 17:42:27.929865 | DELETED

          19 |           5 | departments      | total_employees     | b07395d20d52dbcb38f516077a00500d | 2025-11-13 17:45:58.09453  | ADDED
          20 |           5 | departments      | active_status       | 32725f541bfbd0de9ea4ac4817444b96 | 2025-11-13 17:45:58.094589 | ADDED
          21 |           5 | departments      | updated_by          | 88965135e4291d50cd045e493557bb97 | 2025-11-13 17:45:58.09463  | DELETED
          22 |           5 | departments      | budget_allocated    | 4303381b7b875f4b5f782798e2564a36 | 2025-11-13 17:45:58.094639 | DELETED

-- Test Run 5 — Final Mixed Changes
ALTER TABLE analytics_schema.departments RENAME COLUMN total_employees TO headcount;
ALTER TABLE analytics_schema.departments ALTER COLUMN headcount TYPE BIGINT;
ALTER TABLE analytics_schema.departments DROP COLUMN active_status;
ALTER TABLE analytics_schema.departments ADD COLUMN remarks TEXT;
--  Purpose: simulate rename + modify + drop + add together — final full-cycle test.
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_column_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
select    metadata_id,    snapshot_id,    object_type_name,    object_subtype_name,    object_md5,    processed_time,    change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | object_type_name | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | departments      | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-13 17:38:41.254066 | ADDED
           2 |           1 | departments      | department_name     | 5a841b9bbc928694255504765a33a956 | 2025-11-13 17:38:41.260605 | ADDED
           3 |           1 | departments      | main_location       | d6e08099dee6077445cfbdf123772ea9 | 2025-11-13 17:38:41.260635 | ADDED
           4 |           1 | departments      | ternary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-13 17:38:41.260644 | ADDED
           5 |           1 | departments      | manager_id          | 840f65e90f5ad043ed195535e39dc868 | 2025-11-13 17:38:41.260654 | ADDED
           6 |           1 | departments      | budget_code         | 4c9087b4da4db15878fc5dffc5f1482a | 2025-11-13 17:38:41.260662 | ADDED
           7 |           2 | departments      | department_name     | 2b9f21bc6704c84937a2dd0ca518c14c | 2025-11-13 17:39:09.719905 | MODIFIED
           8 |           2 | departments      | main_location       | 3af8bad4fa96a3a485e23550581e6d8b | 2025-11-13 17:39:09.719967 | MODIFIED
           9 |           2 | departments      | region              | 194c7795dfa2987255dfc248e379b863 | 2025-11-13 17:39:09.720018 | ADDED
          10 |           2 | departments      | established_year    | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-13 17:39:09.720028 | ADDED
          11 |           3 | departments      | department_region   | 194c7795dfa2987255dfc248e379b863 | 2025-11-13 17:40:31.677879 | RENAMED
          12 |           3 | departments      | founded_year        | db7ef0dbfd52d44d7df67906ae719852 | 2025-11-13 17:40:31.677933 | RENAMED
          13 |           3 | departments      | budget_allocated    | 9fcafc65da07baa20e36cbd0d8d92c72 | 2025-11-13 17:40:31.678017 | ADDED
          14 |           3 | departments      | last_updated_by     | a110ecc03220cb8113204e96e1f9cfba | 2025-11-13 17:40:31.678023 | ADDED
          15 |           4 | departments      | founded_year        | fe2a8229c2f4a36a65c346c2a8ec4e92 | 2025-11-13 17:42:27.929675 | MODIFIED
          16 |           4 | departments      | budget_allocated    | 4303381b7b875f4b5f782798e2564a36 | 2025-11-13 17:42:27.929738 | MODIFIED
          17 |           4 | departments      | updated_by          | 88965135e4291d50cd045e493557bb97 | 2025-11-13 17:42:27.9298   | ADDED
          18 |           4 | departments      | last_updated_by     | a110ecc03220cb8113204e96e1f9cfba | 2025-11-13 17:42:27.929865 | DELETED
          19 |           5 | departments      | total_employees     | b07395d20d52dbcb38f516077a00500d | 2025-11-13 17:45:58.09453  | ADDED
          20 |           5 | departments      | active_status       | 32725f541bfbd0de9ea4ac4817444b96 | 2025-11-13 17:45:58.094589 | ADDED
          21 |           5 | departments      | updated_by          | 88965135e4291d50cd045e493557bb97 | 2025-11-13 17:45:58.09463  | DELETED
          22 |           5 | departments      | budget_allocated    | 4303381b7b875f4b5f782798e2564a36 | 2025-11-13 17:45:58.094639 | DELETED

          23 |           6 | departments      | remarks             | 3b04b5ccde27a819b92b5f1db6e5a3ee | 2025-11-13 17:47:01.696037 | ADDED
          24 |           6 | departments      | headcount           | ae84db7cd478eaeb1426f3f97e751cba | 2025-11-13 17:47:01.696093 | ADDED
          25 |           6 | departments      | total_employees     | b07395d20d52dbcb38f516077a00500d | 2025-11-13 17:47:01.698073 | DELETED
          26 |           6 | departments      | active_status       | 32725f541bfbd0de9ea4ac4817444b96 | 2025-11-13 17:47:01.698082 | DELETED

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