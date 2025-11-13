TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;

DROP table IF EXISTS analytics_schema.departments;
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL UNIQUE,
    location VARCHAR(100),
    primary_location VARCHAR(100),
    secondary_location VARCHAR(100),
    ternary_location VARCHAR(100)
);

select 
    snapshot_id,
    object_type_name,
    object_subtype, 
    object_subtype_name,
    object_md5, 
    processed_time, 
    change_type
FROM pdcd_schema.md5_metadata_tbl;

-- First Run
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);

 snapshot_id | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+------------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 | departments      | Column         | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-12 14:30:53.787822 | ADDED
           1 | departments      | Column         | department_name     | f39e54388d58fa7378803e136d7de611 | 2025-11-12 14:30:53.792559 | ADDED
           1 | departments      | Column         | location            | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:30:53.792578 | ADDED
           1 | departments      | Column         | primary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-12 14:30:53.792584 | ADDED
           1 | departments      | Column         | secondary_location  | 57ee8681eea9926a037ef391dc976a13 | 2025-11-12 14:30:53.792589 | ADDED
           1 | departments      | Column         | ternary_location    | 61b2e3cf4714175ccdf73ef3e1838d22 | 2025-11-12 14:30:53.792593 | ADDED


--! CHANGES: Renaming, Dropping, Adding columns  !--
    -- before_changes
    -- department_id | department_name | location | primary_location | secondary_location | ternary_location
    
    -- renaming column
    alter table analytics_schema.departments rename column location to location_old;
    --dropping column
    alter table analytics_schema.departments drop column ternary_location;
    --adding new column
    alter table analytics_schema.departments add column location_new TEXT;
    -- after_changes
    -- department_id | department_name | location_old | primary_location | secondary_location | location_new

-- Second Run and subsequent runs to compare 
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
    select 
    snapshot_id,
    object_type_name,
    object_subtype, 
    object_subtype_name,
    object_md5, 
    processed_time, 
    change_type
FROM pdcd_schema.md5_metadata_tbl;

 snapshot_id | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+------------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 | departments      | Column         | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-12 14:30:53.787822 | ADDED
           1 | departments      | Column         | department_name     | f39e54388d58fa7378803e136d7de611 | 2025-11-12 14:30:53.792559 | ADDED
           1 | departments      | Column         | location            | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:30:53.792578 | ADDED
           1 | departments      | Column         | primary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-12 14:30:53.792584 | ADDED
           1 | departments      | Column         | secondary_location  | 57ee8681eea9926a037ef391dc976a13 | 2025-11-12 14:30:53.792589 | ADDED
           1 | departments      | Column         | ternary_location    | 61b2e3cf4714175ccdf73ef3e1838d22 | 2025-11-12 14:30:53.792593 | ADDED
           2 | departments      | Column         | location_old        | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:31:39.582991 | RENAMED
           2 | departments      | Column         | location_new        | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 14:31:39.583106 | ADDED
           2 | departments      | Column         | ternary_location    | 61b2e3cf4714175ccdf73ef3e1838d22 | 2025-11-12 14:31:39.583135 | DELETED

--! Changes: Modifying column !--
    -- before_changes
    -- department_id | department_name | location_old | primary_location | secondary_location | location_new
    
    -- modifying column
    alter table analytics_schema.departments alter column department_name type VARCHAR(150);
    -- renaming column
    alter table analytics_schema.departments rename column location_new to ternary_location;
    -- adding new column
    alter table analytics_schema.departments add column manager_id TEXT;
    -- dropping column
    alter table analytics_schema.departments drop column location_old;
    -- after_changes
    -- department_id | department_name | primary_location | secondary_location | ternary_location | manager_id
    

-- Third Run and subsequent runs to compare
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);
select 
    snapshot_id,    object_type_name,    object_subtype,  object_subtype_name,    object_md5,     processed_time,     change_type
FROM pdcd_schema.md5_metadata_tbl;

 snapshot_id | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+------------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 | departments      | Column         | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-12 14:30:53.787822 | ADDED
           1 | departments      | Column         | department_name     | f39e54388d58fa7378803e136d7de611 | 2025-11-12 14:30:53.792559 | ADDED
           1 | departments      | Column         | location            | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:30:53.792578 | ADDED
           1 | departments      | Column         | primary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-12 14:30:53.792584 | ADDED
           1 | departments      | Column         | secondary_location  | 57ee8681eea9926a037ef391dc976a13 | 2025-11-12 14:30:53.792589 | ADDED
           1 | departments      | Column         | ternary_location    | 61b2e3cf4714175ccdf73ef3e1838d22 | 2025-11-12 14:30:53.792593 | ADDED
           2 | departments      | Column         | location_old        | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:31:39.582991 | RENAMED
           2 | departments      | Column         | location_new        | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 14:31:39.583106 | ADDED
           2 | departments      | Column         | ternary_location    | 61b2e3cf4714175ccdf73ef3e1838d22 | 2025-11-12 14:31:39.583135 | DELETED
           3 | departments      | Column         | ternary_location    | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 14:33:02.537206 | RENAMED
           3 | departments      | Column         | department_name     | f1e326b4a40aaa809aebffb844408a30 | 2025-11-12 14:33:02.537669 | MODIFIED
           3 | departments      | Column         | manager_id          | 310ec655a81af3aab542966c2f729edb | 2025-11-12 14:33:02.537751 | ADDED
           3 | departments      | Column         | location_old        | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:33:02.537991 | DELETED

--! Fourth, Changes: !--
    -- before_changes
    -- department_id | department_name | primary_location | secondary_location | ternary_location | manager_id

    -- renaming column
    alter table analytics_schema.departments rename column primary_location to main_location;
    -- dropping column
    alter table analytics_schema.departments drop column secondary_location;
    -- adding new column
    alter table analytics_schema.departments add column budget_code TEXT;
    -- modifying column
    alter table analytics_schema.departments alter column manager_id type VARCHAR(50);
    
    -- after_changes
    --  department_id | department_name | main_location | ternary_location | manager_id | budget_code

-- Fourth Run and subsequent runs to compare
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['analytics_schema']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']); 

select 
    snapshot_id,    object_type_name,    object_subtype,  object_subtype_name,    object_md5,     processed_time,     change_type
FROM pdcd_schema.md5_metadata_tbl;
 snapshot_id | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+------------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 | departments      | Column         | department_id       | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-12 14:30:53.787822 | ADDED
           1 | departments      | Column         | department_name     | f39e54388d58fa7378803e136d7de611 | 2025-11-12 14:30:53.792559 | ADDED
           1 | departments      | Column         | location            | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:30:53.792578 | ADDED
           1 | departments      | Column         | primary_location    | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-12 14:30:53.792584 | ADDED
           1 | departments      | Column         | secondary_location  | 57ee8681eea9926a037ef391dc976a13 | 2025-11-12 14:30:53.792589 | ADDED
           1 | departments      | Column         | ternary_location    | 61b2e3cf4714175ccdf73ef3e1838d22 | 2025-11-12 14:30:53.792593 | ADDED
-- changes from second run
           2 | departments      | Column         | location_old        | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:31:39.582991 | RENAMED
           2 | departments      | Column         | location_new        | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 14:31:39.583106 | ADDED
           2 | departments      | Column         | ternary_location    | 61b2e3cf4714175ccdf73ef3e1838d22 | 2025-11-12 14:31:39.583135 | DELETED
-- changes from third run
           3 | departments      | Column         | ternary_location    | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 14:33:02.537206 | RENAMED
           3 | departments      | Column         | department_name     | f1e326b4a40aaa809aebffb844408a30 | 2025-11-12 14:33:02.537669 | MODIFIED
           3 | departments      | Column         | manager_id          | 310ec655a81af3aab542966c2f729edb | 2025-11-12 14:33:02.537751 | ADDED
           3 | departments      | Column         | location_old        | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:33:02.537991 | DELETED
-- changes from fourth run
           4 | departments      | Column         | main_location       | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-12 14:35:18.089771 | RENAMED
           4 | departments      | Column         | manager_id          | cf2f1a32c8477070db95384f50c2d0e7 | 2025-11-12 14:35:18.095809 | MODIFIED
           4 | departments      | Column         | budget_code         | a110ecc03220cb8113204e96e1f9cfba | 2025-11-12 14:35:18.095896 | ADDED
           4 | departments      | Column         | secondary_location  | 57ee8681eea9926a037ef391dc976a13 | 2025-11-12 14:35:18.095942 | DELETED

-- END:
 metadata_id | snapshot_id |   schema_name    | object_type | object_type_name | object_subtype | object_subtype_name |                                                                                                                            object_subtype_details                                                                                                                             |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+-------------+------------------+----------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | analytics_schema | Table       | departments      | Column         | department_id       | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1 | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-12 14:30:53.787822 | ADDED
           2 |           1 | analytics_schema | Table       | departments      | Column         | department_name     | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_department_name_key,ordinal_position:2                                           | f39e54388d58fa7378803e136d7de611 | 2025-11-12 14:30:53.792559 | ADDED
           3 |           1 | analytics_schema | Table       | departments      | Column         | location            | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3                                                                         | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:30:53.792578 | ADDED
           4 |           1 | analytics_schema | Table       | departments      | Column         | primary_location    | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4                                                                         | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-12 14:30:53.792584 | ADDED
           5 |           1 | analytics_schema | Table       | departments      | Column         | secondary_location  | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5                                                                         | 57ee8681eea9926a037ef391dc976a13 | 2025-11-12 14:30:53.792589 | ADDED
           6 |           1 | analytics_schema | Table       | departments      | Column         | ternary_location    | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6                                                                         | 61b2e3cf4714175ccdf73ef3e1838d22 | 2025-11-12 14:30:53.792593 | ADDED
           7 |           2 | analytics_schema | Table       | departments      | Column         | location_old        | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3                                                                         | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:31:39.582991 | RENAMED
           8 |           2 | analytics_schema | Table       | departments      | Column         | location_new        | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7                                                                                         | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 14:31:39.583106 | ADDED
           9 |           2 | analytics_schema | Table       | departments      | Column         | ternary_location    | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:6                                                                         | 61b2e3cf4714175ccdf73ef3e1838d22 | 2025-11-12 14:31:39.583135 | DELETED
          10 |           3 | analytics_schema | Table       | departments      | Column         | ternary_location    | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:7                                                                                         | 194c7795dfa2987255dfc248e379b863 | 2025-11-12 14:33:02.537206 | RENAMED
          11 |           3 | analytics_schema | Table       | departments      | Column         | department_name     | data_type:character varying,max_length:150,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_department_name_key,ordinal_position:2                                           | f1e326b4a40aaa809aebffb844408a30 | 2025-11-12 14:33:02.537669 | MODIFIED
          12 |           3 | analytics_schema | Table       | departments      | Column         | manager_id          | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:8                                                                                         | 310ec655a81af3aab542966c2f729edb | 2025-11-12 14:33:02.537751 | ADDED
          13 |           3 | analytics_schema | Table       | departments      | Column         | location_old        | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:3                                                                         | d6e08099dee6077445cfbdf123772ea9 | 2025-11-12 14:33:02.537991 | DELETED
          14 |           4 | analytics_schema | Table       | departments      | Column         | main_location       | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:4                                                                         | a4e83f1f87ade648d39ec2e7fbdce011 | 2025-11-12 14:35:18.089771 | RENAMED
          15 |           4 | analytics_schema | Table       | departments      | Column         | manager_id          | data_type:character varying,max_length:50,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:8                                                                          | cf2f1a32c8477070db95384f50c2d0e7 | 2025-11-12 14:35:18.095809 | MODIFIED
          16 |           4 | analytics_schema | Table       | departments      | Column         | budget_code         | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9                                                                                         | a110ecc03220cb8113204e96e1f9cfba | 2025-11-12 14:35:18.095896 | ADDED
          17 |           4 | analytics_schema | Table       | departments      | Column         | secondary_location  | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:5




SELECT object_subtype_name, change_type, processed_time
FROM pdcd_schema.md5_metadata_tbl
WHERE object_subtype_name IN (
    SELECT object_subtype_name FROM pdcd_schema.md5_metadata_tbl WHERE change_type = 'DELETED'
)
ORDER BY object_subtype_name, processed_time;          