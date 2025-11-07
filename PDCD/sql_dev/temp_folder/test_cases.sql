
-- WORKING ON Addition of Columns
With current_md5_metadata_cte as (
        SELECT * FROM pdcd_schema.get_table_md5(array['analytics_schema'])
        UNION ALL 
        SELECT * FROM pdcd_schema.get_table_all_columns_md5(array['analytics_schema'])
        UNION ALL 
        SELECT * FROM pdcd_schema.get_table_columns_md5(array['analytics_schema'])
)
select * from current_md5_metadata_cte;

-- pdcd_schema.md5_metadata_tbl
 metadata_id | snapshot_id |   schema_name    | object_type | object_type_name |  object_subtype   |   object_subtype_name    |                                                                                                                            object_subtype_details                                                                                                                             |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+-------------+------------------+-------------------+--------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | analytics_schema | Table       | departments      | All_Table_Objects | columns                  |                                                                                                                                                                                                                                                                               | bc3d6a2fa8aadc8501154b15f8ed5781 | 2025-11-06 13:24:21.68767  | ADDED
           2 |           1 | analytics_schema | Table       | departments      | Columns           | department_id, dept_name |                                                                                                                                                                                                                                                                               | 8bcdd498c4bbb1772f71227d35129222 | 2025-11-06 13:24:21.703847 | ADDED
           3 |           1 | analytics_schema | Table       | departments      | Column            | department_id            | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1 | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-06 13:24:21.714863 | ADDED
           4 |           1 | analytics_schema | Table       | departments      | Column            | dept_name                | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_department_name_key,ordinal_position:2                                           | f39e54388d58fa7378803e136d7de611 | 2025-11-06 13:24:21.714886 | ADDED


-- pdcd_schema.md5_metadata_staging_tbl
 metadata_id | snapshot_id |   schema_name    | object_type | object_type_name |  object_subtype   |   object_subtype_name    |                                                                                                                            object_subtype_details                                                                                                                             |            object_md5            |       processed_time
-------------+-------------+------------------+-------------+------------------+-------------------+--------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           1 | analytics_schema | Table       | departments      | All_Table_Objects | columns                  |                                                                                                                                                                                                                                                                               | bc3d6a2fa8aadc8501154b15f8ed5781 | 2025-11-06 13:33:40.332379
           2 |           1 | analytics_schema | Table       | departments      | Columns           | department_id, dept_name |                                                                                                                                                                                                                                                                               | 8bcdd498c4bbb1772f71227d35129222 | 2025-11-06 13:33:40.350723
           3 |           1 | analytics_schema | Table       | departments      | Column            | department_id            | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1 | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-06 13:33:40.36181
           4 |           1 | analytics_schema | Table       | departments      | Column            | dept_name                | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_department_name_key,ordinal_position:2                                           | f39e54388d58fa7378803e136d7de611 | 2025-11-06 13:33:40.361833

-- Current AFter changes MD5 Metadata

   schema_name    | object_type | object_type_name |  object_subtype   |            object_subtype_name             |                                                                                                                            object_subtype_details                                                                                                                             |            object_md5
------------------+-------------+------------------+-------------------+--------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------
 analytics_schema | Table       | departments      | All_Table_Objects | columns                                    |                                                                                                                                                                                                                                                                               | 05d504c791d99a83d3861c8f57dee241
 analytics_schema | Table       | departments      | Columns           | department_id, dept_name, primary_location |                                                                                                                                                                                                                                                                               | 497e34d64bc4f5f94fa13802391e3d78
 analytics_schema | Table       | departments      | Column            | department_id                              | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1 | 57cdd3e718f6f0349c77a716434d09f8
 analytics_schema | Table       | departments      | Column            | dept_name                                  | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_department_name_key,ordinal_position:2                                           | f39e54388d58fa7378803e136d7de611
 analytics_schema | Table       | departments      | Column            | primary_location                           | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9

-- Assume we added a new column 'primary_location' to departments table
-- Now, we will run the load_changed_md5_metadata_tbl function to capture this change
-- Final pdcd_schema.md5_metadata_tbl

 metadata_id | snapshot_id |   schema_name    | object_type | object_type_name |  object_subtype   |   object_subtype_name    |                                                                                                                            object_subtype_details                                                                                                                             |            object_md5            |       processed_time       | change_type
-------------+-------------+------------------+-------------+------------------+-------------------+--------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | analytics_schema | Table       | departments      | All_Table_Objects | columns                  |                                                                                                                                                                                                                                                                               | bc3d6a2fa8aadc8501154b15f8ed5781 | 2025-11-06 13:24:21.68767  | ADDED
           2 |           1 | analytics_schema | Table       | departments      | Columns           | department_id, dept_name |                                                                                                                                                                                                                                                                               | 8bcdd498c4bbb1772f71227d35129222 | 2025-11-06 13:24:21.703847 | ADDED
           3 |           1 | analytics_schema | Table       | departments      | Column            | department_id            | data_type:integer,max_length:,numeric_precision:32,numeric_scale:0,nullable:NO,default_value:nextval('analytics_schema.departments_department_id_seq'::regclass),is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_pkey,ordinal_position:1 | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-06 13:24:21.714863 | ADDED
           4 |           1 | analytics_schema | Table       | departments      | Column            | dept_name                | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_department_name_key,ordinal_position:2                                           | f39e54388d58fa7378803e136d7de611 | 2025-11-06 13:24:21.714886 | ADDED
           5 |           2 | analytics_schema | Table       | departments      | All_Table_Objects | columns                  |                                                                                                                                                                                                                                                                               | 05d504c791d99a83d3861c8f57dee241 | 2025-11-06 13:38:10.123456 | ADDED
           6 |           2 | analytics_schema | Table       | departments      | Columns           | department_id, dept_name, primary_location |                                                                                                                                                                                                                                                             | 497e34d64bc4f5f94fa13802391e3d78 | 2025-11-06 13:38:10.123456 | ADDED
           7 |           2 | analytics_schema | Table       | departments      | Column            | primary_location         | data_type:text,max_length:,numeric_precision:,numeric_scale:,nullable:YES,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:,ordinal_position:9                                                                                         | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-06 13:38:10.123456 | ADDED



-- WORKING ON Deletion of Columns
   schema_name    | object_type | object_type_name |  object_subtype   |   object_subtype_name    |            object_md5            |          processed_time          | change_type |         prev_object_md5
------------------+-------------+------------------+-------------------+--------------------------+----------------------------------+----------------------------------+-------------+----------------------------------
 analytics_schema | Table       | departments      | All_Table_Objects | columns                  | bc3d6a2fa8aadc8501154b15f8ed5781 | 2025-11-06 16:12:12.540874+05:30 | DELETED     | 05d504c791d99a83d3861c8f57dee241
 analytics_schema | Table       | departments      | Column            | department_id            | 57cdd3e718f6f0349c77a716434d09f8 | 2025-11-06 16:12:12.540878+05:30 | DELETED     | 57cdd3e718f6f0349c77a716434d09f8
 analytics_schema | Table       | departments      | Columns           | department_id, dept_name | 8bcdd498c4bbb1772f71227d35129222 | 2025-11-06 16:12:12.540881+05:30 | DELETED     |
 analytics_schema | Table       | departments      | Column            | dept_name                | f39e54388d58fa7378803e136d7de611 | 2025-11-06 16:12:12.540879+05:30 | DELETED     |

   schema_name    | object_type | object_type_name | object_subtype | object_subtype_name |                                                                                                       object_subtype_details                                                                                                        |            object_md5            |         processed_time          | change_type
------------------+-------------+------------------+----------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+---------------------------------+-------------
 analytics_schema | Table       | departments      | Column         | department_name     | data_type:character varying,max_length:100,numeric_precision:,numeric_scale:,nullable:NO,default_value:,is_identity:NO,is_generated:NEVER,generation_expression:,constraint_name:departments_department_name_key,ordinal_position:2 | f39e54388d58fa7378803e136d7de611 | 2025-11-06 16:16:36.79091+05:30 | MODIFIED

SELECT 
schema_name, object_type,
 object_type_name,
  object_subtype,
  object_subtype_name,
  object_subtype_details
FROM pdcd_schema.md5_metadata_tbl;


WITH current_md5_metadata_cte AS (
    SELECT * FROM pdcd_schema.get_table_columns_md5(ARRAY['analytics_schema'])
),

-- RENAMED: same MD5, different column name
renamed_objects AS (
    SELECT
        c.schema_name,
        c.object_type,
        c.object_type_name,
        c.object_subtype,
        c.object_subtype_name,
        NULL::text AS prev_object_subtype_details,
        NULL::text AS new_object_subtype_details,
        s.object_md5   AS prev_object_md5,
        c.object_md5   AS object_md5,
        clock_timestamp() AS processed_time,
        'RENAMED' AS change_type
    FROM current_md5_metadata_cte c
    JOIN pdcd_schema.md5_metadata_staging_tbl s
      ON  s.schema_name = c.schema_name
      AND s.object_type = c.object_type
      AND s.object_type_name = c.object_type_name
      AND COALESCE(s.object_subtype, '') = COALESCE(c.object_subtype, '')
      AND s.object_md5 = c.object_md5
    WHERE s.object_subtype_name IS DISTINCT FROM c.object_subtype_name
),

-- MODIFIED: same identity (name) but MD5 changed
modified_objects AS (
    SELECT
        c.schema_name,
        c.object_type,
        c.object_type_name,
        c.object_subtype,
        c.object_subtype_name,
        s.object_subtype_details AS prev_object_subtype_details,
        c.object_subtype_details AS new_object_subtype_details,
        s.object_md5 AS prev_object_md5,
        c.object_md5 AS object_md5,
        clock_timestamp() AS processed_time,
        'MODIFIED' AS change_type
    FROM current_md5_metadata_cte c
    JOIN pdcd_schema.md5_metadata_staging_tbl s
      ON  s.schema_name = c.schema_name
      AND s.object_type = c.object_type
      AND s.object_type_name = c.object_type_name
      AND COALESCE(s.object_subtype, '') = COALESCE(c.object_subtype, '')
      AND COALESCE(s.object_subtype_name, '') = COALESCE(c.object_subtype_name, '')
    WHERE s.object_md5 IS DISTINCT FROM c.object_md5
),

-- ADDED: MD5 not present in staging AND not part of renamed/modified (safety)
added_objects AS (
    SELECT
        c.schema_name,
        c.object_type,
        c.object_type_name,
        c.object_subtype,
        c.object_subtype_name,
        NULL::text AS prev_object_subtype_details,
        c.object_subtype_details AS new_object_subtype_details,
        NULL::text AS prev_object_md5,
        c.object_md5 AS object_md5,
        clock_timestamp() AS processed_time,
        'ADDED' AS change_type
    FROM current_md5_metadata_cte c
    LEFT JOIN pdcd_schema.md5_metadata_staging_tbl s
      ON s.object_md5 = c.object_md5
    WHERE s.object_md5 IS NULL
      AND c.object_md5 NOT IN (
          SELECT prev_object_md5 FROM renamed_objects
          UNION
          SELECT prev_object_md5 FROM modified_objects
      )
),

-- DELETED: present in staging but missing in current AND NOT renamed/modified
deleted_objects AS (
    SELECT
        s.schema_name,
        s.object_type,
        s.object_type_name,
        s.object_subtype,
        s.object_subtype_name,
        s.object_subtype_details AS prev_object_subtype_details,
        NULL::text AS new_object_subtype_details,
        s.object_md5 AS prev_object_md5,
        NULL::text AS object_md5,
        clock_timestamp() AS processed_time,
        'DELETED' AS change_type
    FROM pdcd_schema.md5_metadata_staging_tbl s
    LEFT JOIN current_md5_metadata_cte c
      ON  s.schema_name = c.schema_name
      AND s.object_type = c.object_type
      AND s.object_type_name = c.object_type_name
      AND COALESCE(s.object_subtype, '') = COALESCE(c.object_subtype, '')
      AND COALESCE(s.object_subtype_name, '') = COALESCE(c.object_subtype_name, '')
    WHERE c.object_md5 IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM renamed_objects r WHERE r.prev_object_md5 = s.object_md5
      )
      AND NOT EXISTS (
          SELECT 1 FROM modified_objects m WHERE m.prev_object_md5 = s.object_md5
      )
)

-- UNIFIED OUTPUT (same column shape for each CTE)
SELECT
    schema_name,
    object_type,
    object_type_name,
    object_subtype,
    object_subtype_name,
    COALESCE(new_object_subtype_details, prev_object_subtype_details) AS object_subtype_details,
    COALESCE(object_md5, prev_object_md5) AS object_md5,
    processed_time,
    change_type
FROM (
    SELECT * FROM added_objects
    UNION ALL
    SELECT * FROM renamed_objects
    UNION ALL
    SELECT * FROM modified_objects
    UNION ALL
    SELECT * FROM deleted_objects
) t
ORDER BY schema_name, object_type_name, object_subtype_name, change_type;
