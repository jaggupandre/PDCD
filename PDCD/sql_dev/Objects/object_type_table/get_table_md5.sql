--Summarizes the high level MD5 hashes of all table objects (columns, constraints, indexes, triggers, references)
--for each table in the provided list. 
CREATE OR REPLACE FUNCTION pdcd_schema.get_table_md5(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
    object_subtype TEXT,
    object_subtype_name TEXT,
    object_subtype_details TEXT,
    object_md5 TEXT
)
LANGUAGE SQL
AS $function$
    WITH all_objects AS (
        SELECT
            g.schema_name,
            g.object_type_name,
            'columns' AS obj_type,
            g.object_md5 AS obj_md5
        FROM pdcd_schema.get_table_all_columns_md5(p_table_list) g
    )
    SELECT
        ao.schema_name,
        'Table' AS object_type,
        ao.object_type_name,
        'All_Table_Objects' AS object_subtype,
        array_to_string(array_agg(DISTINCT ao.obj_type ORDER BY ao.obj_type), ', ') AS object_subtype_name,
        NULL AS object_subtype_details,
        md5(string_agg(ao.obj_md5, '' ORDER BY ao.obj_type)) AS object_md5
    FROM all_objects ao
    GROUP BY ao.schema_name, ao.object_type_name
    ORDER BY ao.schema_name, ao.object_type_name;
$function$;

-- \i '/Users/jagdish_pandre/PDCD/sql_dev/Objects/table_objects/get_table_md5.sql'
-- drop function get_table_md5(TEXT[]);
-- SELECT * FROM pdcd_schema.get_table_md5(ARRAY['analytics_schema']);
-- SELECT * FROM get_table_md5(ARRAY['sales','public']);


-- select * from public.md5_metadata_table where object_subtype  IN (
-- 'Columns'
-- );

-- select * from public.md5_metadata_table where object_subtype  IN (
-- 'Column'
-- );