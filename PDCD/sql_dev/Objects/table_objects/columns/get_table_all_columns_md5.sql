CREATE OR REPLACE FUNCTION pdcd_schema.get_table_all_columns_md5(
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
    SELECT
        g.schema_name,
        'Table' AS object_type,
        g.object_type_name,
        'Columns' AS object_subtype,
        array_to_string(array_agg(g.object_subtype_name ORDER BY g.ordinal_position), ', ') AS object_subtype_name,
        NULL AS object_subtype_details,
        md5(string_agg(g.object_md5, '' ORDER BY g.ordinal_position)) AS object_md5
    FROM (
        SELECT 
            gtd.schema_name,
            gtd.table_name AS object_type_name,
            gtd.column_name AS object_subtype_name,
            gtd.ordinal_position,
            md5(gtd.object_subtype_details) AS object_md5
        FROM pdcd_schema.get_columns_details(p_table_list) AS gtd
    ) AS g
    GROUP BY g.schema_name, g.object_type_name
    ORDER BY g.schema_name, g.object_type_name;
$function$;

-- \i '/Users/jagdish_pandre/PDCD/sql_dev/Objects/table_objects/columns/get_table_all_columns_md5.sql'
-- drop function get_table_all_columns_md5(TEXT[]);

-- SELECT * FROM pdcd_schema.get_table_all_columns_md5(ARRAY['analytics_schema.departments']);

-- SELECT * FROM pdcd_schema.get_table_all_columns_md5();
-- SELECT * FROM pdcd_schema.get_table_all_columns_md5(ARRAY['public']);
-- SELECT * FROM pdcd_schema.get_table_all_columns_md5(ARRAY['public.people', 'public.orders']);
