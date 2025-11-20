CREATE OR REPLACE FUNCTION get_table_all_triggers_md5(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    object_type TEXT,
    object_name TEXT,
    object_subtype TEXT,
    object_subtype_name TEXT,
    object_subtype_details TEXT,
    object_md5 TEXT
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        g.schema_name,
        'Table' AS object_type,
        g.object_name,
        'Triggers' AS object_subtype,
        array_to_string(array_agg(g.object_subtype_name ORDER BY g.object_subtype_name), ', ') AS object_subtype_name,
        NULL as object_subtype_details,
        md5(string_agg(g.object_md5, '' ORDER BY g.object_subtype_name)) AS object_md5
    FROM get_table_triggers_md5(p_table_list) g
    GROUP BY g.schema_name, g.object_name
    ORDER BY g.schema_name, g.object_name;
END;
$function$;

-- SELECT * FROM get_table_all_triggers_md5(ARRAY['sales']);