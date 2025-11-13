CREATE OR REPLACE FUNCTION get_table_triggers_md5(
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
        gtd.schema_name,
        'Table' AS object_type,
        gtd.table_name AS object_name,
        'Trigger' AS object_subtype,
        gtd.trigger_name AS object_subtype_name,
        concat_ws(
                ',',
                -- coalesce(gtd.trigger_name, ''),
                'trigger_definition:' || coalesce(gtd.trigger_definition, ''),
                'trigger_function_name:' || coalesce(gtd.trigger_function_name, ''),
                'trigger_function_definition:' || coalesce(gtd.trigger_function_definition, '')
            ) as object_subtype_details,
        md5(
            concat_ws(
                ':',
                coalesce(gtd.schema_name, ''),
                coalesce(gtd.table_name, ''),
                coalesce(gtd.trigger_name, ''),
                coalesce(gtd.trigger_definition, ''),
                coalesce(gtd.trigger_function_name, ''),
                coalesce(gtd.trigger_function_definition, '')
            )
        ) AS object_md5
    FROM get_trigger_details(p_table_list) gtd
    ORDER BY gtd.schema_name, gtd.table_name, gtd.trigger_name;
END;
$function$;

drop function get_table_triggers_md5(TEXT[]);
SELECT * FROM get_table_triggers_md5(ARRAY['companies.employees']);

-- SELECT * FROM get_table_triggers_md5(ARRAY['sales']);