CREATE OR REPLACE FUNCTION pdcd_schema.get_table_triggers_md5(
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
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        gtd.schema_name,
        gtd.object_type,                       -- Table / View / Materialized View
        gtd.object_type_name,                -- table or view name
        'Trigger' AS object_subtype,
        gtd.trigger_name AS object_subtype_name,

        concat_ws(
            ',',
            'trigger_event:' || coalesce(gtd.trigger_event, ''),
            'trigger_timing:' || coalesce(gtd.trigger_timing, ''),
            'trigger_level:' || coalesce(gtd.trigger_level, ''),
            'trigger_enabled:' || coalesce(gtd.trigger_enabled::TEXT, ''),
            'trigger_definition:' || coalesce(gtd.trigger_definition, ''),
            'trigger_function_name:' || coalesce(gtd.trigger_function_name, ''),
            'trigger_function_arguments:' || coalesce(gtd.trigger_function_arguments, ''),
            'trigger_function_definition:' || coalesce(gtd.trigger_function_definition, '')
        ) AS object_subtype_details,

        md5(
            concat_ws(
                ':',
                'trigger_event:' || coalesce(gtd.trigger_event, ''),
                'trigger_timing:' || coalesce(gtd.trigger_timing, ''),
                'trigger_level:' || coalesce(gtd.trigger_level, ''),
                'trigger_enabled:' || coalesce(gtd.trigger_enabled::TEXT, ''),
                'trigger_function_name:' || coalesce(gtd.trigger_function_name, ''),
                'trigger_function_arguments:' || coalesce(gtd.trigger_function_arguments, ''),
                'trigger_function_definition:' || coalesce(gtd.trigger_function_definition, '')
            )
        ) AS object_md5

    FROM pdcd_schema.get_trigger_details(p_table_list) gtd
    ORDER BY gtd.schema_name, gtd.object_type_name, gtd.trigger_name;
END;
$function$;

-- SELECT * FROM pdcd_schema.get_table_triggers_md5(ARRAY['sales','hr']);