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
        'Table' AS object_type,
        gtd.table_name AS object_type_name,
        'Trigger' AS object_subtype,
        gtd.trigger_name AS object_subtype_name,
        concat_ws(
                ',',
                -- coalesce(gtd.trigger_name, ''),
                'trigger_event:' || coalesce(gtd.trigger_event, ''),
                'trigger_timing:' || coalesce(gtd.trigger_timing, ''),
                'trigger_level:' || coalesce(gtd.trigger_level, ''),
                'trigger_enabled:' || coalesce(gtd.trigger_enabled::TEXT, ''),
                'trigger_definition:' || coalesce(gtd.trigger_definition, ''),
                'trigger_function_name:' || coalesce(gtd.trigger_function_name, ''),
                'trigger_function_arguments:' || coalesce(gtd.trigger_function_arguments, ''),
                'trigger_function_definition:' || coalesce(gtd.trigger_function_definition, '')
            ) as object_subtype_details,
        md5(
            concat_ws(
                ':',
                -- coalesce(gtd.trigger_name, ''),
                'trigger_event:' || coalesce(gtd.trigger_event, ''),
                'trigger_timing:' || coalesce(gtd.trigger_timing, ''),
                'trigger_level:' ||  coalesce(gtd.trigger_level, ''),
                'trigger_enabled:' || coalesce(gtd.trigger_enabled::TEXT, ''),
                -- 'trigger_definition:' || coalesce(gtd.trigger_definition, ''),
                'trigger_function_name:' || coalesce(gtd.trigger_function_name, ''),
                'trigger_function_arguments:' || coalesce(gtd.trigger_function_arguments, ''),
                'trigger_function_definition:' || coalesce(gtd.trigger_function_definition, '')
            )
        ) AS object_md5
    FROM pdcd_schema.get_trigger_details(p_table_list) gtd
    ORDER BY gtd.schema_name, gtd.table_name, gtd.trigger_name;
END;
$function$;

-- \i '/Users/manoj_anumalla/Desktop/PDCD/PDCD/sql_dev/Objects/table_objects/triggers/trigger_details_md5.sql'
-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/triggers/get_table_triggers_md5.sql'
-- drop function get_table_triggers_md5(TEXT[]);
-- SELECT * FROM pdcd_schema.get_table_triggers_md5(ARRAY['analytics_schema']);