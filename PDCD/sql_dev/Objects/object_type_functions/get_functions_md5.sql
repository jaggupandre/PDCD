CREATE OR REPLACE FUNCTION pdcd_schema.get_table_functions_md5(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
    object_subtype TEXT,
    object_subtype_name TEXT,
    object_subtype_details TEXT,  -- metadata string
    object_md5 TEXT               -- MD5 hash of details
)
LANGUAGE SQL
AS $function$
    SELECT
        gtd.schema_name,
        'Function' AS object_type,
        gtd.function_name AS object_type_name,
        NULL AS object_subtype,
        NULL AS object_subtype_name,

        -- Build column details for tracking changes
        CONCAT_WS(
            ',',
            CONCAT('data_type:', COALESCE(gtd.argument_types, '')),
            CONCAT('max_length:', COALESCE(gtd.argument_modes::TEXT, '')),
            CONCAT('numeric_precision:', COALESCE(gtd.return_type::TEXT, '')),
            CONCAT('numeric_scale:', COALESCE(gtd.language::TEXT, '')),
            CONCAT('nullable:', COALESCE(gtd.volatility, '')),
            CONCAT('default_value:', COALESCE(gtd.parallel_safe, '')),
            CONCAT('is_identity:', COALESCE(gtd.owner_role, '')),
            CONCAT('is_generated:', COALESCE(gtd.privileges, '')),
            CONCAT('generation_expression:', COALESCE(gtd.dependencies, '')),
            CONCAT('constraint_name:', COALESCE(gtd.function_body, ''))
        ) AS object_subtype_details,

        -- Create MD5 hash from normalized concatenated string
        MD5(
            CONCAT_WS(
                ',',
                CONCAT('data_type:', COALESCE(gtd.argument_types, '')),
                CONCAT('max_length:', COALESCE(gtd.argument_modes::TEXT, '')),
                CONCAT('numeric_precision:', COALESCE(gtd.return_type::TEXT, '')),
                CONCAT('numeric_scale:', COALESCE(gtd.language::TEXT, '')),
                CONCAT('nullable:', COALESCE(gtd.volatility, '')),
                CONCAT('default_value:', COALESCE(gtd.parallel_safe, '')),
                CONCAT('is_identity:', COALESCE(gtd.owner_role, '')),
                CONCAT('is_generated:', COALESCE(gtd.privileges, '')),
                CONCAT('generation_expression:', COALESCE(gtd.dependencies, '')),
                CONCAT('constraint_name:', COALESCE(gtd.function_body, ''))
            )
        ) AS object_md5

    FROM pdcd_schema.get_functions_details(p_table_list) AS gtd
    ORDER BY gtd.schema_name, gtd.function_name;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/object_type_functions/get_functions_md5.sql'
-- Example usage:
-- drop function get_table_columns_md5;
-- SELECT * FROM pdcd_schema.get_table_functions_md5(ARRAY['analytics_schema']);


