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
            CONCAT('argument_types:', COALESCE(gtd.argument_types, '')),
            CONCAT('argument_modes:', COALESCE(gtd.argument_modes::TEXT, '')),
            CONCAT('return_type:', COALESCE(gtd.return_type::TEXT, '')),
            CONCAT('language:', COALESCE(gtd.language::TEXT, '')),
            CONCAT('volatility:', COALESCE(gtd.volatility, '')),
            CONCAT('parallel_safe:', COALESCE(gtd.parallel_safe, '')),
            CONCAT('owner_role:', COALESCE(gtd.owner_role, '')),
            CONCAT('privileges:', COALESCE(gtd.privileges, '')),
            CONCAT('dependencies:', COALESCE(gtd.dependencies, '')),
            CONCAT('function_body:', COALESCE(gtd.function_body, ''))
        ) AS object_subtype_details,

        -- Create MD5 hash from normalized concatenated string
        MD5(
              CONCAT_WS(
                ',',
                CONCAT('argument_types:', COALESCE(gtd.argument_types, '')),
                CONCAT('argument_modes:', COALESCE(gtd.argument_modes::TEXT, '')),
                CONCAT('return_type:', COALESCE(gtd.return_type::TEXT, '')),
                CONCAT('language:', COALESCE(gtd.language::TEXT, '')),
                CONCAT('volatility:', COALESCE(gtd.volatility, '')),
                CONCAT('parallel_safe:', COALESCE(gtd.parallel_safe, '')),
                CONCAT('owner_role:', COALESCE(gtd.owner_role, '')),
                CONCAT('privileges:', COALESCE(gtd.privileges, '')),
                CONCAT('dependencies:', COALESCE(gtd.dependencies, '')),
                CONCAT('function_body:', COALESCE(gtd.function_body, ''))
            )
        ) AS object_md5

    FROM pdcd_schema.get_functions_details(p_table_list) AS gtd
    ORDER BY gtd.schema_name, gtd.function_name;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/object_type_functions/get_functions_md5.sql'
-- Example usage:
-- drop function get_table_columns_md5;
-- SELECT * FROM pdcd_schema.get_table_functions_md5(ARRAY['analytics_schema']);


