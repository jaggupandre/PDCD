CREATE OR REPLACE FUNCTION pdcd_schema.get_table_columns_md5(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
    object_subtype TEXT,          -- 'Column'
    object_subtype_name TEXT,     -- column_name
    object_subtype_details TEXT,  -- metadata string
    object_md5 TEXT               -- MD5 hash of details
)
LANGUAGE SQL
AS $function$
    SELECT
        gtd.schema_name,
        'Table' AS object_type,
        gtd.table_name AS object_type_name,
        'Column' AS object_subtype,
        gtd.column_name AS object_subtype_name,

        -- Build column details for tracking changes
        -- gtd.object_subtype_details AS object_subtype_details,
        CONCAT_WS(
            ',',
            -- CONCAT('schema_name:', COALESCE(c.table_schema, '')),
            -- CONCAT('table_name:', COALESCE(c.table_name, '')),
            -- CONCAT('column_name:', COALESCE(c.column_name, '')),
            CONCAT('data_type:', COALESCE(gtd.data_type, '')),
            CONCAT('max_length:', COALESCE(gtd.character_maximum_length::TEXT, '')),
            CONCAT('numeric_precision:', COALESCE(gtd.numeric_precision::TEXT, '')),
            CONCAT('numeric_scale:', COALESCE(gtd.numeric_scale::TEXT, '')),
            CONCAT('nullable:', COALESCE(gtd.is_nullable, '')),
            CONCAT('default_value:', COALESCE(gtd.column_default, '')),
            CONCAT('is_identity:', COALESCE(gtd.is_identity, '')),
            CONCAT('is_generated:', COALESCE(gtd.is_generated, '')),
            CONCAT('generation_expression:', COALESCE(gtd.generation_expression, '')),
            CONCAT('constraint_name:', COALESCE(gtd.constraint_name, '')),
            CONCAT('ordinal_position:', gtd.ordinal_position::TEXT)
        ) AS object_subtype_details,

        -- Create MD5 hash from normalized concatenated string
        -- MD5(gtd.object_subtype_details) AS object_md5
        MD5(
            CONCAT_WS(
            ',',
            -- CONCAT('schema_name:', COALESCE(c.table_schema, '')),
            -- CONCAT('table_name:', COALESCE(c.table_name, '')),
            -- CONCAT('column_name:', COALESCE(c.column_name, '')),
            CONCAT('data_type:', COALESCE(gtd.data_type, '')),
            CONCAT('max_length:', COALESCE(gtd.character_maximum_length::TEXT, '')),
            CONCAT('numeric_precision:', COALESCE(gtd.numeric_precision::TEXT, '')),
            CONCAT('numeric_scale:', COALESCE(gtd.numeric_scale::TEXT, '')),
            CONCAT('nullable:', COALESCE(gtd.is_nullable, '')),
            CONCAT('default_value:', COALESCE(gtd.column_default, '')),
            CONCAT('is_identity:', COALESCE(gtd.is_identity, '')),
            CONCAT('is_generated:', COALESCE(gtd.is_generated, '')),
            CONCAT('generation_expression:', COALESCE(gtd.generation_expression, '')),
            CONCAT('constraint_name:', COALESCE(gtd.constraint_name, '')),
            CONCAT('ordinal_position:', gtd.ordinal_position::TEXT)
            )
        ) AS object_md5

    FROM pdcd_schema.get_columns_details(p_table_list) AS gtd
    ORDER BY gtd.schema_name, gtd.table_name, gtd.ordinal_position;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/columns/get_table_columns_md5.sql'
-- Example usage:
-- drop function get_table_columns_md5
-- SELECT * FROM load_md5_metadata_table(ARRAY['companies.employees']);



-- SELECT * FROM pdcd_schema.get_table_columns_md5();
-- SELECT * FROM pdcd_schema.get_table_columns_md5(ARRAY['analytics_schema.departments']);
-- SELECT * FROM pdcd_schema.get_table_columns_md5(ARRAY['pdcd_schema', 'analytics_schema']);

