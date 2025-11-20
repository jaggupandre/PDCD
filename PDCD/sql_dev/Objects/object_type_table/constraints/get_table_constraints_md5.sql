CREATE OR REPLACE FUNCTION pdcd_schema.get_table_constraints_md5(
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
        'Constraint' AS object_subtype,
        gtd.constraint_name AS object_subtype_name,
        -- Build column details for tracking changes
        CONCAT_WS(
            ',',
            CONCAT('constraint_type:', COALESCE(gtd.constraint_type, '')),
            CONCAT('column_name:', COALESCE(gtd.column_name, '')),
            CONCAT('definition:', COALESCE(gtd.definition, ''))
        ) AS object_subtype_details,
        -- Create MD5 hash from normalized concatenated string
        MD5(
            CONCAT_WS(
            ',',
            CONCAT('constraint_type:', COALESCE(gtd.constraint_type, '')),
            CONCAT('column_name:', COALESCE(gtd.column_name, '')),
            CONCAT('definition:', COALESCE(gtd.definition, ''))
            )
        ) AS object_md5
    FROM pdcd_schema.get_constraint_details(p_table_list) gtd
    ORDER BY gtd.schema_name, gtd.table_name, gtd.constraint_name;
END;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/constraints/get_table_constraints_md5.sql'
-- Example usages:
-- drop function pdcd_schema.get_table_constraints_md5(TEXT[]);
-- SELECT * FROM pdcd_schema.get_table_constraints_md5(ARRAY['analytics_schema.employees']);

-- SELECT * FROM pdcd_schema.get_table_columns_md5(ARRAY['analytics_schema.employees'])
-- UNION ALL
-- SELECT * FROM pdcd_schema.get_table_constraints_md5(ARRAY['analytics_schema.employees']);