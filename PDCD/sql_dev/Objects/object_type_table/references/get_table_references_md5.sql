CREATE OR REPLACE FUNCTION pdcd_schema.get_table_references_md5(
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
        grd.schema_name,
        'Table' AS object_type,
        grd.table_name AS object_type_name,
        'Reference' AS object_subtype,
        grd.constraint_name AS object_subtype_name,
        concat_ws(
                ',',
                'source_column:' || coalesce(grd.source_column, ''),
                'target_schema:' || coalesce(grd.target_schema, ''),
                'target_table:' || coalesce(grd.target_table, ''),
                'target_column:' || coalesce(grd.target_column, ''),
                'constraint_name:' || coalesce(grd.constraint_name, '')
            ) as object_subtype_details,
        md5(
            concat_ws(
                ':',
                coalesce(grd.schema_name, ''),
                coalesce(grd.table_name, ''),
                coalesce(grd.source_column, ''),
                coalesce(grd.target_schema, ''),
                coalesce(grd.target_table, ''),
                coalesce(grd.target_column, ''),
                coalesce(grd.constraint_name, '')
            )
        ) AS object_md5
    FROM pdcd_schema.get_reference_details(p_table_list) grd
    ORDER BY grd.schema_name, grd.table_name, grd.constraint_name, grd.source_column;
END;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/references/get_table_references_md5.sql'

-- SELECT * FROM pdcd_schema.get_table_references_md5(ARRAY['analytics_schema']);
-- SELECT * FROM get_table_references_md5(ARRAY['sales','public']);
-- drop function get_table_references_md5(TEXT[]);
-- SELECT * FROM get_table_references_md5(ARRAY['companies.employees']);

-- SELECT *
-- FROM get_table_references_md5(
--     ARRAY[
--     	'public.orders',
--     	'public.order_items',
--     	'public.payments'
--     ]
-- );