CREATE OR REPLACE FUNCTION pdcd_schema.get_table_indexes_md5(
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
        gid.schema_name,
        'Table' AS object_type,
        gid.table_name AS object_type_name,
        'Index' AS object_subtype,
        gid.index_name AS object_subtype_name,
        concat_ws(
                ',',
                'tablespace:' || coalesce(gid.tablespace, ''),
                'indexdef:' || coalesce(gid.indexdef, ''),
                'is_unique:' || coalesce(gid.is_unique::TEXT, ''),
                'is_primary:' || coalesce(gid.is_primary::TEXT, ''),
                'index_columns:' || coalesce(gid.index_columns::TEXT, ''),
                'index_predicate:' || coalesce(gid.index_predicate::TEXT, ''),
                'access_method:' || coalesce(gid.access_method::TEXT, '')
            ) as object_subtype_details,
        md5(
            concat_ws(
                ':',
                'tablespace:' || coalesce(gid.tablespace, ''),
                'indexdef:' || coalesce(gid.indexdef, ''),
                'is_unique:' || coalesce(gid.is_unique::TEXT, ''),
                'is_primary:' || coalesce(gid.is_primary::TEXT, ''),
                'index_columns:' || coalesce(gid.index_columns::TEXT, ''),
                'index_predicate:' || coalesce(gid.index_predicate::TEXT, ''),
                'access_method:' || coalesce(gid.access_method::TEXT, '')
            )
        ) AS object_md5
    FROM pdcd_schema.get_index_details(p_table_list) gid
    ORDER BY gid.schema_name, gid.table_name, gid.index_name;
END;
$function$;



drop function get_table_indexes_md5(TEXT[]);
SELECT * FROM get_table_indexes_md5(ARRAY['companies.employees']);


SELECT * FROM get_table_indexes_md5();
SELECT * FROM get_table_indexes_md5(ARRAY['public','legacy']);
SELECT * FROM get_table_all_get_table_indexes_md5indexes_md5(ARRAY['public.people','sales.region_sales_west']);

