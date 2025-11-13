CREATE OR REPLACE FUNCTION pdcd_schema.get_table_constraints_md5(
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
        'Constraint' AS object_subtype,
        gtd.constraint_name AS object_subtype_name,
        gtd.object_subtype_details,
        md5(gtd.object_subtype_details) AS object_md5
    FROM pdcd_schema.get_constraint_details(p_table_list) gtd
    ORDER BY gtd.schema_name, gtd.table_name, gtd.constraint_name;
END;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/constraints/get_table_constraints_md5.sql'
-- Example usages:
-- drop function get_table_constraints_md5(TEXT[]);
-- SELECT * FROM get_table_constraints_md5(ARRAY['companies.employees']);



-- SELECT * FROM get_table_constraints_md5();
-- SELECT * FROM get_table_constraints_md5(ARRAY['public', 'legacy']);
-- SELECT * FROM get_table_constraints_md5(ARRAY['public.people', 'public.orders']);
