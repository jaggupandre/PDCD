CREATE OR REPLACE FUNCTION get_index_details(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    table_name TEXT,
    index_name TEXT,
    tablespace TEXT,
    indexdef TEXT
)
LANGUAGE plpgsql
AS $function$
DECLARE
    v_all_tables TEXT[];
BEGIN
    -- CASE 1: No input -> all user tables
    IF p_table_list IS NULL OR array_length(p_table_list, 1) IS NULL THEN
        SELECT array_agg(n.nspname || '.' || c.relname)
        INTO v_all_tables
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema');

    -- CASE 2: Schema-only
    ELSIF NOT EXISTS (
        SELECT 1 FROM unnest(p_table_list) t WHERE position('.' in t) > 0
    ) THEN
        SELECT array_agg(n.nspname || '.' || c.relname)
        INTO v_all_tables
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY(p_table_list)
          AND c.relkind = 'r';

    -- CASE 3: Specific schema.table
    ELSE
        v_all_tables := p_table_list;
    END IF;

    RETURN QUERY
    SELECT
        n.nspname::TEXT AS schema_name,
        t.relname::TEXT AS table_name,
        i.relname::TEXT AS index_name,
        ts.spcname::TEXT AS tablespace,
        pg_get_indexdef(i.oid)::TEXT AS indexdef
    FROM pg_index x
    JOIN pg_class t ON t.oid = x.indrelid
    JOIN pg_class i ON i.oid = x.indexrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    LEFT JOIN pg_tablespace ts ON i.reltablespace = ts.oid
    WHERE (n.nspname || '.' || t.relname) = ANY(v_all_tables)
    ORDER BY n.nspname, t.relname, i.relname;
END;
$function$;


SELECT * FROM get_index_details();
SELECT * FROM get_index_details(ARRAY['public','legacy']);
SELECT * FROM get_index_details(ARRAY['public.people','sales.region_sales_west']);