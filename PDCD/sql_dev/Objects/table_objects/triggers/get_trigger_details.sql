CREATE OR REPLACE FUNCTION get_trigger_details(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    table_name TEXT,
    trigger_name TEXT,
    trigger_definition TEXT,
    trigger_function_name TEXT,
    trigger_function_definition TEXT
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
        c.relname::TEXT AS table_name,
        t.tgname::TEXT AS trigger_name,
        pg_get_triggerdef(t.oid)::TEXT AS trigger_definition,
        p.proname::TEXT AS trigger_function_name,
        pg_get_functiondef(p.oid)::TEXT AS trigger_function_definition
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_proc p ON p.oid = t.tgfoid
    WHERE t.tgisinternal = false
      AND (n.nspname || '.' || c.relname) = ANY(v_all_tables)
    ORDER BY n.nspname, c.relname, t.tgname;
END;
$function$;

-- SELECT * FROM get_trigger_details(ARRAY['sales']);