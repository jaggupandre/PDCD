CREATE OR REPLACE FUNCTION get_reference_details(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    table_name TEXT,
    source_column TEXT,
    target_schema TEXT,
    target_table TEXT,
    target_column TEXT,
    constraint_name TEXT
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
        src_ns.nspname::TEXT AS schema_name,
        src_tbl.relname::TEXT AS table_name,
        src_col.attname::TEXT AS source_column,
        tgt_ns.nspname::TEXT AS target_schema,
        tgt_tbl.relname::TEXT AS target_table,
        tgt_col.attname::TEXT AS target_column,
        con.conname::TEXT AS constraint_name
    FROM pg_constraint con
    JOIN pg_class src_tbl ON src_tbl.oid = con.conrelid
    JOIN pg_namespace src_ns ON src_ns.oid = src_tbl.relnamespace
    JOIN unnest(con.conkey) WITH ORDINALITY AS src_cols(attnum, ord) ON TRUE
    JOIN pg_attribute src_col ON src_col.attrelid = con.conrelid AND src_col.attnum = src_cols.attnum
    JOIN pg_class tgt_tbl ON tgt_tbl.oid = con.confrelid
    JOIN pg_namespace tgt_ns ON tgt_ns.oid = tgt_tbl.relnamespace
    JOIN unnest(con.confkey) WITH ORDINALITY AS tgt_cols(attnum, ord) ON tgt_cols.ord = src_cols.ord
    JOIN pg_attribute tgt_col ON tgt_col.attrelid = con.confrelid AND tgt_col.attnum = tgt_cols.attnum
    WHERE con.contype = 'f'
      AND (src_ns.nspname || '.' || src_tbl.relname) = ANY(v_all_tables)
    ORDER BY src_ns.nspname, src_tbl.relname, con.conname, src_cols.ord;
END;
$function$;

SELECT *
FROM get_reference_details(
    ARRAY[
    	'public.orders',
    	'public.order_items',
    	'public.payments'
    ]
);
