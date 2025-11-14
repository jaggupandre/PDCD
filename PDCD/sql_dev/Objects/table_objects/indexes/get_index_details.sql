CREATE OR REPLACE FUNCTION pdcd_schema.get_index_details(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    table_name TEXT,
    index_name TEXT,
    tablespace TEXT,
    indexdef TEXT
)
LANGUAGE sql
AS $function$
WITH input_tables AS (

    -- CASE 1: No input → all non-system tables
    SELECT n.nspname || '.' || c.relname AS full_table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND (p_table_list IS NULL OR array_length(p_table_list, 1) IS NULL)
      AND n.nspname NOT IN ('pg_catalog','information_schema')

    UNION ALL

    -- CASE 2: Schema-only input
    SELECT n.nspname || '.' || c.relname AS full_table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND p_table_list IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM unnest(p_table_list) t
          WHERE position('.' IN t) = 0
            AND n.nspname = t
      )

    UNION ALL

    -- CASE 3: Fully-qualified schema.table input
    SELECT unnest(p_table_list) AS full_table_name
    WHERE p_table_list IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM unnest(p_table_list) t
          WHERE position('.' IN t) > 0
      )
)

SELECT
    n.nspname::TEXT AS schema_name,
    t.relname::TEXT AS table_name,
    i.relname::TEXT AS index_name,
    ts.spcname::TEXT AS tablespace,
    pg_get_indexdef(i.oid)::TEXT AS indexdef
FROM pg_class t
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_index x ON x.indrelid = t.oid
JOIN pg_class i ON i.oid = x.indexrelid
LEFT JOIN pg_tablespace ts ON ts.oid = i.reltablespace
WHERE (n.nspname || '.' || t.relname) IN (SELECT full_table_name FROM input_tables)
ORDER BY n.nspname, t.relname, i.relname;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/indexes/get_index_details.sql'

-- SELECT * FROM get_index_details();
-- SELECT * FROM get_index_details(ARRAY['public','legacy']);
-- SELECT * FROM get_index_details(ARRAY['public.people','sales.region_sales_west']);