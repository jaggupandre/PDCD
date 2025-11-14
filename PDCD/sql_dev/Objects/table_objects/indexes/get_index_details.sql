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
LANGUAGE sql
AS $function$
WITH input_raw AS (
    SELECT unnest(p_table_list) AS val
),
flags AS (
    SELECT
        (p_table_list IS NULL OR array_length(p_table_list, 1) IS NULL) AS is_empty,
        EXISTS (
            SELECT 1 FROM input_raw WHERE position('.' IN val) > 0
        ) AS has_dot
),

-- Build resolved table list using a single CTE
resolved AS (
    SELECT 
        -- CASE 1: no input → use system tables
        CASE 
            WHEN (SELECT is_empty FROM flags) THEN n.nspname
            WHEN (SELECT has_dot FROM flags) THEN split_part(ir.val, '.', 1)
            ELSE ir.val
        END AS schema_name,

        CASE 
            WHEN (SELECT is_empty FROM flags) THEN c.relname
            WHEN (SELECT has_dot FROM flags) THEN split_part(ir.val, '.', 2)
            ELSE NULL
        END AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN flags f
    LEFT JOIN input_raw ir ON TRUE
    WHERE
        -- CASE 1: no input → all user tables
        (f.is_empty = TRUE
         AND c.relkind = 'r'
         AND n.nspname NOT IN ('pg_catalog','information_schema'))

        OR

        -- CASE 2: schema-only list
        (f.is_empty = FALSE
         AND f.has_dot = FALSE
         AND ir.val = n.nspname
         AND c.relkind = 'r')

        OR

        -- CASE 3: fully-qualified schema.table
        (f.has_dot = TRUE)
)

SELECT
    r.schema_name,
    r.table_name,
    i.relname AS index_name,
    ts.spcname AS tablespace,
    pg_get_indexdef(i.oid) AS indexdef
FROM resolved r
JOIN pg_namespace n ON n.nspname = r.schema_name
JOIN pg_class t ON t.relname = r.table_name AND t.relnamespace = n.oid
JOIN pg_index x ON x.indrelid = t.oid
JOIN pg_class i ON i.oid = x.indexrelid
LEFT JOIN pg_tablespace ts ON ts.oid = i.reltablespace
ORDER BY r.schema_name, r.table_name, i.relname;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/indexes/get_index_details.sql'

-- SELECT * FROM get_index_details();
-- SELECT * FROM get_index_details(ARRAY['public','legacy']);
-- SELECT * FROM get_index_details(ARRAY['public.people','sales.region_sales_west']);