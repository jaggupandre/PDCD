CREATE OR REPLACE FUNCTION pdcd_schema.get_reference_details(
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
LANGUAGE sql
AS $function$

WITH resolved_tables AS (

    /* CASE 1: No input → include all user tables */
    SELECT array_agg(n.nspname || '.' || c.relname) AS tbls
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE (p_table_list IS NULL OR array_length(p_table_list, 1) IS NULL)
      AND c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')

    UNION ALL

    /* CASE 2: Schema-only → all tables in those schemas */
    SELECT array_agg(n.nspname || '.' || c.relname) AS tbls
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE p_table_list IS NOT NULL
      AND NOT EXISTS (
            SELECT 1 FROM unnest(p_table_list) t WHERE position('.' IN t) > 0
      )
      AND n.nspname = ANY(p_table_list)
      AND c.relkind = 'r'

    UNION ALL

    /* CASE 3: Full schema.table list provided */
    SELECT p_table_list AS tbls
    WHERE p_table_list IS NOT NULL
      AND EXISTS (
            SELECT 1 FROM unnest(p_table_list) t WHERE position('.' IN t) > 0
      )
),
final_tables AS (
    SELECT tbls FROM resolved_tables WHERE tbls IS NOT NULL LIMIT 1
)

SELECT
    src_ns.nspname        AS schema_name,
    src_tbl.relname       AS table_name,
    src_col.attname       AS source_column,
    tgt_ns.nspname        AS target_schema,
    tgt_tbl.relname       AS target_table,
    tgt_col.attname       AS target_column,
    con.conname           AS constraint_name
FROM pg_constraint con
JOIN pg_class src_tbl ON src_tbl.oid = con.conrelid
JOIN pg_namespace src_ns ON src_ns.oid = src_tbl.relnamespace
JOIN unnest(con.conkey)     WITH ORDINALITY AS src_cols(attnum, ord) ON TRUE
JOIN pg_attribute src_col
     ON src_col.attrelid = con.conrelid
    AND src_col.attnum = src_cols.attnum
JOIN pg_class tgt_tbl ON tgt_tbl.oid = con.confrelid
JOIN pg_namespace tgt_ns ON tgt_ns.oid = tgt_tbl.relnamespace
JOIN unnest(con.confkey)    WITH ORDINALITY AS tgt_cols(attnum, ord)
     ON tgt_cols.ord = src_cols.ord
JOIN pg_attribute tgt_col
     ON tgt_col.attrelid = con.confrelid
    AND tgt_col.attnum = tgt_cols.attnum
WHERE con.contype = 'f'
  AND (src_ns.nspname || '.' || src_tbl.relname) = ANY(
        ARRAY(SELECT unnest(tbls) FROM final_tables)
      )
ORDER BY src_ns.nspname, src_tbl.relname, con.conname, src_cols.ord;

$function$;
-- SELECT * FROM pdcd_schema.get_reference_details(ARRAY['analytics_schema']);
-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/references/get_reference_details.sql'