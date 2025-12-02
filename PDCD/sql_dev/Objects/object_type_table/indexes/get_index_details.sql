---==== Index Details Function Updated to include Views and Materialized Views ====---
CREATE OR REPLACE FUNCTION pdcd_schema.get_index_details(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
    index_name TEXT,
    tablespace TEXT,
    indexdef TEXT,
    is_unique BOOLEAN,
    is_primary BOOLEAN,
    index_columns TEXT,
    index_predicate TEXT,
    access_method TEXT
)
LANGUAGE sql
AS $function$
WITH input_objects AS (

    /* CASE 1: No input -> all tables, views & materialized views */
    SELECT n.nspname || '.' || c.relname AS full_object_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r','m')
      AND (p_table_list IS NULL OR array_length(p_table_list,1) IS NULL)
      AND n.nspname NOT IN ('pg_catalog','information_schema')

    UNION ALL

    /* CASE 2: Schema-only input */
    SELECT n.nspname || '.' || c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r','m')
      AND EXISTS (
          SELECT 1 FROM unnest(p_table_list) t
          WHERE position('.' IN t) = 0
            AND n.nspname = t
      )

    UNION ALL

    /* CASE 3: schema.object input */
    SELECT unnest(p_table_list)
    WHERE p_table_list IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM unnest(p_table_list) t
          WHERE position('.' IN t) > 0
      )
)

SELECT
    n.nspname::TEXT AS schema_name,

    CASE 
        WHEN t.relkind = 'r' THEN 'Table'
       -- WHEN t.relkind = 'v' THEN 'View'
        WHEN t.relkind = 'm' THEN 'Materialized View'
    END AS object_type,

    t.relname::TEXT AS object_type_name,
    i.relname::TEXT AS index_name,
    ts.spcname::TEXT AS tablespace,
    pg_get_indexdef(i.oid)::TEXT AS indexdef,
    x.indisunique AS is_unique,
    x.indisprimary AS is_primary,

    array_to_string(
        ARRAY(
            SELECT a.attname
            FROM unnest(x.indkey::int[]) WITH ORDINALITY AS u(attnum, ord)
            JOIN pg_attribute a ON a.attnum = u.attnum AND a.attrelid = t.oid
            ORDER BY u.ord
        ),
        ','
    ) AS index_columns,

    pg_get_expr(x.indpred, x.indrelid)::TEXT AS index_predicate,
    am.amname::TEXT AS access_method

FROM pg_class t
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_index x ON x.indrelid = t.oid
JOIN pg_class i ON i.oid = x.indexrelid
LEFT JOIN pg_tablespace ts ON ts.oid = i.reltablespace
JOIN pg_am am ON am.oid = i.relam

WHERE (n.nspname || '.' || t.relname) IN (SELECT full_object_name FROM input_objects)
ORDER BY n.nspname, t.relname, i.relname;
$function$;

-- SELECT * FROM pdcd_schema.get_index_details(ARRAY['sales','hr']);