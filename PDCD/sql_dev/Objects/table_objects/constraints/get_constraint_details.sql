CREATE OR REPLACE FUNCTION pdcd_schema.get_constraint_details(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    table_name TEXT,
    constraint_name TEXT,
    constraint_type TEXT,
    column_name TEXT,
    definition TEXT
)
LANGUAGE sql
AS $function$
    WITH input_tables AS (
        -- CASE 1: If no input given → all non-system tables
        SELECT n.nspname || '.' || c.relname AS full_table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND (p_table_list IS NULL OR array_length(p_table_list, 1) IS NULL)
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')

        UNION ALL

        -- CASE 2: Schema-only input
        SELECT n.nspname || '.' || c.relname AS full_table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND p_table_list IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM unnest(p_table_list) t 
              WHERE position('.' in t) = 0 
                AND n.nspname = t
          )

        UNION ALL

        -- CASE 3: Specific schema.table input
        SELECT unnest(p_table_list) AS full_table_name
        WHERE p_table_list IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM unnest(p_table_list) t WHERE position('.' in t) > 0
          )
    )
    SELECT
        n.nspname::TEXT AS schema_name,
        t.relname::TEXT AS table_name,
        con.conname::TEXT AS constraint_name,
        CASE con.contype
            WHEN 'p' THEN 'PRIMARY KEY'
            WHEN 'u' THEN 'UNIQUE'
            WHEN 'f' THEN 'FOREIGN KEY'
            WHEN 'c' THEN 'CHECK'
            WHEN 'x' THEN 'EXCLUDE'
            ELSE con.contype::TEXT
        END AS constraint_type,
        array_to_string(
            ARRAY(
                SELECT att.attname
                FROM unnest(con.conkey) AS colnum
                JOIN pg_attribute att 
                  ON att.attrelid = con.conrelid 
                 AND att.attnum = colnum
                ORDER BY att.attnum
            ),
            ','
        ) AS column_name,
        pg_get_constraintdef(con.oid, true)::TEXT AS definition
    FROM pg_constraint con
    JOIN pg_class t ON t.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE (n.nspname || '.' || t.relname) IN (SELECT full_table_name FROM input_tables)
    ORDER BY n.nspname, t.relname, con.conname;
$function$;


-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/constraints/get_constraint_details.sql'
-- Example usages:

-- SELECT * FROM pdcd_schema.get_constraint_details();
-- SELECT * FROM pdcd_schema.get_constraint_details(ARRAY['analytics_schema']);
-- SELECT * FROM pdcd_schema.get_constraint_details(ARRAY['analytics_schema.departments']);
-- SELECT * FROM pdcd_schema.get_constraint_details(ARRAY['analytics_schema.employees']);


-- SELECT * FROM pdcd_schema.get_constraint_details(ARRAY['public', 'legacy']);
-- SELECT * FROM pdcd_schema.get_constraint_details(ARRAY['public.people', 'public.orders']);


