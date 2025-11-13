CREATE OR REPLACE FUNCTION pdcd_schema.get_columns_details(
    p_table_list TEXT[] DEFAULT NULL  -- Array input like ARRAY['public'] or ARRAY['public.people','public.orders']
)
RETURNS TABLE (
    schema_name TEXT,
    table_name TEXT,
    column_name TEXT,
    data_type TEXT,
    character_maximum_length INT,
    numeric_precision INT,
    numeric_scale INT,
    is_nullable TEXT,
    column_default TEXT,
    is_identity TEXT,
    is_generated TEXT,
    generation_expression TEXT,
    constraint_name TEXT,
    ordinal_position INT
)
LANGUAGE SQL
AS $function$
    WITH input_tables AS (
        -- Case 1: No input array provided → all user schemas
        SELECT NULL::TEXT AS schema_table
        WHERE p_table_list IS NULL
        UNION ALL
        -- Case 2: Expand provided array into individual entries
        SELECT UNNEST(p_table_list) AS schema_table
    )
    SELECT
        c.table_schema AS schema_name,
        c.table_name,
        c.column_name,
        c.data_type,
        c.character_maximum_length,
        c.numeric_precision,
        c.numeric_scale,
        c.is_nullable,
        c.column_default,
        c.is_identity,
        c.is_generated,
        c.generation_expression,
        tc.constraint_name,
        c.ordinal_position AS ordinal_position

    FROM information_schema.columns c
    LEFT JOIN information_schema.key_column_usage tc
        ON c.table_schema = tc.table_schema
       AND c.table_name = tc.table_name
       AND c.column_name = tc.column_name

    WHERE
        (
            -- CASE 1: no input → all user schemas except system ones
            p_table_list IS NULL
            AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
        )
        OR
        (
            -- CASE 2a: Only schema name(s) provided (like ARRAY['public'])
            p_table_list IS NOT NULL
            AND c.table_schema = ANY(p_table_list)
        )
        OR
        (
            -- CASE 2b: Fully qualified schema.table provided (like ARRAY['public.people'])
            p_table_list IS NOT NULL
            AND (c.table_schema || '.' || c.table_name) = ANY(p_table_list)
        )

    ORDER BY c.table_schema, c.table_name, c.ordinal_position;
$function$;


--            List of schemas
--        Name       |       Owner
-- ------------------+-------------------
--  analytics_schema | jagdish_pandre
--  pdcd_schema      | jagdish_pandre
--  public           | pg_database_owner

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/columns/get_columns_details.sql'

-- All user tables:
-- SELECT * FROM pdcd_schema.get_columns_details();

-- Specific tables:
-- SELECT * FROM pdcd_schema.get_columns_details(ARRAY['pdcd_schema.snapshot_tbl']);
-- SELECT * FROM pdcd_schema.get_columns_details(ARRAY['analytics_schema.departments']);

-- SELECT * FROM pdcd_schema.get_columns_details('public.sales,analytics.customers');

-- -- Example: compute md5 hash for columns
-- SELECT
--     table_schema,
--     table_name,
--     column_name,
--     md5(object_subtype_details) AS column_md5
-- FROM pdcd_schema.get_columns_details('public.sales');
