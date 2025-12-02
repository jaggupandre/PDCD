CREATE OR REPLACE FUNCTION pdcd_schema.get_columns_details(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
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

SELECT
    ns.nspname AS schema_name,

    CASE
        WHEN cls.relkind = 'r' THEN 'Table'
        WHEN cls.relkind = 'v' THEN 'View'
        WHEN cls.relkind = 'm' THEN 'Materialized View'
        ELSE 'Other'
    END AS object_type,

    cls.relname AS object_type_name,
    att.attname AS column_name,

    /* ---------- CLEAN BASE DATA TYPE ---------- */
    CASE typ.typname
        WHEN 'varchar'   THEN 'character varying'
        WHEN 'bpchar'    THEN 'character'
        WHEN 'int4'      THEN 'integer'
        WHEN 'int8'      THEN 'bigint'
        WHEN 'int2'      THEN 'smallint'
        WHEN 'float4'    THEN 'real'
        WHEN 'float8'    THEN 'double precision'
        WHEN 'bool'      THEN 'boolean'
        WHEN 'timestamptz' THEN 'timestamp with time zone'
        WHEN 'timestamp'   THEN 'timestamp without time zone'
        WHEN 'timetz'      THEN 'time with time zone'
        WHEN 'time'        THEN 'time without time zone'
        ELSE typ.typname
    END AS data_type,

    /* ---------- CHARACTER LENGTH ---------- */
    CASE 
        WHEN typ.typname IN ('varchar','bpchar')
            THEN att.atttypmod - 4
        ELSE NULL
    END AS character_maximum_length,

    /* ---------- NUMERIC PRECISION ---------- */
    CASE
        WHEN typ.typname = 'numeric'
            THEN ((att.atttypmod - 4) >> 16) & 65535
        WHEN typ.typname = 'int2' THEN 16
        WHEN typ.typname = 'int4' THEN 32
        WHEN typ.typname = 'int8' THEN 64
        WHEN typ.typname = 'float4' THEN 24
        WHEN typ.typname = 'float8' THEN 53
        ELSE NULL
    END AS numeric_precision,

    /* ---------- NUMERIC SCALE ---------- */
    CASE
        WHEN typ.typname = 'numeric'
            THEN (att.atttypmod - 4) & 65535
        WHEN typ.typname IN ('int2','int4','int8','float4','float8')
            THEN 0
        ELSE NULL
    END AS numeric_scale,

    CASE WHEN att.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,

    pg_get_expr(ad.adbin, ad.adrelid) AS column_default,

    'NO' AS is_identity,
    'NEVER' AS is_generated,
    NULL AS generation_expression,

    tc.conname AS constraint_name,
    att.attnum AS ordinal_position

FROM pg_class cls
JOIN pg_namespace ns ON ns.oid = cls.relnamespace
JOIN pg_attribute att ON att.attrelid = cls.oid 
   AND att.attnum > 0 
   AND NOT att.attisdropped
JOIN pg_type typ ON typ.oid = att.atttypid
LEFT JOIN pg_attrdef ad ON ad.adrelid = cls.oid AND ad.adnum = att.attnum
LEFT JOIN pg_constraint tc ON tc.conrelid = cls.oid AND att.attnum = ANY(tc.conkey)

WHERE cls.relkind IN ('r','v','m')
AND (
    p_table_list IS NULL
    AND ns.nspname NOT IN ('pg_catalog','information_schema')
    OR
    p_table_list IS NOT NULL
    AND ns.nspname = ANY(p_table_list)
)

ORDER BY
    ns.nspname,
    cls.relname,
    att.attnum;

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
