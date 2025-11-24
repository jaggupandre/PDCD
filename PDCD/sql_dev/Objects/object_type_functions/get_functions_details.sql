CREATE OR REPLACE FUNCTION pdcd_schema.get_functions_details(
    p_schema_list TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    schema_name TEXT,
    function_name TEXT,
    argument_types TEXT,
    argument_modes TEXT,
    return_type TEXT,
    language TEXT,
    volatility TEXT,
    parallel_safe TEXT,
    owner_role TEXT,
    privileges TEXT,
    dependencies TEXT,
    function_body TEXT
)
LANGUAGE SQL
AS $function$
    SELECT
        n.nspname AS schema_name,
        p.proname AS function_name,

        pg_get_function_arguments(p.oid) AS argument_types,
        pg_get_function_identity_arguments(p.oid) AS argument_modes,
        format_type(p.prorettype, NULL) AS return_type,

        l.lanname AS language,
        CASE p.provolatile
            WHEN 'i' THEN 'IMMUTABLE'
            WHEN 's' THEN 'STABLE'
            WHEN 'v' THEN 'VOLATILE'
        END AS volatility,

        CASE p.proparallel
            WHEN 's' THEN 'SAFE'
            WHEN 'r' THEN 'RESTRICTED'
            WHEN 'u' THEN 'UNSAFE'
        END AS parallel_safe,

        pg_get_userbyid(p.proowner) AS owner_role,
        p.proacl::TEXT AS privileges,

        (
            SELECT string_agg(refobjid::regclass::text, ',')
            FROM pg_depend
            WHERE objid = p.oid
        ) AS dependencies,

        --------------------------------------------------------------------
        -- FUNCTION_BODY EXTRACTION (SQL + PLPGSQL, DECLARE + BEGIN, No END)
        --------------------------------------------------------------------
        CASE
            WHEN l.lanname = 'sql' THEN
                regexp_replace(trim(p.prosrc), '\s+', ' ', 'g')
            WHEN l.lanname = 'plpgsql' THEN
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            pg_get_functiondef(p.oid),
                            '.*?(DECLARE|BEGIN)(.*?)END;?\s*\$[^$]*\$.*$',
                            '\1\2',
                            'nsi'
                        ),
                        '\s*RETURN\s+[^;]+;?\s*$',
                        '',
                        'nsi'
                    ),
                    '\s+',
                    ' ',
                    'g'
                )
            ELSE trim(p.prosrc)
        END AS function_body


    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    JOIN pg_language l ON l.oid = p.prolang
    WHERE
        n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND (p_schema_list IS NULL OR n.nspname = ANY(p_schema_list))
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/object_type_functions/get_functions_details.sql'

-- SELECT * FROM pdcd_schema.get_functions_details(ARRAY['analytics_schema']);