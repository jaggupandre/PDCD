CREATE OR REPLACE FUNCTION pdcd_schema.get_trigger_details(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    object_type TEXT,         -- Table / View
    object_type_name TEXT,    -- table or view name
    trigger_name TEXT,
    trigger_definition TEXT,
    trigger_event TEXT,       -- INSERT / UPDATE / DELETE / TRUNCATE
    trigger_timing TEXT,      -- BEFORE / AFTER / INSTEAD OF
    trigger_level TEXT,       -- ROW / STATEMENT
    trigger_enabled BOOLEAN,  -- TRUE = enabled / FALSE = disabled
    trigger_function_name TEXT,
    trigger_function_arguments TEXT,
    trigger_function_definition TEXT
)
LANGUAGE plpgsql
AS $function$
DECLARE
    v_all_objects TEXT[];
BEGIN
    ------------------------------------------------------------------------
    -- Resolve input list into fully-qualified schema.object list
    -- Supports: tables (relkind = 'r') and views (relkind = 'v')
    ------------------------------------------------------------------------
    IF p_table_list IS NULL OR array_length(p_table_list, 1) IS NULL THEN
        -- CASE 1: No input → all user tables + views
        SELECT array_agg(n.nspname || '.' || c.relname)
        INTO v_all_objects
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','v')
          AND n.nspname NOT IN ('pg_catalog','information_schema');

    ELSIF NOT EXISTS (
        SELECT 1 FROM unnest(p_table_list) t WHERE position('.' IN t) > 0
    ) THEN
        -- CASE 2: schema-only list
        SELECT array_agg(n.nspname || '.' || c.relname)
        INTO v_all_objects
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','v')
          AND n.nspname = ANY(p_table_list);

    ELSE
        -- CASE 3: schema.object list
        v_all_objects := p_table_list;
    END IF;

    ------------------------------------------------------------------------
    -- Return trigger details (for both tables and views)
    ------------------------------------------------------------------------
    RETURN QUERY
    SELECT
        n.nspname::TEXT AS schema_name,

        CASE 
            WHEN c.relkind = 'r' THEN 'Table'
            WHEN c.relkind = 'v' THEN 'View'
            ELSE 'Other'
        END AS object_type,

        c.relname::TEXT AS object_type_name,
        t.tgname::TEXT  AS trigger_name,
        pg_get_triggerdef(t.oid)::TEXT AS trigger_definition,

        --------------------------------------------------------------------
        -- EVENT LIST (INSERT / UPDATE / DELETE / TRUNCATE)
        --------------------------------------------------------------------
        (
            SELECT string_agg(event, ',')
            FROM (
                SELECT CASE WHEN (t.tgtype & 4)  <> 0 THEN 'INSERT'  END
                UNION ALL
                SELECT CASE WHEN (t.tgtype & 8)  <> 0 THEN 'DELETE'  END
                UNION ALL
                SELECT CASE WHEN (t.tgtype & 16) <> 0 THEN 'UPDATE'  END
                UNION ALL
                SELECT CASE WHEN (t.tgtype & 32) <> 0 THEN 'TRUNCATE' END
            ) AS events(event)
            WHERE event IS NOT NULL
        ) AS trigger_event,

        --------------------------------------------------------------------
        -- TIMING (BEFORE / AFTER / INSTEAD OF)
        --------------------------------------------------------------------
        CASE
            WHEN (t.tgtype & 2)  <> 0 THEN 'BEFORE'
            WHEN (t.tgtype & 64) <> 0 THEN 'INSTEAD OF'
            ELSE 'AFTER'
        END AS trigger_timing,

        --------------------------------------------------------------------
        -- LEVEL (ROW / STATEMENT)
        --------------------------------------------------------------------
        CASE
            WHEN (t.tgtype & 1) <> 0 THEN 'ROW'
            ELSE 'STATEMENT'
        END AS trigger_level,

        --------------------------------------------------------------------
        -- Enabled flag
        --------------------------------------------------------------------
        (t.tgenabled <> 'D') AS trigger_enabled,

        --------------------------------------------------------------------
        -- Function name + arguments + full definition
        --------------------------------------------------------------------
        p.proname::TEXT                      AS trigger_function_name,
        pg_get_function_arguments(p.oid)::TEXT AS trigger_function_arguments,
        pg_get_functiondef(p.oid)::TEXT      AS trigger_function_definition

    FROM pg_trigger t
    JOIN pg_class c     ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_proc p      ON p.oid = t.tgfoid
    WHERE t.tgisinternal = false
      AND (n.nspname || '.' || c.relname) = ANY(v_all_objects)
    ORDER BY n.nspname, c.relname, t.tgname;

END;
$function$;

-- SELECT * FROM get_trigger_details(ARRAY['sales','hr']);