CREATE OR REPLACE FUNCTION pdcd_schema.get_sequence_details(
    p_sequence_list TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    schema_name TEXT,
    sequence_name TEXT,
    table_name TEXT,
    -- column_name TEXT,
    owned_by TEXT,
    sequence_type TEXT,
    -- owner_role TEXT,
    privileges TEXT,
    data_type TEXT,
    start_value BIGINT,
    minimum_value BIGINT,
    maximum_value BIGINT,
    increment_by BIGINT,
    cycle_option TEXT,
    cache_size BIGINT
)
LANGUAGE SQL
AS $function$
WITH seqs AS (
    SELECT 
        n.nspname AS schema_name,
        c.relname AS sequence_name,
        s.seqstart AS start_value,
        s.seqmin AS minimum_value,
        s.seqmax AS maximum_value,
        s.seqincrement AS increment_by,
        s.seqcycle AS cycle_bool,
        s.seqcache AS cache_size,
        s.seqtypid,
        c.oid AS seq_oid,
        c.relowner,
        c.relacl
    FROM pg_sequence s
    JOIN pg_class c ON c.oid = s.seqrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'S'
),
deps AS (
    SELECT 
        d.objid AS seq_oid,
        t.relname AS table_name,
        a.attname AS column_name,
        tn.nspname AS table_schema,
        d.deptype
    FROM pg_depend d
    JOIN pg_class t ON t.oid = d.refobjid
    JOIN pg_namespace tn ON tn.oid = t.relnamespace
    LEFT JOIN pg_attribute a 
           ON a.attrelid = t.oid
          AND a.attnum = d.refobjsubid
    WHERE d.classid = 'pg_class'::regclass
      AND d.refclassid = 'pg_class'::regclass
)
SELECT
    seq.schema_name,
    seq.sequence_name,
    dep.table_name,
    -- dep.column_name,
    CASE 
        WHEN dep.table_name IS NOT NULL AND dep.column_name IS NOT NULL THEN
            format('%I.%I.%I', dep.table_schema, dep.table_name, dep.column_name)
        ELSE NULL
    END AS owned_by,

    CASE
        WHEN dep.deptype = 'i' THEN 'IDENTITY'
        WHEN dep.deptype = 'a' THEN 'SERIAL'
        ELSE 'MANUAL'
    END AS sequence_type,

    -- pg_catalog.pg_get_userbyid(seq.relowner) AS owner_role,
    seq.relacl::TEXT AS privileges,
    pg_catalog.format_type(seq.seqtypid, NULL) AS data_type,

    seq.start_value,
    seq.minimum_value,
    seq.maximum_value,
    seq.increment_by,
    CASE WHEN seq.cycle_bool THEN 'YES' ELSE 'NO' END AS cycle_option,
    seq.cache_size

FROM seqs seq
LEFT JOIN deps dep ON dep.seq_oid = seq.seq_oid

WHERE
    (
        p_sequence_list IS NULL
        AND seq.schema_name NOT IN ('pg_catalog', 'information_schema')
    )
    OR (
        p_sequence_list IS NOT NULL
        AND seq.schema_name = ANY(p_sequence_list)
    )
    OR (
        p_sequence_list IS NOT NULL
        AND (seq.schema_name || '.' || seq.sequence_name) = ANY(p_sequence_list)
    )

ORDER BY seq.schema_name, seq.sequence_name;
$function$;



-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/sequences/get_sequences_details.sql'
-- SElECT * FROM pdcd_schema.get_sequence_details(ARRAY['analytics_schema']);
--         SELECT 
--        s.*
--         FROM pg_sequence s
--         JOIN pg_class c ON c.oid = s.seqrelid
--         JOIN pg_namespace n ON n.oid = c.relnamespace
--         WHERE c.relkind = 'S'
--         AND n.nspname = 'analytics_schema';
-- seqrelid | seqtypid | seqstart | seqincrement |   seqmax   | seqmin | seqcache | seqcycle |  oid   |            relname            | relnamespace | reltype | reloftype | relowner | relam | relfilenode | reltablespace | relpages | reltuples | relallvisible | reltoastrelid | relhasindex | relisshared | relpersistence | relkind | relnatts | relchecks | relhasrules | relhastriggers | relhassubclass | relrowsecurity | relforcerowsecurity | relispopulated | relreplident | relispartition | relrewrite | relfrozenxid | relminmxid | relacl | reloptions | relpartbound |  oid   |     nspname      | nspowner | nspacl
-- ----------+----------+----------+--------------+------------+--------+----------+----------+--------+-------------------------------+--------------+---------+-----------+----------+-------+-------------+---------------+----------+-----------+---------------+---------------+-------------+-------------+----------------+---------+----------+-----------+-------------+----------------+----------------+----------------+---------------------+----------------+--------------+----------------+------------+--------------+------------+--------+------------+--------------+--------+------------------+----------+--------
--    167698 |       23 |        1 |            1 | 2147483647 |      1 |        1 | f        | 167698 | departments_department_id_seq |       167694 |       0 |         0 |       10 |     0 |      167698 |             0 |        1 |         1 |             0 |             0 | f           | f           | p              | S       |        3 |         0 | f           | f              | f              | f              | f                   | t              | n            | f              |          0 |            0 |          0 |        |            |              | 167694 | analytics_schema |       10 |
-- (1 row)