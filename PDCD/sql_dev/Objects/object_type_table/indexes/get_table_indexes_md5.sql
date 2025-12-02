
-- update function to include Indexes on Views and Materialized Views

CREATE OR REPLACE FUNCTION pdcd_schema.get_table_indexes_md5(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
    object_subtype TEXT,
    object_subtype_name TEXT,
    object_subtype_details TEXT,
    object_md5 TEXT
)
LANGUAGE sql
AS $function$
SELECT
    gid.schema_name,
    gid.object_type,
    gid.object_type_name,
    'Index' AS object_subtype,
    gid.index_name AS object_subtype_name,

    concat_ws(
        ',',
        'tablespace:' || COALESCE(gid.tablespace,''),
        'indexdef:' || COALESCE(gid.indexdef,''),
        'is_unique:' || COALESCE(gid.is_unique::TEXT,''),
        'is_primary:' || COALESCE(gid.is_primary::TEXT,''),
        'index_columns:' || COALESCE(gid.index_columns,''),
        'index_predicate:' || COALESCE(gid.index_predicate,''),
        'access_method:' || COALESCE(gid.access_method,'')
    ) AS object_subtype_details,

    md5(
        concat_ws(
            ':',
            'object_type:' || gid.object_type,
            'object_name:' || gid.object_type_name,
            'index_name:' || gid.index_name,
            'tablespace:' || COALESCE(gid.tablespace,''),
            'is_unique:' || COALESCE(gid.is_unique::TEXT,''),
            'is_primary:' || COALESCE(gid.is_primary::TEXT,''),
            'index_columns:' || COALESCE(gid.index_columns,''),
            'index_predicate:' || COALESCE(gid.index_predicate,''),
            'access_method:' || COALESCE(gid.access_method,'')
        )
    ) AS object_md5

FROM pdcd_schema.get_index_details(p_table_list) gid
ORDER BY gid.schema_name, gid.object_type_name, gid.index_name;
$function$;

-- select * from pdcd_schema.get_table_indexes_md5();


-- \i '/Users/manoj_anumalla/Desktop/PDCD/PDCD/sql_dev/Objects/table_objects/indexes/get_table_indexes_md5.sql'

-- drop function get_table_indexes_md5(TEXT[]);
-- SELECT * FROM pdcd_schema.get_table_indexes_md5(ARRAY['sales','hr']);