--=================================================
-- Table: pdcd_schema.md5_metadata_staging_tbl
--=================================================
-- DROP TABLE IF EXISTS pdcd_schema.md5_metadata_staging_tbl;
-- CREATE TABLE pdcd_schema.md5_metadata_staging_tbl (
--   metadata_id BIGSERIAL PRIMARY KEY,
--   snapshot_id INT NOT NULL REFERENCES pdcd_schema.snapshot_tbl(snapshot_id) ON DELETE CASCADE,
--   schema_name TEXT NOT NULL,
--   object_type TEXT NOT NULL,
--   object_type_name TEXT NOT NULL,
--   object_subtype TEXT,
--   object_subtype_name TEXT,
--   object_subtype_details TEXT,
--   object_md5 TEXT NOT NULL,
--   processed_time TIMESTAMP DEFAULT clock_timestamp()
-- );

--=================================================
-- Function: pdcd_schema.load_md5_metadata_staging_tbl
--=================================================
-- drop table pdcd_schema.md5_metadata_staging_tbl;

CREATE OR REPLACE FUNCTION pdcd_schema.load_md5_metadata_staging_tbl(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    metadata_id BIGINT,
    snapshot_id INTEGER,
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
    object_subtype TEXT,
    object_subtype_name TEXT,
    object_subtype_details TEXT,
    object_md5 TEXT,
    processed_time TIMESTAMP
)
LANGUAGE SQL
AS $function$
    WITH new_snapshot AS (
        SELECT snapshot_id, processed_time
        FROM pdcd_schema.snapshot_tbl
        ORDER BY snapshot_id DESC
        LIMIT 1   -- only latest snapshot
    ),
    combined_data AS (
        SELECT DISTINCT * FROM pdcd_schema.get_table_columns_md5(p_table_list)
        UNION ALL
        SELECT DISTINCT * FROM pdcd_schema.get_table_constraints_md5(p_table_list)
        UNION ALL
        SELECT DISTINCT * FROM pdcd_schema.get_table_indexes_md5(p_table_list)
        UNION ALL
        SELECT DISTINCT * FROM pdcd_schema.get_table_references_md5(p_table_list)
    ),
    inserted AS (
        INSERT INTO pdcd_schema.md5_metadata_staging_tbl (
            snapshot_id,
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5
        )
        SELECT
            ns.snapshot_id,
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            c.object_subtype_name,
            c.object_subtype_details,
            c.object_md5
        FROM combined_data c
        CROSS JOIN new_snapshot ns
        RETURNING metadata_id, snapshot_id, schema_name, object_type, object_type_name,
                  object_subtype, object_subtype_name, object_subtype_details,
                  object_md5
    )
    SELECT
        i.metadata_id,
        i.snapshot_id,
        i.schema_name,
        i.object_type,
        i.object_type_name,
        i.object_subtype,
        i.object_subtype_name,
        i.object_subtype_details,
        i.object_md5,
        ns.processed_time
    FROM inserted i
    JOIN new_snapshot ns ON TRUE
    ORDER BY i.schema_name, i.object_type_name, i.object_subtype_name;
$function$;


-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/load_compare/load_md5_metadata_staging_tbl.sql'
-- SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['analytics_schema']);