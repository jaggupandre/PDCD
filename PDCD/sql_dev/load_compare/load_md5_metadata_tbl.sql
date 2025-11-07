--=================================================
-- Table: pdcd_schema.md5_metadata_tbl
--=================================================
-- DROP TABLE IF EXISTS pdcd_schema.md5_metadata_tbl;
-- CREATE TABLE pdcd_schema.md5_metadata_tbl (
--   metadata_id BIGSERIAL PRIMARY KEY,
--   snapshot_id INT NOT NULL REFERENCES pdcd_schema.snapshot_tbl(snapshot_id) ON DELETE CASCADE,
--   schema_name TEXT NOT NULL,
--   object_type TEXT NOT NULL,          -- TABLE, VIEW, FUNCTION, ...
--   object_type_name TEXT NOT NULL,     -- table/view name
--   object_subtype TEXT,                -- Column, Index, Trigger, ...
--   object_subtype_name TEXT,           -- column name or index name
--   object_subtype_details TEXT,        -- raw detail string
--   object_md5 TEXT NOT NULL,           -- md5 fingerprint of the object_subtype_details (or full row)
--   processed_time TIMESTAMP DEFAULT clock_timestamp(),
--   change_type TEXT DEFAULT 'ADDED'   -- ADDED | MODIFIED | UNCHANGED | DELETED (we'll use ADDED/MODIFIED/DELETED on insert)
-- );

-- Main metadata load function, load_md5_metadata_tbl
CREATE OR REPLACE FUNCTION pdcd_schema.load_md5_metadata_tbl(
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
    processed_time TIMESTAMP,
    change_type TEXT
)
LANGUAGE SQL
AS $function$
    WITH new_snapshot AS (
        SELECT MAX(snapshot_id) as snapshot_id
        FROM pdcd_schema.snapshot_tbl
    ),
    combined_data AS (

        SELECT * FROM pdcd_schema.get_table_columns_md5(p_table_list)
    ),
    inserted AS (
        INSERT INTO pdcd_schema.md5_metadata_tbl (
            snapshot_id,
            schema_name,
            object_type,
            object_type_name,
            object_subtype,
            object_subtype_name,
            object_subtype_details,
            object_md5,
            change_type
        )
        SELECT
            ns.snapshot_id,
            c.schema_name,
            c.object_type,
            c.object_type_name,
            c.object_subtype,
            c.object_subtype_name,
            c.object_subtype_details,
            c.object_md5,
            'ADDED'
        FROM combined_data c
        CROSS JOIN new_snapshot ns
        RETURNING metadata_id, snapshot_id, schema_name, object_type, object_type_name,
                  object_subtype, object_subtype_name, object_subtype_details,
                  object_md5, change_type
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
        clock_timestamp() AS processed_time,
        i.change_type
    FROM inserted i
    JOIN new_snapshot ns ON TRUE
    ORDER BY i.schema_name, i.object_type_name, i.object_subtype;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/load_compare/load_md5_metadata_tbl.sql'


-- md5_metadata_tbl

-- SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);

-- SELECT * FROM pdcd_schema.load_snapshot_tbl();
-- SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);