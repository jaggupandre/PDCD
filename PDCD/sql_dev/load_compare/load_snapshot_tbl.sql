-- Function to create a new snapshot, load_snapshot_tbl
CREATE OR REPLACE FUNCTION pdcd_schema.load_snapshot_tbl()
RETURNS TABLE (
    snapshot_id INT,
    snapshot_name TEXT,
    processed_time TIMESTAMP
)
LANGUAGE SQL
AS $function$
    INSERT INTO pdcd_schema.snapshot_tbl (snapshot_name)
    VALUES (
        CONCAT_WS('_',
            'snapshot',
            COALESCE((SELECT MAX(snapshot_id) FROM pdcd_schema.snapshot_tbl), 0) + 1,
            TO_CHAR(clock_timestamp(), 'YYYY_MM_DD_HH24MISS')
        )
    )
    RETURNING snapshot_id, snapshot_name, processed_time;
$function$;


-- \i '/Users/jagdish_pandre/PDCD/sql_dev/Objects/load_snapshot_tbl.sql'