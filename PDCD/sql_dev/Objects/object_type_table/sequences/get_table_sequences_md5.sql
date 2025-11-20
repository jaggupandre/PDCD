CREATE OR REPLACE FUNCTION pdcd_schema.get_table_sequences_md5(
    p_table_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
    object_subtype TEXT,          -- sequence
    object_subtype_name TEXT,     -- sequence_name
    object_subtype_details TEXT,  -- metadata string
    object_md5 TEXT               -- MD5 hash of details
)
LANGUAGE SQL
AS $function$
    SELECT
        gtd.schema_name,
        'Table' AS object_type,
        gtd.table_name AS object_type_name,
        'Sequence' AS object_subtype,
        gtd.sequence_name AS object_subtype_name,
        -- Build sequence details for tracking changes
        -- gtd.object_subtype_details AS object_subtype_details,
        CONCAT_WS(
            ',',
            CONCAT('owned_by:', COALESCE(gtd.owned_by, '')),
            CONCAT('sequence_type:', COALESCE(gtd.sequence_type, '')),
            CONCAT('privileges:', COALESCE(gtd.privileges, '')),
            CONCAT('data_type:', COALESCE(gtd.data_type, '')),
            CONCAT('start_value:', COALESCE(gtd.start_value::TEXT, '')),
            CONCAT('minimum_value:', COALESCE(gtd.minimum_value::TEXT, '')),
            CONCAT('maximum_value:', COALESCE(gtd.maximum_value::TEXT, '')),
            CONCAT('increment_by:', COALESCE(gtd.increment_by::TEXT, '')),
            CONCAT('cycle_option:', COALESCE(gtd.cycle_option::TEXT, '')),
            CONCAT('cache_size:', COALESCE(gtd.cache_size::TEXT, ''))
        ) AS object_subtype_details,

        -- Create MD5 hash from normalized concatenated string
        -- MD5(gtd.object_subtype_details) AS object_md5
        MD5(
            CONCAT_WS(
                ',',
                CONCAT('owned_by:', COALESCE(gtd.owned_by, '')),
                CONCAT('sequence_type:', COALESCE(gtd.sequence_type, '')),
                CONCAT('privileges:', COALESCE(gtd.privileges, '')),
                CONCAT('data_type:', COALESCE(gtd.data_type, '')),
                CONCAT('start_value:', COALESCE(gtd.start_value::TEXT, '')),
                CONCAT('minimum_value:', COALESCE(gtd.minimum_value::TEXT, '')),
                CONCAT('maximum_value:', COALESCE(gtd.maximum_value::TEXT, '')),
                CONCAT('increment_by:', COALESCE(gtd.increment_by::TEXT, '')),
                CONCAT('cycle_option:', COALESCE(gtd.cycle_option::TEXT, '')),
                CONCAT('cache_size:', COALESCE(gtd.cache_size::TEXT, ''))
            )
        ) AS object_md5

    FROM pdcd_schema.get_sequence_details(p_table_list) AS gtd
    ORDER BY gtd.schema_name, gtd.table_name;
$function$;

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/Objects/table_objects/sequences/get_table_sequences_md5.sql'
-- SElECT * FROM pdcd_schema.get_table_sequences_md5(ARRAY['analytics_schema']);