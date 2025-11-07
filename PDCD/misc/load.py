import psycopg2
import hashlib
from datetime import datetime

def connect_db():
    return psycopg2.connect(dbname='test_db', user='jagdish_pandre', password='', host='localhost')

def md5_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()
import psycopg2

def insert_column_base_metadata(conn, snapshot_id):
    sql = """
    INSERT INTO pdcd_schema.md5_metadata_table (
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
        %s AS snapshot_id,
        c.table_schema AS schema_name,
        'Table' AS object_type,
        c.table_name AS object_type_name,
        'Column' AS object_subtype,
        c.column_name AS object_subtype_name,

        (
            'schema_name:' || COALESCE(c.table_schema, '') ||
            ',table_name:' || COALESCE(c.table_name, '') ||
            ',column_name:' || COALESCE(c.column_name, '') ||
            ',data_type:' || COALESCE(c.data_type, '') ||
            ',max_length:' || COALESCE(c.character_maximum_length::TEXT, '') ||
            ',numeric_precision:' || COALESCE(c.numeric_precision::TEXT, '') ||
            ',numeric_scale:' || COALESCE(c.numeric_scale::TEXT, '') ||
            ',nullable:' || COALESCE(c.is_nullable, '') ||
            ',default_value:' || COALESCE(c.column_default, '') ||
            ',is_identity:' || COALESCE(c.is_identity, '') ||
            ',is_generated:' || COALESCE(c.is_generated, '') ||
            ',generation_expression:' || COALESCE(c.generation_expression, '') ||
            ',constraint_name:' || COALESCE(tc.constraint_name, '') ||
            ',column_position:' || c.ordinal_position::TEXT
        ) AS object_subtype_details,

        md5(
            'schema_name:' || COALESCE(c.table_schema, '') ||
            ',table_name:' || COALESCE(c.table_name, '') ||
            ',column_name:' || COALESCE(c.column_name, '') ||
            ',data_type:' || COALESCE(c.data_type, '') ||
            ',max_length:' || COALESCE(c.character_maximum_length::TEXT, '') ||
            ',numeric_precision:' || COALESCE(c.numeric_precision::TEXT, '') ||
            ',numeric_scale:' || COALESCE(c.numeric_scale::TEXT, '') ||
            ',nullable:' || COALESCE(c.is_nullable, '') ||
            ',default_value:' || COALESCE(c.column_default, '') ||
            ',is_identity:' || COALESCE(c.is_identity, '') ||
            ',is_generated:' || COALESCE(c.is_generated, '') ||
            ',generation_expression:' || COALESCE(c.generation_expression, '') ||
            ',constraint_name:' || COALESCE(tc.constraint_name, '') ||
            ',column_position:' || c.ordinal_position::TEXT
        ) AS object_md5
    FROM information_schema.columns c
    LEFT JOIN information_schema.key_column_usage kcu
        ON c.table_catalog = kcu.table_catalog
        AND c.table_schema = kcu.table_schema
        AND c.table_name = kcu.table_name
        AND c.column_name = kcu.column_name
    LEFT JOIN information_schema.table_constraints tc 
        ON kcu.table_catalog = tc.table_catalog
        AND kcu.table_schema = tc.table_schema
        AND kcu.table_name = tc.table_name
        AND kcu.constraint_name = tc.constraint_name
        AND tc.constraint_type = 'PRIMARY KEY'
    WHERE c.table_schema IN ('analytics_schema')
    ;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (snapshot_id,))
    conn.commit()

    sql = """
    INSERT INTO pdcd_schema.md5_metadata_table (
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
        %s as snapshot_id,
        g.schema_name,
        'Table' AS object_type,
        g.object_name,
        'Columns' AS object_subtype,
        array_to_string(array_agg(g.object_subtype_name ORDER BY g.ordinal_position), ', ') AS object_subtype_name,
        NULL AS object_subtype_details,
        md5(string_agg(g.object_md5, '' ORDER BY g.ordinal_position)) AS object_md5
    FROM (
        SELECT
            c.table_schema AS schema_name,
            c.table_name AS object_name,
            c.column_name AS object_subtype_name,
            c.ordinal_position,
            md5(
                'schema_name:' || COALESCE(c.table_schema, '') ||
                ',table_name:' || COALESCE(c.table_name, '') ||
                ',column_name:' || COALESCE(c.column_name, '') ||
                ',data_type:' || COALESCE(c.data_type, '') ||
                ',max_length:' || COALESCE(c.character_maximum_length::TEXT, '') ||
                ',numeric_precision:' || COALESCE(c.numeric_precision::TEXT, '') ||
                ',numeric_scale:' || COALESCE(c.numeric_scale::TEXT, '') ||
                ',nullable:' || COALESCE(c.is_nullable, '') ||
                ',default_value:' || COALESCE(c.column_default, '') ||
                ',is_identity:' || COALESCE(c.is_identity, '') ||
                ',is_generated:' || COALESCE(c.is_generated, '') ||
                ',generation_expression:' || COALESCE(c.generation_expression, '')
            ) AS object_md5
        FROM information_schema.columns c
        WHERE c.table_schema IN ('analytics_schema')
    ) g
    GROUP BY g.schema_name, g.object_name;
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, (snapshot_id,))
    conn.commit()
# 
def insert_table_columns_aggregate(conn, snapshot_id):
    sql = """
    INSERT INTO pdcd_schema.md5_metadata_table (
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
        %s as snapshot_id,
        c.table_schema AS schema_name,
        'Table' AS object_type,
        c.table_name AS object_type_name,
        'Columns' AS object_subtype,
        string_agg(c.column_name, ', ' ORDER BY c.ordinal_position) AS object_subtype_name,
        NULL AS object_subtype_details,
        md5(string_agg(
            md5(
                'schema_name:' || COALESCE(c.table_schema, '') || 
                ',table_name:' || COALESCE(c.table_name, '') || 
                ',column_name:' || COALESCE(c.column_name, '') || 
                ',data_type:' || COALESCE(c.data_type, '') || 
                ',max_length:' || COALESCE(c.character_maximum_length::TEXT, '') || 
                ',numeric_precision:' || COALESCE(c.numeric_precision::TEXT, '') || 
                ',numeric_scale:' || COALESCE(c.numeric_scale::TEXT, '') || 
                ',nullable:' || COALESCE(c.is_nullable, '') || 
                ',default_value:' || COALESCE(c.column_default, '') || 
                ',is_identity:' || COALESCE(c.is_identity, '') || 
                ',is_generated:' || COALESCE(c.is_generated, '') || 
                ',generation_expression:' || COALESCE(c.generation_expression, '')
            )::text, '' ORDER BY c.ordinal_position
        )) AS object_md5
    FROM information_schema.columns c
    WHERE c.table_schema IN ('analytics_schema')
    GROUP BY c.table_schema, c.table_name
    ;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (snapshot_id,))
    conn.commit()
# 
def insert_snapshot(conn, snapshot_name):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pdcd_schema.snapshot_tbl (snapshot_name)
            VALUES (%s) RETURNING snapshot_id
        """, (snapshot_name,))
        snapshot_id = cur.fetchone()[0]
    conn.commit()
    return snapshot_id

def extract_current_metadata(conn):
    # Replace this query with your actual metadata extraction logic
    query = """
    SELECT 
        schema_name,
        object_type,
        object_type_name,
        object_subtype,
        object_subtype_name,
        object_subtype_details
    FROM your_metadata_source
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    current_meta = []
    for row in rows:
        details = row[5]
        hash_value = md5_hash(details)
        current_meta.append({
            'schema_name': row[0],
            'object_type': row[1],
            'object_type_name': row[2],
            'object_subtype': row[3],
            'object_subtype_name': row[4],
            'object_subtype_details': details,
            'object_md5': hash_value
        })
    return current_meta

def get_last_snapshot_metadata(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT snapshot_id FROM pdcd_schema.snapshot_tbl
            ORDER BY snapshot_process_time DESC LIMIT 1
        """)
        last_snapshot = cur.fetchone()
        if not last_snapshot:
            return None, {}
        last_snapshot_id = last_snapshot[0]

        cur.execute("""
            SELECT schema_name, object_type, object_type_name, object_subtype, object_subtype_name, object_subtype_details, object_md5
            FROM pdcd_schema.md5_metadata_table
            WHERE snapshot_id = %s
        """, (last_snapshot_id,))
        rows = cur.fetchall()
        last_meta_dict = { 
            (r[0], r[1], r[2], r[3], r[4]): {'details': r[5], 'md5': r[6]} for r in rows 
        }
    return last_snapshot_id, last_meta_dict

def compute_diffs(current_meta, last_meta_dict):
    new_keys = set([(m['schema_name'], m['object_type'], m['object_type_name'], m['object_subtype'], m['object_subtype_name']) for m in current_meta])
    last_keys = set(last_meta_dict.keys())

    diffs = []

    # Additions and Modifications
    for obj in current_meta:
        key = (obj['schema_name'], obj['object_type'], obj['object_type_name'], obj['object_subtype'], obj['object_subtype_name'])
        if key not in last_keys:
            diffs.append((obj, 'ADDED'))
        elif last_meta_dict[key]['md5'] != obj['object_md5']:
            diffs.append((obj, 'MODIFIED'))

    # Deletions
    for key in last_keys - new_keys:
        obj = {
            'schema_name': key[0],
            'object_type': key[1],
            'object_type_name': key[2],
            'object_subtype': key[3],
            'object_subtype_name': key[4],
            'object_subtype_details': last_meta_dict[key]['details'],
            'object_md5': last_meta_dict[key]['md5']
        }
        diffs.append((obj, 'DELETED'))

    return diffs

def insert_differences(conn, snapshot_id, diffs):
    with conn.cursor() as cur:
        for obj, change_type in diffs:
            cur.execute("""
                INSERT INTO pdcd_schema.md5_metadata_table (
                    snapshot_id, schema_name, object_type, object_type_name,
                    object_subtype, object_subtype_name, object_subtype_details,
                    object_md5, processed_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, clock_timestamp())
            """, (
                snapshot_id,
                obj['schema_name'], obj['object_type'], obj['object_type_name'],
                obj['object_subtype'], obj['object_subtype_name'], obj['object_subtype_details'],
                obj['object_md5']
            ))
    conn.commit()

def main(snapshot_name):
    conn = connect_db()
    try:
        # Insert new snapshot record
        # snapshot_id = insert_snapshot(conn, snapshot_name)
        # Insert base column metadata
        # insert_column_base_metadata(conn, snapshot_id)
        # Insert aggregated column table metadata
        insert_table_columns_aggregate(conn, snapshot_id = 1)
        # Extract latest metadata
        # current_meta = extract_current_metadata(conn)
        # Get previous snapshot metadata
        # last_snapshot_id, last_meta_dict = get_last_snapshot_metadata(conn)
        # Compute differences
        # diffs = compute_diffs(current_meta, last_meta_dict if last_meta_dict else {})
        # Insert only differences for current snapshot
        # insert_differences(conn, snapshot_id, diffs)
        # print(f"Snapshot {snapshot_id} saved: {len(diffs)} changes detected.")
        # print(f"Snapshot {snapshot_id} saved.")
    finally:
        conn.close()

if __name__ == "__main__":
    main("snapshot_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
