"""
Schema Change Report Generator
Jupyter Notebook for analyzing PostgreSQL schema changes between two timestamps
"""

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set plot style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

# ============================================
# DATABASE CONNECTION CONFIGURATION
# ============================================

DB_CONFIG = {
    'host': 'localhost',
    'database': 'your_database',
    'user': 'your_username',
    'password': 'your_password',
    'port': 5432
}

# ============================================
# TIME RANGE PARAMETERS - MODIFY THESE
# ============================================

START_TIME = '2025-11-12 14:30:00'
END_TIME = '2025-11-12 14:36:00'

# ============================================
# DATABASE CONNECTION
# ============================================

def get_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Database connection successful")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

conn = get_connection()

# ============================================
# 1. EXECUTIVE SUMMARY
# ============================================

print("="*80)
print("SCHEMA CHANGE REPORT")
print(f"Period: {START_TIME} to {END_TIME}")
print("="*80)

query_executive_summary = """
SELECT 
    COUNT(DISTINCT snapshot_id) as total_snapshots,
    COUNT(DISTINCT schema_name) as schemas_affected,
    COUNT(DISTINCT object_type_name) as tables_affected,
    COUNT(DISTINCT object_subtype_name) as columns_affected,
    COUNT(*) as total_changes,
    MIN(processed_time) as first_change,
    MAX(processed_time) as last_change
FROM md5_metadata_tbl
WHERE processed_time BETWEEN %s AND %s;
"""

df_summary = pd.read_sql_query(query_executive_summary, conn, params=(START_TIME, END_TIME))

print("\n📊 EXECUTIVE SUMMARY")
print("-" * 80)
for col in df_summary.columns:
    print(f"{col.replace('_', ' ').title()}: {df_summary[col].iloc[0]}")

# ============================================
# 2. CHANGE TYPE BREAKDOWN
# ============================================

query_change_breakdown = """
SELECT 
    change_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM md5_metadata_tbl
WHERE processed_time BETWEEN %s AND %s
GROUP BY change_type
ORDER BY count DESC;
"""

df_change_types = pd.read_sql_query(query_change_breakdown, conn, params=(START_TIME, END_TIME))

print("\n📈 CHANGE TYPE BREAKDOWN")
print("-" * 80)
print(df_change_types.to_string(index=False))

# Visualization
if not df_change_types.empty:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Bar chart
    ax1.bar(df_change_types['change_type'], df_change_types['count'], color='steelblue')
    ax1.set_xlabel('Change Type')
    ax1.set_ylabel('Count')
    ax1.set_title('Changes by Type')
    ax1.tick_params(axis='x', rotation=45)
    
    # Pie chart
    colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99']
    ax2.pie(df_change_types['count'], labels=df_change_types['change_type'], 
            autopct='%1.1f%%', colors=colors, startangle=90)
    ax2.set_title('Change Distribution')
    
    plt.tight_layout()
    plt.show()

# ============================================
# 3. CHANGES BY TABLE
# ============================================

query_changes_by_table = """
SELECT 
    schema_name,
    object_type_name as table_name,
    COUNT(*) as total_changes,
    COUNT(*) FILTER (WHERE change_type = 'ADDED') as added,
    COUNT(*) FILTER (WHERE change_type = 'MODIFIED') as modified,
    COUNT(*) FILTER (WHERE change_type = 'DELETED') as deleted,
    COUNT(*) FILTER (WHERE change_type = 'RENAMED') as renamed
FROM md5_metadata_tbl
WHERE processed_time BETWEEN %s AND %s
GROUP BY schema_name, object_type_name
ORDER BY total_changes DESC;
"""

df_by_table = pd.read_sql_query(query_changes_by_table, conn, params=(START_TIME, END_TIME))

print("\n📋 CHANGES BY TABLE")
print("-" * 80)
print(df_by_table.to_string(index=False))

# Visualization
if not df_by_table.empty:
    fig, ax = plt.subplots(figsize=(12, 6))
    df_plot = df_by_table.set_index('table_name')[['added', 'modified', 'deleted', 'renamed']]
    df_plot.plot(kind='bar', stacked=True, ax=ax, 
                 color=['#2ecc71', '#3498db', '#e74c3c', '#f39c12'])
    ax.set_xlabel('Table Name')
    ax.set_ylabel('Number of Changes')
    ax.set_title('Changes by Table (Stacked)')
    ax.legend(title='Change Type', bbox_to_anchor=(1.05, 1))
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

# ============================================
# 4. DETAILED COLUMN ADDITIONS
# ============================================

query_additions = """
SELECT 
    snapshot_id,
    processed_time,
    schema_name,
    object_type_name as table_name,
    object_subtype_name as column_name,
    REGEXP_REPLACE(object_subtype_details, '.*data_type:([^,]+).*', '\\1') as data_type,
    REGEXP_REPLACE(object_subtype_details, '.*max_length:([^,]*),.*', '\\1') as max_length,
    REGEXP_REPLACE(object_subtype_details, '.*nullable:([^,]+).*', '\\1') as nullable,
    REGEXP_REPLACE(object_subtype_details, '.*default_value:([^,]*),.*', '\\1') as default_value,
    REGEXP_REPLACE(object_subtype_details, '.*ordinal_position:([0-9]+).*', '\\1') as position
FROM md5_metadata_tbl
WHERE processed_time BETWEEN %s AND %s
    AND change_type = 'ADDED'
ORDER BY processed_time, schema_name, object_type_name, object_subtype_name;
"""

df_additions = pd.read_sql_query(query_additions, conn, params=(START_TIME, END_TIME))

print("\n➕ COLUMN ADDITIONS")
print("-" * 80)
if df_additions.empty:
    print("No columns added in this period.")
else:
    print(df_additions.to_string(index=False))

# ============================================
# 5. DETAILED COLUMN MODIFICATIONS
# ============================================

query_modifications = """
WITH current_changes AS (
    SELECT 
        snapshot_id,
        processed_time,
        schema_name,
        object_type_name,
        object_subtype_name,
        object_subtype_details,
        object_md5
    FROM md5_metadata_tbl
    WHERE processed_time BETWEEN %s AND %s
        AND change_type = 'MODIFIED'
),
previous_state AS (
    SELECT DISTINCT ON (cc.schema_name, cc.object_type_name, cc.object_subtype_name)
        cc.schema_name,
        cc.object_type_name,
        cc.object_subtype_name,
        m.object_subtype_details as old_details,
        m.processed_time as old_time
    FROM current_changes cc
    JOIN md5_metadata_tbl m 
        ON cc.schema_name = m.schema_name 
        AND cc.object_type_name = m.object_type_name
        AND cc.object_subtype_name = m.object_subtype_name
        AND m.processed_time < cc.processed_time
    ORDER BY cc.schema_name, cc.object_type_name, cc.object_subtype_name, m.processed_time DESC
)
SELECT 
    cc.snapshot_id,
    cc.processed_time,
    cc.schema_name,
    cc.object_type_name as table_name,
    cc.object_subtype_name as column_name,
    REGEXP_REPLACE(ps.old_details, '.*data_type:([^,]+).*', '\\1') as old_data_type,
    REGEXP_REPLACE(cc.object_subtype_details, '.*data_type:([^,]+).*', '\\1') as new_data_type,
    REGEXP_REPLACE(ps.old_details, '.*max_length:([^,]*),.*', '\\1') as old_max_length,
    REGEXP_REPLACE(cc.object_subtype_details, '.*max_length:([^,]*),.*', '\\1') as new_max_length,
    REGEXP_REPLACE(ps.old_details, '.*nullable:([^,]+).*', '\\1') as old_nullable,
    REGEXP_REPLACE(cc.object_subtype_details, '.*nullable:([^,]+).*', '\\1') as new_nullable
FROM current_changes cc
LEFT JOIN previous_state ps 
    ON cc.schema_name = ps.schema_name 
    AND cc.object_type_name = ps.object_type_name
    AND cc.object_subtype_name = ps.object_subtype_name
ORDER BY cc.processed_time, cc.schema_name, cc.object_type_name;
"""

df_modifications = pd.read_sql_query(query_modifications, conn, params=(START_TIME, END_TIME))

print("\n✏️ COLUMN MODIFICATIONS")
print("-" * 80)
if df_modifications.empty:
    print("No columns modified in this period.")
else:
    # Create readable change descriptions
    changes = []
    for _, row in df_modifications.iterrows():
        change_desc = []
        if row['old_data_type'] != row['new_data_type']:
            change_desc.append(f"Type: {row['old_data_type']} → {row['new_data_type']}")
        if row['old_max_length'] != row['new_max_length']:
            change_desc.append(f"Length: {row['old_max_length']} → {row['new_max_length']}")
        if row['old_nullable'] != row['new_nullable']:
            change_desc.append(f"Nullable: {row['old_nullable']} → {row['new_nullable']}")
        changes.append('; '.join(change_desc) if change_desc else 'Other changes')
    
    df_modifications['changes'] = changes
    print(df_modifications[['processed_time', 'table_name', 'column_name', 'changes']].to_string(index=False))

# ============================================
# 6. COLUMN DELETIONS
# ============================================

query_deletions = """
SELECT 
    snapshot_id,
    processed_time,
    schema_name,
    object_type_name as table_name,
    object_subtype_name as column_name,
    REGEXP_REPLACE(object_subtype_details, '.*data_type:([^,]+).*', '\\1') as data_type,
    REGEXP_REPLACE(object_subtype_details, '.*ordinal_position:([0-9]+).*', '\\1') as position
FROM md5_metadata_tbl
WHERE processed_time BETWEEN %s AND %s
    AND change_type = 'DELETED'
ORDER BY processed_time, schema_name, object_type_name;
"""

df_deletions = pd.read_sql_query(query_deletions, conn, params=(START_TIME, END_TIME))

print("\n❌ COLUMN DELETIONS")
print("-" * 80)
if df_deletions.empty:
    print("No columns deleted in this period.")
else:
    print(df_deletions.to_string(index=False))

# ============================================
# 7. COLUMN RENAMES
# ============================================

query_renames = """
SELECT 
    snapshot_id,
    processed_time,
    schema_name,
    object_type_name as table_name,
    object_subtype_name as new_name,
    object_md5
FROM md5_metadata_tbl
WHERE processed_time BETWEEN %s AND %s
    AND change_type = 'RENAMED'
ORDER BY processed_time, schema_name, object_type_name;
"""

df_renames = pd.read_sql_query(query_renames, conn, params=(START_TIME, END_TIME))

print("\n🔄 COLUMN RENAMES")
print("-" * 80)
if df_renames.empty:
    print("No columns renamed in this period.")
else:
    print(df_renames.to_string(index=False))

# ============================================
# 8. CHRONOLOGICAL TIMELINE (Enhanced with Details)
# ============================================

query_timeline = """
WITH timeline_data AS (
    SELECT 
        m1.snapshot_id,
        m1.processed_time,
        m1.schema_name,
        m1.object_type_name as table_name,
        m1.object_subtype_name as column_name,
        m1.change_type,
        m1.object_subtype_details as current_details,
        m1.object_md5,
        -- Get previous column name for renames
        (SELECT m2.object_subtype_name 
         FROM md5_metadata_tbl m2
         WHERE m2.schema_name = m1.schema_name
           AND m2.object_type_name = m1.object_type_name
           AND m2.object_md5 = m1.object_md5
           AND m2.processed_time < m1.processed_time
           AND m2.change_type != 'RENAMED'
         ORDER BY m2.processed_time DESC
         LIMIT 1
        ) as previous_column_name,
        -- Get previous details for modifications
        (SELECT m3.object_subtype_details
         FROM md5_metadata_tbl m3
         WHERE m3.schema_name = m1.schema_name
           AND m3.object_type_name = m1.object_type_name
           AND m3.object_subtype_name = m1.object_subtype_name
           AND m3.processed_time < m1.processed_time
         ORDER BY m3.processed_time DESC
         LIMIT 1
        ) as previous_details
    FROM md5_metadata_tbl m1
    WHERE m1.processed_time BETWEEN %s AND %s
)
SELECT 
    snapshot_id,
    processed_time,
    schema_name,
    table_name,
    column_name,
    change_type,
    previous_column_name,
    REGEXP_REPLACE(current_details, '.*data_type:([^,]+).*', '\\1') as current_data_type,
    REGEXP_REPLACE(previous_details, '.*data_type:([^,]+).*', '\\1') as previous_data_type,
    REGEXP_REPLACE(current_details, '.*max_length:([^,]*),.*', '\\1') as current_max_length,
    REGEXP_REPLACE(previous_details, '.*max_length:([^,]*),.*', '\\1') as previous_max_length,
    REGEXP_REPLACE(current_details, '.*nullable:([^,]+).*', '\\1') as current_nullable,
    REGEXP_REPLACE(previous_details, '.*nullable:([^,]+).*', '\\1') as previous_nullable
FROM timeline_data
ORDER BY processed_time, snapshot_id, schema_name, table_name;
"""

df_timeline = pd.read_sql_query(query_timeline, conn, params=(START_TIME, END_TIME))

print("\n📅 CHRONOLOGICAL TIMELINE")
print("-" * 80)
if df_timeline.empty:
    print("No changes in this period.")
else:
    # Create detailed descriptions
    descriptions = []
    for _, row in df_timeline.iterrows():
        if row['change_type'] == 'ADDED':
            desc = f"➕ Added: {row['column_name']} ({row['current_data_type']})"
        
        elif row['change_type'] == 'MODIFIED':
            changes = []
            # Check data type change
            if pd.notna(row['previous_data_type']) and row['previous_data_type'] != row['current_data_type']:
                changes.append(f"Type: {row['previous_data_type']} → {row['current_data_type']}")
            # Check max length change
            if (pd.notna(row['previous_max_length']) and 
                row['previous_max_length'] != row['current_max_length'] and 
                row['current_max_length'] != ''):
                changes.append(f"Length: {row['previous_max_length']} → {row['current_max_length']}")
            # Check nullable change
            if pd.notna(row['previous_nullable']) and row['previous_nullable'] != row['current_nullable']:
                changes.append(f"Nullable: {row['previous_nullable']} → {row['current_nullable']}")
            
            change_detail = ', '.join(changes) if changes else 'Other changes'
            desc = f"✏️ Modified: {row['column_name']} - {change_detail}"
        
        elif row['change_type'] == 'DELETED':
            desc = f"❌ Deleted: {row['column_name']} ({row['current_data_type']})"
        
        elif row['change_type'] == 'RENAMED':
            old_name = row['previous_column_name'] if pd.notna(row['previous_column_name']) else '(unknown)'
            desc = f"🔄 Renamed: {old_name} → {row['column_name']}"
        
        else:
            desc = f"{row['change_type']}: {row['column_name']}"
        
        descriptions.append(desc)
    
    df_timeline['description'] = descriptions
    
    # Display timeline
    display_cols = ['processed_time', 'snapshot_id', 'table_name', 'description']
    print(df_timeline[display_cols].to_string(index=False))

# Visualization - Timeline
if not df_timeline.empty:
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Convert to numeric for plotting
    df_timeline['time_numeric'] = pd.to_datetime(df_timeline['processed_time'])
    
    for change_type in df_timeline['change_type'].unique():
        subset = df_timeline[df_timeline['change_type'] == change_type]
        ax.scatter(subset['time_numeric'], subset['snapshot_id'], 
                  label=change_type, s=100, alpha=0.6)
    
    ax.set_xlabel('Time')
    ax.set_ylabel('Snapshot ID')
    ax.set_title('Change Timeline')
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# ============================================
# 9. HIGH-RISK CHANGES
# ============================================

query_high_risk = """
SELECT 
    processed_time,
    schema_name,
    object_type_name as table_name,
    object_subtype_name as column_name,
    change_type,
    CASE 
        WHEN change_type = 'DELETED' THEN '🔴 CRITICAL: Column deleted - potential data loss'
        WHEN change_type = 'MODIFIED' THEN '🟡 MEDIUM: Data type or constraint changed'
        WHEN change_type = 'RENAMED' THEN '🟡 MEDIUM: Column renamed - update application code'
        ELSE '🟢 LOW: Standard change'
    END as risk_level
FROM md5_metadata_tbl
WHERE processed_time BETWEEN %s AND %s
    AND change_type IN ('DELETED', 'MODIFIED', 'RENAMED')
ORDER BY 
    CASE 
        WHEN change_type = 'DELETED' THEN 1
        WHEN change_type = 'MODIFIED' THEN 2
        ELSE 3
    END,
    processed_time;
"""

df_high_risk = pd.read_sql_query(query_high_risk, conn, params=(START_TIME, END_TIME))

print("\n⚠️ HIGH-RISK CHANGES")
print("-" * 80)
if df_high_risk.empty:
    print("No high-risk changes detected.")
else:
    print(df_high_risk.to_string(index=False))

# ============================================
# EXPORT OPTIONS
# ============================================

print("\n" + "="*80)
print("EXPORT OPTIONS")
print("="*80)

# Export to Excel
try:
    with pd.ExcelWriter(f'schema_change_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx') as writer:
        df_summary.to_excel(writer, sheet_name='Summary', index=False)
        df_change_types.to_excel(writer, sheet_name='Change Types', index=False)
        df_by_table.to_excel(writer, sheet_name='By Table', index=False)
        df_additions.to_excel(writer, sheet_name='Additions', index=False)
        df_modifications.to_excel(writer, sheet_name='Modifications', index=False)
        df_deletions.to_excel(writer, sheet_name='Deletions', index=False)
        df_renames.to_excel(writer, sheet_name='Renames', index=False)
        df_timeline.to_excel(writer, sheet_name='Timeline', index=False)
        df_high_risk.to_excel(writer, sheet_name='High Risk', index=False)
    print("✅ Excel report exported successfully")
except Exception as e:
    print(f"⚠️ Excel export failed: {e}")

# Export to CSV
try:
    df_timeline.to_csv(f'schema_changes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv', index=False)
    print("✅ CSV export successful")
except Exception as e:
    print(f"⚠️ CSV export failed: {e}")

# Close connection
if conn:
    conn.close()
    print("\n✅ Database connection closed")

print("\n" + "="*80)
print("REPORT GENERATION COMPLETE")
print("="*80)