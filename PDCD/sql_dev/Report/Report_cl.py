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
# 8. CHRONOLOGICAL TIMELINE
# ============================================

query_timeline = """
SELECT 
    snapshot_id,
    processed_time,
    schema_name,
    object_type_name as table_name,
    object_subtype_name as column_name,
    change_type
FROM md5_metadata_tbl
WHERE processed_time BETWEEN %s AND %s
ORDER BY processed_time, snapshot_id, schema_name, object_type_name;
"""

df_timeline = pd.read_sql_query(query_timeline, conn, params=(START_TIME, END_TIME))

print("\n📅 CHRONOLOGICAL TIMELINE")
print("-" * 80)
if df_timeline.empty:
    print("No changes in this period.")
else:
    # Add emoji indicators
    emoji_map = {
        'ADDED': '➕',
        'MODIFIED': '✏️',
        'DELETED': '❌',
        'RENAMED': '🔄'
    }
    df_timeline['change_indicator'] = df_timeline['change_type'].map(emoji_map)
    df_timeline['description'] = (df_timeline['change_indicator'] + ' ' + 
                                  df_timeline['change_type'] + ': ' +
                                  df_timeline['table_name'] + '.' + 
                                  df_timeline['column_name'])
    print(df_timeline[['processed_time', 'snapshot_id', 'description']].to_string(index=False))

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