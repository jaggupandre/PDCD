# Schema Change Report - Columns + Constraints (robust, production-ready)
# Run in Jupyter or as a script

import re
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ---------------------------
# CONFIG
# ---------------------------
DB_CONFIG = {
    "host": "localhost",
    "database": "your_database",
    "user": "your_user",
    "password": "your_password",
    "port": 5432
}

# Time window for report (adjust)
START_TIME = "2025-11-14 13:00:00"
END_TIME   = "2025-11-14 15:00:00"

# Schema + table where you store metadata
MD_SCHEMA = "pdcd_schema"
MD_TABLE  = "md5_metadata_tbl"   # fully referenced below as pdcd_schema.md5_metadata_tbl

# ---------------------------
# DB helper
# ---------------------------
def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn

# ---------------------------
# Parsing helpers
# ---------------------------
def _re_search_any(s, candidates):
    """Return first regex group matched for any of candidate keys.
       candidates: list of possible key names (e.g. ['is_nullable','nullable'])
       s: details string
    """
    if not s:
        return ""
    for key in candidates:
        m = re.search(rf"{re.escape(key)}:([^,]+)", s)
        if m:
            return m.group(1).strip()
    return ""

def parse_column_details(detail_str):
    """Return dict for column fields with normalized keys."""
    if not detail_str:
        return {}
    return {
        "data_type": _re_search_any(detail_str, ["data_type", "dtype"]),
        "max_length": _re_search_any(detail_str, ["max_length", "character_maximum_length"]),
        "numeric_precision": _re_search_any(detail_str, ["numeric_precision"]),
        "numeric_scale": _re_search_any(detail_str, ["numeric_scale"]),
        "nullable": _re_search_any(detail_str, ["is_nullable", "nullable"]),
        "default_value": _re_search_any(detail_str, ["column_default", "default_value"]),
        "ordinal_position": _re_search_any(detail_str, ["ordinal_position", "position"])
    }

def parse_constraint_details(detail_str):
    """Return dict for constraint fields."""
    if not detail_str:
        return {}
    return {
        "constraint_type": _re_search_any(detail_str, ["constraint_type"]),
        "constraint_column": _re_search_any(detail_str, ["column_name", "column"]),
        "definition": _re_search_any(detail_str, ["definition"])
    }

# Generic parser that chooses column or constraint parsing based on subtype
def parse_details_row(row):
    detail = row.get("object_subtype_details", "") or ""
    subtype = (row.get("object_subtype") or "").strip().lower()
    if subtype == "column":
        return parse_column_details(detail)
    if subtype == "constraint":
        return parse_constraint_details(detail)
    # fallback: try both patterns
    dcol = parse_column_details(detail)
    dcon = parse_constraint_details(detail)
    # merge preferring column keys if present
    merged = {}
    merged.update(dcon)
    merged.update(dcol)
    return merged

# ---------------------------
# Query functions
# ---------------------------

def run_query(conn, sql, params=None):
    return pd.read_sql_query(sql, conn, params=params)

def executive_summary(conn, start_time, end_time):
    sql = f"""
    SELECT 
        COUNT(DISTINCT snapshot_id) AS total_snapshots,
        COUNT(DISTINCT schema_name) AS schemas_affected,
        COUNT(DISTINCT object_type_name) AS tables_affected,
        COUNT(*) FILTER (WHERE object_subtype = 'Column') AS columns_affected,
        COUNT(*) FILTER (WHERE object_subtype = 'Constraint') AS constraints_affected,
        COUNT(*) AS total_changes,
        MIN(processed_time) AS first_change,
        MAX(processed_time) AS last_change
    FROM {MD_SCHEMA}.{MD_TABLE}
    WHERE processed_time BETWEEN %s AND %s;
    """
    return run_query(conn, sql, (start_time, end_time))

def change_type_breakdown(conn, start_time, end_time):
    sql = f"""
    SELECT change_type, COUNT(*) AS cnt
    FROM {MD_SCHEMA}.{MD_TABLE}
    WHERE processed_time BETWEEN %s AND %s
    GROUP BY change_type
    ORDER BY cnt DESC;
    """
    return run_query(conn, sql, (start_time, end_time))

def changes_by_table(conn, start_time, end_time):
    sql = f"""
    SELECT schema_name,
           object_type_name AS table_name,
           COUNT(*) AS total_changes,
           COUNT(*) FILTER (WHERE object_subtype='Column') AS column_changes,
           COUNT(*) FILTER (WHERE object_subtype='Constraint') AS constraint_changes,
           COUNT(*) FILTER (WHERE change_type='ADDED') AS added,
           COUNT(*) FILTER (WHERE change_type='MODIFIED') AS modified,
           COUNT(*) FILTER (WHERE change_type='DELETED') AS deleted,
           COUNT(*) FILTER (WHERE change_type='RENAMED') AS renamed
    FROM {MD_SCHEMA}.{MD_TABLE}
    WHERE processed_time BETWEEN %s AND %s
    GROUP BY schema_name, object_type_name
    ORDER BY total_changes DESC;
    """
    return run_query(conn, sql, (start_time, end_time))

def column_changes(conn, start_time, end_time):
    sql = f"""
    SELECT metadata_id, snapshot_id, processed_time, schema_name, object_type_name,
           object_subtype_name AS column_name, object_subtype_details, object_md5, change_type
    FROM {MD_SCHEMA}.{MD_TABLE}
    WHERE object_subtype = 'Column'
      AND processed_time BETWEEN %s AND %s
    ORDER BY processed_time, schema_name, object_type_name, object_subtype_name;
    """
    return run_query(conn, sql, (start_time, end_time))

def constraint_changes(conn, start_time, end_time):
    sql = f"""
    SELECT metadata_id, snapshot_id, processed_time, schema_name, object_type_name,
           object_subtype_name AS constraint_name, object_subtype_details, object_md5, change_type
    FROM {MD_SCHEMA}.{MD_TABLE}
    WHERE object_subtype = 'Constraint'
      AND processed_time BETWEEN %s AND %s
    ORDER BY processed_time, schema_name, object_type_name, object_subtype_name;
    """
    return run_query(conn, sql, (start_time, end_time))

def timeline(conn, start_time, end_time):
    sql = f"""
    SELECT processed_time, snapshot_id, schema_name, object_type_name, object_subtype_name,
           object_subtype, change_type, object_subtype_details
    FROM {MD_SCHEMA}.{MD_TABLE}
    WHERE processed_time BETWEEN %s AND %s
    ORDER BY processed_time, snapshot_id;
    """
    return run_query(conn, sql, (start_time, end_time))

# ---------------------------
# Report generation
# ---------------------------

def generate_report(start_time, end_time, export_excel=True, plot=True):
    conn = None
    try:
        conn = get_conn()
        print("Generating report for", start_time, "→", end_time)
        # Executive summary
        df_summary = executive_summary(conn, start_time, end_time)
        print("\n=== EXECUTIVE SUMMARY ===")
        if df_summary.empty:
            print("No changes in the given window.")
        else:
            print(df_summary.to_string(index=False))

        # Change types
        df_types = change_type_breakdown(conn, start_time, end_time)
        print("\n=== CHANGE TYPE BREAKDOWN ===")
        print(df_types.to_string(index=False))

        # Changes by table
        df_by_table = changes_by_table(conn, start_time, end_time)
        print("\n=== CHANGES BY TABLE ===")
        print(df_by_table.head(50).to_string(index=False))

        # Columns
        df_cols = column_changes(conn, start_time, end_time)
        if not df_cols.empty:
            detail_rows = df_cols.apply(lambda r: parse_details_row(r.to_dict()), axis=1).apply(pd.Series)
            df_cols_expanded = pd.concat([df_cols.reset_index(drop=True), detail_rows.reset_index(drop=True)], axis=1)
        else:
            df_cols_expanded = df_cols.copy()
        print("\n=== COLUMN CHANGES ===")
        if df_cols_expanded.empty:
            print("No column changes.")
        else:
            display_cols = ["processed_time", "snapshot_id", "schema_name", "object_type_name",
                            "column_name", "change_type", "data_type", "nullable", "default_value", "ordinal_position"]
            # Safe print - some columns may not exist
            print(df_cols_expanded[[c for c in display_cols if c in df_cols_expanded.columns]].to_string(index=False))

        # Constraints
        df_cons = constraint_changes(conn, start_time, end_time)
        if not df_cons.empty:
            con_details = df_cons.apply(lambda r: parse_details_row(r.to_dict()), axis=1).apply(pd.Series)
            df_cons_expanded = pd.concat([df_cons.reset_index(drop=True), con_details.reset_index(drop=True)], axis=1)
        else:
            df_cons_expanded = df_cons.copy()
        print("\n=== CONSTRAINT CHANGES ===")
        if df_cons_expanded.empty:
            print("No constraint changes.")
        else:
            cons_cols = ["processed_time", "snapshot_id", "schema_name", "object_type_name",
                         "constraint_name", "change_type", "constraint_type", "constraint_column", "definition"]
            print(df_cons_expanded[[c for c in cons_cols if c in df_cons_expanded.columns]].to_string(index=False))

        # Timeline
        df_tl = timeline(conn, start_time, end_time)
        if not df_tl.empty:
            df_tl["indicator"] = df_tl["change_type"].map({
                "ADDED": "➕", "MODIFIED": "✏️", "DELETED": "❌", "RENAMED": "🔄"
            })
            df_tl["description"] = df_tl["indicator"] + " " + df_tl["change_type"] + " → " + \
                                   df_tl["object_type_name"] + "." + df_tl["object_subtype_name"] + \
                                   " (" + df_tl["object_subtype"] + ")"
            print("\n=== TIMELINE (sample) ===")
            print(df_tl[["processed_time", "snapshot_id", "description"]].head(100).to_string(index=False))
        else:
            print("\nNo timeline events in window.")

        # High risk
        if not df_tl.empty:
            df_high = df_tl[df_tl["change_type"].isin(["DELETED", "MODIFIED"])].copy()
            if not df_high.empty:
                df_high["risk"] = df_high["change_type"].map({
                    "DELETED": "🔴 Critical – Object deleted",
                    "MODIFIED": "🟡 Medium – Object changed"
                })
                print("\n=== HIGH RISK CHANGES ===")
                print(df_high[["processed_time", "object_type_name", "object_subtype_name", "change_type", "risk"]].to_string(index=False))
            else:
                print("\nNo high-risk changes detected.")
        # Export to Excel
        if export_excel:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"schema_change_report_{timestamp}.xlsx"
            with pd.ExcelWriter(fname) as writer:
                df_summary.to_excel(writer, sheet_name="Summary", index=False)
                df_types.to_excel(writer, sheet_name="ChangeTypes", index=False)
                df_by_table.to_excel(writer, sheet_name="ByTable", index=False)
                df_cols_expanded.to_excel(writer, sheet_name="Columns", index=False)
                df_cons_expanded.to_excel(writer, sheet_name="Constraints", index=False)
                df_tl.to_excel(writer, sheet_name="Timeline", index=False)
            print(f"\n✅ Excel exported: {fname}")

        # Optional plotting - simple bar of change types
        if plot and not df_types.empty:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.bar(df_types['change_type'], df_types['cnt'])
            ax.set_xlabel("Change Type")
            ax.set_ylabel("Count")
            ax.set_title("Change Type Distribution")
            plt.tight_layout()
            plt.show()

    except Exception as e:
        print("Error while generating report:", e)
    finally:
        if conn:
            conn.close()
            print("DB connection closed.")

# ---------------------------
# Run
# ---------------------------
if __name__ == "__main__":
    generate_report(START_TIME, END_TIME, export_excel=True, plot=False)
