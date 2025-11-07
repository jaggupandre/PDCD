import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import warnings

warnings.filterwarnings("ignore")

def load_core_columns_details(ts=None):
    """
    Load core column details from analytics_schema.sales_data table.

    Parameters:
        ts (str or None): Optional timestamp or identifier for logging/tracking.

    Returns:
        pandas.DataFrame: DataFrame containing sales data.
    """
    conn_params = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "bill_gates",
        "password": "bill_pass"  # Add password if required
    }

    q = """
        SELECT * 
        FROM analytics_schema.sales_data;
    """

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**conn_params)
        print("✅ Connected to database successfully.")

        # Read query results into DataFrame
        df = pd.read_sql(q, conn)

        # Optional: log row count
        print(f"📊 Retrieved {len(df)} records from analytics_schema.sales_data.")

        return df

    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        return pd.DataFrame()  # return empty dataframe on error

    except Exception as ex:
        print(f"⚠️ Unexpected error: {ex}")
        return pd.DataFrame()

    finally:
        # Always close connection
        try:
            if conn:
                conn.close()
                print("🔒 Connection closed.")
        except Exception:
            pass


# ---------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------
if __name__ == "__main__":
    df_sales = load_core_columns_details()
    print(df_sales.head())
