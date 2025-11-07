import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import warnings

warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)

def load_core_columns_details(ts=None):
    """
    Load core column details from analytics_schema.sales_data table.
    Returns:
        pandas.DataFrame: DataFrame containing sales data.
    """
    conn_params = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "bill_gates",
        "password": "bill_pass" 
    }

    q = """
            SELECT
                c.table_schema                            AS schema_name,
                c.table_name                              AS table_name,
                c.column_name                             AS column_name,
                COALESCE(c.data_type, '')                 AS data_type,
                COALESCE(c.character_maximum_length::TEXT, '') AS max_length,
                COALESCE(c.numeric_precision::TEXT, '')   AS numeric_precision,
                COALESCE(c.numeric_scale::TEXT, '')       AS numeric_scale,
                COALESCE(c.is_nullable, '')               AS nullable,
                COALESCE(c.column_default, '')            AS default_value,
                COALESCE(c.is_identity, '')               AS is_identity,
                COALESCE(c.is_generated, '')              AS is_generated,
                COALESCE(c.generation_expression, '')     AS generation_expression,
                COALESCE(k.constraint_name, '')           AS constraint_name,
                c.ordinal_position                        AS column_position
            FROM information_schema.columns c
            LEFT JOIN information_schema.key_column_usage k
                ON c.table_schema = k.table_schema
                AND c.table_name = k.table_name
                AND c.column_name = k.column_name
            WHERE c.table_schema = 'analytics_schema'
            AND c.table_name = 'sales_data'
            ORDER BY c.ordinal_position;
    """

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**conn_params)
        print("Connected to database successfully.")

        # Read query results into DataFrame
        df = pd.read_sql(q, conn)

        # Optional: log row count
        print(f"Retrieved {len(df)} records from analytics_schema.sales_data.")

        return df

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return pd.DataFrame()  # return empty dataframe on error

    except Exception as ex:
        print(f"Unexpected error: {ex}")
        return pd.DataFrame()

    finally:
        # Always close connection
        try:
            if conn:
                conn.close()
                print("Connection closed.")
        except Exception:
            pass


# ---------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------
if __name__ == "__main__":
    df_sales = load_core_columns_details()
    # Set the option to display all columns

    print(df_sales.head())
