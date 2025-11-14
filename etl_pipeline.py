import pandas as pd
import sqlite3
import os
from datetime import datetime


CSV_FILE = 'sales_data.csv'
DB_FILE = 'analytics_db.sqlite'
TABLE_NAME = 'product_monthly_revenue'

def generate_mock_data():
    
    print(f"--- Generating mock data at {CSV_FILE} ---")
    data = {
        'transaction_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'product_id': ['P101', 'P102', 'P101', 'P103', 'P102', 'P104', 'P101', 'P103', 'P105', 'P102'],
        'date': ['2024-09-01', '2024-09-01', '2024-09-02', '2024-09-02', '2024-09-03', '2024-09-03', '2024-09-04', '2024-09-04', '2024-09-05', '2024-09-05'],
        'amount': [15.50, 20.00, 15.50, 50.99, 20.00, -10.00, None, 50.99, 5.00, 20.00], # Includes a negative and a missing value
        'category': ['Gadget', 'Food', 'Gadget', 'Service', 'Food', 'Refund', 'Gadget', 'Service', 'Book', 'Food'],
    }
    df = pd.DataFrame(data)
    df.to_csv(CSV_FILE, index=False)
    print("Mock data generated successfully.")

def extract_data(csv_file_path: str) -> pd.DataFrame or None:
    """Extracts data from the local CSV file."""
    print(f"\n--- 1. EXTRACT: Reading data from {csv_file_path} ---")
    try:
        df = pd.read_csv(csv_file_path)
        print(f"Successfully read {len(df)} records.")
        return df
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_file_path}")
        return None
    except Exception as e:
        print(f"An error occurred during extraction: {e}")
        return None

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Performs cleaning, filtering, and aggregation."""
    print("\n--- 2. TRANSFORM: Cleaning and aggregating data ---")

    
    initial_count = len(df)
    df_cleaned = df.dropna()
    print(f"  - Dropped {initial_count - len(df_cleaned)} rows with missing values.")

    df_filtered = df_cleaned[df_cleaned['amount'] > 0]
    print(f"  - Dropped {len(df_cleaned) - len(df_filtered)} rows with zero or negative amounts.")

  
    df_filtered['date'] = pd.to_datetime(df_filtered['date'])
    df_filtered['amount'] = pd.to_numeric(df_filtered['amount'])

    
    df_aggregated = (
        df_filtered.groupby('product_id')['amount']
        .sum()
        .reset_index()
        .rename(columns={'amount': 'total_revenue_usd'})
    )

    df_aggregated['processed_at'] = datetime.now()

    print(f"Transformation complete. Resulting DataFrame has {len(df_aggregated)} unique products.")
    return df_aggregated

def load_data(df: pd.DataFrame, db_file_path: str, table_name: str):
    """Loads the transformed data into the SQLite database."""
    print(f"\n--- 3. LOAD: Writing data to SQLite database ({db_file_path}) ---")
    try:
       
        conn = sqlite3.connect(db_file_path)

        
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Successfully loaded {len(df)} records into table '{table_name}'.")

        verification_query = f"SELECT product_id, total_revenue_usd FROM {table_name}"
        verification_df = pd.read_sql_query(verification_query, conn)
        print("\n--- Verification Query Results ---")
        print(verification_df)
        print("----------------------------------")

       
        conn.close()

    except Exception as e:
        print(f"An error occurred during loading: {e}")

def main():
    """The main ETL process orchestrator."""

    
    if not os.path.exists(CSV_FILE):
        generate_mock_data()

    try:
       
        sales_df = extract_data(CSV_FILE)
        if sales_df is None:
            return

       
        analytics_df = transform_data(sales_df)

        load_data(analytics_df, DB_FILE, TABLE_NAME)

        print("\n--- ETL Pipeline Run Complete! ---")

    except Exception as e:
        print(f"\nFATAL ERROR in pipeline execution: {e}")

if __name__ == "__main__":
    main() 
