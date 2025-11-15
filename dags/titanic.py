from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.http.operators.http import HttpOperator
from datetime import datetime
from typing import Tuple, List, Dict


def validate_and_insert_titanic(csv_path: str, db_path: str, table_name: str = 'titanic') -> Dict:
    """
    Validate Titanic CSV and insert into SQLite database.

    Args:
        csv_path: Path to the CSV file
        db_path: Path to SQLite database file
        table_name: Name of the table to insert into

    Returns:
        Dictionary with processing statistics
    """
    import pandas as pd
    import sys
    stats = {
        'total_rows': 0,
        'inserted_rows': 0,
        'skipped_rows': 0,
        'warnings': []
    }

    # Load CSV
    try:
        df = pd.read_csv(csv_path)
        stats['total_rows'] = len(df)
        print(f"Loaded {stats['total_rows']} rows from {csv_path}")
    except Exception as e:
        print(f"Error loading CSV: {e}")
        sys.exit(1)

    # 1. SCHEMA VALIDATION
    # Normalize column names (strip whitespace, lowercase for comparison)
    df.columns = df.columns.str.strip()
    column_map = {col.lower(): col for col in df.columns}

    # Check required columns exist
    required_cols = ['age', 'survived']
    missing_cols = [col for col in required_cols if col not in column_map]

    if missing_cols:
        print(f"ERROR: Missing required columns: {missing_cols}")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)

    print(f"✓ Schema validation passed. Found required columns: Age, Survived")

    # Get actual column names (preserving original case)
    age_col = column_map['age']
    survived_col = column_map['survived']

    # 2. DATA INTEGRITY CHECKS

    # Check Survived column data type
    if not pd.api.types.is_numeric_dtype(df[survived_col]):
        print(f"ERROR: Survived column is not numeric. Type: {df[survived_col].dtype}")
        sys.exit(1)

    # Check Age column data type (allowing for NaN)
    if not pd.api.types.is_numeric_dtype(df[age_col]):
        print(f"ERROR: Age column is not numeric. Type: {df[age_col].dtype}")
        sys.exit(1)

    print(f"✓ Column data types are correct")

    # Process each row
    valid_rows = []

    for idx, row in df.iterrows():
        skip_row = False
        row_num = idx + 2  # +2 for header and 0-indexing

        # Validate Survived
        survived_val = row[survived_col]
        if pd.isna(survived_val):
            # Missing survived -> set to NULL
            survived_val = None
        elif survived_val not in [0, 1, 0.0, 1.0]:
            warning = f"Row {row_num}: Invalid Survived value '{survived_val}' (must be 0 or 1). Skipping row."
            print(f"WARNING: {warning}")
            stats['warnings'].append(warning)
            stats['skipped_rows'] += 1
            skip_row = True

        # Validate Age
        age_val = row[age_col]
        if pd.isna(age_val):
            # Missing age -> set to NULL
            age_val = None
        elif not isinstance(age_val, (int, float)) or age_val < 0 or age_val > 120:
            warning = f"Row {row_num}: Invalid Age value '{age_val}' (must be 0-120). Skipping row."
            print(f"WARNING: {warning}")
            stats['warnings'].append(warning)
            stats['skipped_rows'] += 1
            skip_row = True

        if not skip_row:
            # Create cleaned row
            cleaned_row = row.copy()
            cleaned_row[survived_col] = survived_val
            cleaned_row[age_col] = age_val
            valid_rows.append(cleaned_row)

    print(f"\n✓ Data validation complete:")
    print(f"  - Valid rows: {len(valid_rows)}")
    print(f"  - Skipped rows: {stats['skipped_rows']}")

    if len(valid_rows) == 0:
        print("ERROR: No valid rows to insert")
        sys.exit(1)

    # Create DataFrame from valid rows
    valid_df = pd.DataFrame(valid_rows)

    # 3. INSERT INTO SQLITE
    try:
        hook = SqliteHook(sqlite_conn_id='my_sqlite_conn')
        conn = hook.get_conn()
        # Create table (if not exists) - using pandas to infer schema
        valid_df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.commit()
        stats['inserted_rows'] = len(valid_rows)
        print(f"\n✓ Successfully inserted {stats['inserted_rows']} rows into {db_path}:{table_name}")

    except Exception as e:
        print(f"ERROR during database insertion: {e}")
        sys.exit(1)

    return stats


def save_csv_to_file(response: bytes) -> str:
    """
    Save CSV content to file and return the path for XCom
    """
    file_path = '/opt/airflow/data/titanic.csv'
    with open(file_path, 'wb') as f:
        f.write(response.content)
    return file_path

# Define DAG
with DAG(
    'sqlite_example_dag',
    start_date=datetime(2025, 11, 15),
    schedule=None, # Run manually
    catchup=False,
) as dag:

    download_task = HttpOperator(
    task_id='download_csv',
    http_conn_id='titanic_http',
    endpoint='class/archive/cs/cs109/cs109.1166/stuff/titanic.csv',
    method='GET',
    response_filter=save_csv_to_file,
    response_check=lambda response: response.status_code == 200,
    log_response=False,
)
    validate_and_insert_task = PythonOperator(
        task_id='validate_and_insert_titanic',
        python_callable=validate_and_insert_titanic,
        op_kwargs={
            'csv_path': "{{ ti.xcom_pull(task_ids='download_csv') }}",
            'db_path': '/opt/airflow/data/sqlite.db',
            'table_name': 'titanic',
        },
    )

    download_task >> validate_and_insert_task