"""
transaction_pipeline.py

Bronze -> Silver pipeline for financial transactions.

Task 1 (extract_data): read raw_transactions.csv, load AS-IS into bronze_transactions.
Task 2 (clean_data):   read bronze_transactions, clean it, return a cleaned dataframe.
Task 3 (load_silver):  load the cleaned dataframe into silver_transactions.
"""

from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Name of the Airflow Connection (Admin -> Connections) or the
# AIRFLOW_CONN_ANALYTICS_DB_CONN env var set in docker-compose.yml
POSTGRES_CONN_ID = "analytics_db_conn"
CSV_PATH = "/opt/airflow/dags/raw_transactions.csv"


def extract_data(**context):
    """Read the raw CSV and dump it, unedited, into bronze_transactions."""
    df = pd.read_csv(CSV_PATH, dtype=str)  # dtype=str keeps it "raw" -- no type coercion yet

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        "bronze_transactions",
        engine,
        if_exists="replace",  # simplest for a daily full-refresh; switch to "append" if you want history
        index=False,
    )
    print(f"Loaded {len(df)} raw rows into bronze_transactions")


def _standardize_date(raw_value: str):
    """Try a list of known formats before giving up (returns None if unparseable)."""
    if pd.isna(raw_value) or str(raw_value).strip() == "":
        return None

    raw_value = str(raw_value).strip()
    candidate_formats = [
        "%d-%m-%Y",  # 12-05-2026
        "%Y/%m/%d",  # 2026/05/13
        "%m-%d-%y",  # 05-14-26
        "%d-%m-%y",  # fallback two-digit-year variant
    ]
    for fmt in candidate_formats:
        try:
            return datetime.strptime(raw_value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None  # truly unparseable -> treated like missing


def clean_data(**context):
    """Pull bronze_transactions, apply cleaning rules, push cleaned df to XCom."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql("SELECT * FROM bronze_transactions", engine)

    # 1. Standardize dates to YYYY-MM-DD; drop rows where date is missing entirely
    df["txn_date"] = df["txn_date"].apply(_standardize_date)
    df = df.dropna(subset=["txn_date"])

    # 2. Strip "$" from amount and cast to float
    df["amount"] = (
        df["amount"].astype(str).str.replace("$", "", regex=False).astype(float)
    )

    # 3. Fill missing currency with 'USD'
    df["currency"] = df["currency"].fillna("USD")
    df["currency"] = df["currency"].replace("", "USD")

    # 4. Standardize status to uppercase
    df["status"] = df["status"].str.upper()

    print(f"Cleaned dataframe has {len(df)} rows (started from bronze)")

    # Push the cleaned data to the next task via XCom (fine for small datasets;
    # for large data you'd stage it to a temp table or file instead)
    context["ti"].xcom_push(key="cleaned_df", value=df.to_json(orient="records"))


def load_silver(**context):
    """Read the cleaned dataframe from XCom and load it into silver_transactions."""
    ti = context["ti"]
    cleaned_json = ti.xcom_pull(task_ids="clean_data", key="cleaned_df")
    df = pd.read_json(cleaned_json, orient="records")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        "silver_transactions",
        engine,
        if_exists="replace",
        index=False,
    )
    print(f"Loaded {len(df)} cleaned rows into silver_transactions")


default_args = {
    "owner": "data_engineering_intern",
    "retries": 1,
}

with DAG(
    dag_id="transaction_pipeline",
    default_args=default_args,
    description="Bronze -> Silver pipeline for messy financial transactions",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze-silver", "intern-project"],
) as dag:

    t1_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    t2_clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )

    t3_load = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver,
    )

    # load_silver only runs if clean_data succeeds -- this is Airflow's default
    # trigger_rule ("all_success"), so the plain ">>" chaining already enforces it.
    t1_extract >> t2_clean >> t3_load