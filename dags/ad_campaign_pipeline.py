from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.append('/opt/airflow/bigquery')
from bigquery.load_kaggle_data import main as load_kaggle_data


# Default arguments
default_args = {
    'owner': 'HaiChu',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'ad_campaign_data_pipeline',
    default_args=default_args,
    description='Weekly ad campaign data pipeline from Kaggle to BigQuery',
    schedule_interval='0 6 * * 1',  # Run every Monday at 6 AM
    catchup=False,
    tags=['bigquery', 'ads', 'etl', 'weekly'],
)

def read_sql_file(file_path):
    """Read SQL content from file"""
    with open(file_path, 'r') as f:
        return f.read()

setup_tables = BigQueryInsertJobOperator(
    task_id='setup_tables',
    configuration={
        "query": {
            "query": read_sql_file('/opt/airflow/bigquery/01_create_dataset.sql'),
            "useLegacySql": False,
        }
    },
    dag=dag,
)

load_kaggle_task = PythonOperator(
    task_id='load_kaggle_data',
    python_callable=load_kaggle_data,
    dag=dag,
)


# Set task dependencies
setup_tables >> load_kaggle_task