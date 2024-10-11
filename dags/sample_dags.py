from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sample_etl_pipeline',
    default_args=default_args,
    description='A sample ETL pipeline DAG',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False
)

# Python function to process data
def process_data(**context):
    print("Processing data...")
    # Add your data processing logic here
    processed_data = "Sample processed data"
    context['task_instance'].xcom_push(key='processed_data', value=processed_data)
    return "Data processing completed"

# Define tasks
start_task = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "Starting the pipeline at $(date)"',
    dag=dag
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag
)

end_task = BashOperator(
    task_id='end_pipeline',
    bash_command='echo "Pipeline completed at $(date)"',
    dag=dag
)

# Set task dependencies
start_task >> process_data_task >> end_task