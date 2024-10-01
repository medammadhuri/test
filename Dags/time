from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def print_current_datetime():
    now = datetime.now()
    print(f"Current date and time: {now}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'time_dag',
    default_args=default_args,
    description='A simple DAG to print current datetime',
    schedule_interval='0/5 * * * *',
)

datetime_task = PythonOperator(
    task_id='datetime_task',
    python_callable=print_current_datetime,
    dag=dag,
)
