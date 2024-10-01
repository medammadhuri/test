from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from mail import send_email_on_failure, send_email_on_retry, send_email_on_success

def start_time():
    # just print the time    
    start_time = datetime.now()
    print(start_time)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 4)
}

# Define the first DAG
with DAG(
    'start1',
    default_args=default_args,
    catchup=False,
    schedule_interval='30 14 * * 1-5'
) as start1:

    # Start Time
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start_time,
        on_failure_callback=lambda context: send_email_on_failure(context, 'start1', 'start_task'),
        on_retry_callback=lambda context: send_email_on_retry(context, 'start1', 'start_task'),
        on_success_callback=lambda context: send_email_on_success(context, 'start1', 'start_task'),
    )

    # Trigger the 'end1' DAG after the start_task runs successfully
    trigger_end1 = TriggerDagRunOperator(
        task_id='end1',
        trigger_dag_id='end1',
        on_success_callback=lambda context: send_email_on_success(context, 'start1', "trigger"),
        on_failure_callback=lambda context: send_email_on_failure(context, 'start1', "trigger"),
        on_retry_callback=lambda context: send_email_on_retry(context, 'start1', "trigger"),
    )

    start_task >> trigger_end1

# Define the second DAG
with DAG(
    'end1',
    default_args=default_args,
    catchup=False,
    schedule_interval=None
) as end1:

    check = PythonOperator(
        task_id='check',
        python_callable=start_time,
        on_failure_callback=lambda context: send_email_on_failure(context, 'end1', 'check'),
        on_retry_callback=lambda context: send_email_on_retry(context, 'end1', 'check'),
        on_success_callback=lambda context: send_email_on_success(context, 'end1', 'check'),
    )

    check
