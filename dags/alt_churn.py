import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from steps.messages import send_telegram_success_message, send_telegram_failure_message
from steps.churn import extract, transform, load, create_table

with DAG(
    dag_id='alt_churn',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
) as dag:
    create_table_step = PythonOperator(task_id='create_table', python_callable=create_table)
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    load_step = PythonOperator(task_id='load', python_callable=load)
    create_table_step >> extract_step
    extract_step >> transform_step
    transform_step >> load_step