import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Pipeline.crawl import crawl_id, crawl_product_data, clean_write_data

dag = DAG(
    dag_id='tiki_flow',
    default_args={
        "start_date": datetime(2024, 5, 7),
    },
    schedule_interval="@daily",
    catchup=False
)

crawl_id = PythonOperator(
    task_id='crawl_id',
    python_callable=crawl_id,
    provide_context=True,
    dag=dag
)

crawl_product_data = PythonOperator(
    task_id='crawl_product_data',
    provide_context=True,
    python_callable=crawl_product_data,
    dag=dag
)

clean_write_data = PythonOperator(
    task_id='clean_write_data',
    provide_context=True,
    python_callable=clean_write_data,
    dag=dag
)

crawl_id >> crawl_product_data >> clean_write_data