from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from extract_data import extract_and_load_to_staging
from transform_dim_customers import transform_dim_customers
from transform_dim_products import transform_dim_products
from transform_dim_locations import transform_dim_locations
from transform_dim_shipmode import transform_dim_shipmode
from transform_dim_date import transform_dim_date
from transform_fact_sales import transform_fact_sales

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='globalsuperstore',
    default_args=default_args,
    description='ETL process for GlobalSuperStore Data Warehouse',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    # Task Group: Extract
    with TaskGroup("extract") as extract_group:
        extract_task = PythonOperator(
            task_id='extract_and_load_to_staging',
            python_callable=extract_and_load_to_staging,
        )

    # Task Group: Transform
    with TaskGroup("transform") as transform_group:
        task_dim_customers = PythonOperator(
            task_id='transform_dim_customers',
            python_callable=transform_dim_customers,
        )

        task_dim_products = PythonOperator(
            task_id='transform_dim_products',
            python_callable=transform_dim_products,
        )

        task_dim_locations = PythonOperator(
            task_id='transform_dim_locations',
            python_callable=transform_dim_locations,
        )

        task_dim_shipmode = PythonOperator(
            task_id='transform_dim_shipmode',
            python_callable=transform_dim_shipmode,
        )

        task_dim_date = PythonOperator(
            task_id='transform_dim_date',
            python_callable=transform_dim_date,
        )

    # Task Group: Load
    with TaskGroup("load") as load_group:
        task_fact_sales = PythonOperator(
            task_id='transform_fact_sales',
            python_callable=transform_fact_sales,
        )

    # Dependencies
    extract_group >> transform_group >> load_group
