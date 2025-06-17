from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

from mysql_operator import MySQLOperators
from postgresql_operator import PostgresOperators

def extract_and_load_to_staging(**kwargs):
    source_operator = MySQLOperators('mysql')
    staging_operator = PostgresOperators('postgres')

    tables = [
        "Category",
        "Customer",
        "Order_Details",
        "Orders",
        "Product",
        "Sub_Category",
    ]
    
    for table in tables:
        print(f"⏳ Đang tải dữ liệu từ bảng {table}...")

        # Trích xuất dữ liệu từ MySQL
        df = source_operator.get_data_to_pd(f"SELECT * FROM {table}")

        # Bỏ qua nếu không có dữ liệu
        if df.empty:
            print(f"⚠ {table} không có dữ liệu, bỏ qua.")
            continue

        # Ghi vào schema staging của PostgreSQL
        staging_operator.save_data_to_staging(
            df=df,
            table_name=f"stg_{table}",
            schema='staging',
            if_exists='replace'  # Ghi đè dữ liệu staging mỗi lần extract
        )

        print(f"✔ Đã load {table} → staging.stg_{table}")
