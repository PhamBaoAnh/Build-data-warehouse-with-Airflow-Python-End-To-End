from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, text
import pandas as pd

class PostgresOperators:
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.engine = create_engine(self.hook.get_uri())

    def get_connection(self):
        return self.hook.get_conn()

    def get_data_to_pd(self, sql: str) -> pd.DataFrame:
        return self.hook.get_pandas_df(sql)

    def save_data_to_staging(self, df: pd.DataFrame, table_name: str, schema: str = 'public', if_exists: str = 'replace'):
        """Ghi DataFrame vào schema staging (hoặc schema chỉ định)"""
        with self.engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method='multi'
        )
        print(f'✔ Đã lưu dữ liệu vào {schema}.{table_name} ({len(df)} dòng)')

    def delete_all_warehouse_data(self, schema='warehouse'):
        """
        Xóa toàn bộ dữ liệu các bảng trong schema warehouse, đúng thứ tự ràng buộc.
        Chú ý: phải xóa các bảng dimension trước, fact sau cùng nếu không dùng CASCADE.
        """
        tables_in_order = [
            'dim_customer',
            'dim_product',
            'dim_date',
            'dim_shipmode',
            'dim_location',
            'fact_sales'  # XÓA SAU CÙNG nếu có khóa ngoại
        ]

        with self.engine.begin() as conn:
            for table in tables_in_order:
                try:
                    conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE;'))
                    print(f'✔ Đã xóa dữ liệu trong {schema}.{table}')
                except Exception as e:
                    print(f'⚠ Không thể xóa {schema}.{table}: {e}')

    def save_data_to_postgres(self, df: pd.DataFrame, table_name: str, schema: str = 'public', if_exists: str = 'append'):
        """
        Ghi DataFrame vào PostgreSQL warehouse.
        - if_exists='truncate': xóa sạch bảng (giữ nguyên schema) trước khi insert.
        - if_exists='append': chỉ thêm dữ liệu mới.
        """
        with self.engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

            if if_exists == 'truncate':
                conn.execute(text(f'''
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = '{schema}' AND table_name = '{table_name}'
                        ) THEN
                            EXECUTE 'TRUNCATE TABLE "{schema}"."{table_name}" RESTART IDENTITY CASCADE';
                        END IF;
                    END$$;
                '''))
                actual_if_exists = 'append'
            else:
                actual_if_exists = if_exists

        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists=actual_if_exists,
            index=False,
            method='multi'
        )
        print(f'✔ Đã lưu dữ liệu vào {schema}.{table_name} ({len(df)} dòng)')

    def execute_query(self, sql: str):
        """Chạy câu lệnh SQL bất kỳ"""
        self.hook.run(sql)

    def get_engine(self):
        """Trả về SQLAlchemy engine để tương tác tự do"""
        return self.engine
     
    def run_sql(self, sql: str):
     self.hook.run(sql)


 