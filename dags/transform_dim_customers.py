import pandas as pd
from postgresql_operator import PostgresOperators

def transform_dim_customers():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')

    # Xóa bảng fact trước vì có FK đến dim_customer
    warehouse_operator.run_sql('TRUNCATE TABLE "warehouse"."fact_sales" RESTART IDENTITY CASCADE')

    # Xóa dim_customer sau khi fact đã bị xóa
    warehouse_operator.run_sql('TRUNCATE TABLE "warehouse"."dim_customer" RESTART IDENTITY CASCADE')

    # Bước 3: Đọc dữ liệu từ staging
    df = staging_operator.get_data_to_pd('SELECT * FROM "staging"."stg_Customer"')

    # Bước 4: Transform phù hợp với bảng dim_customer
    dim_df = df[['customer_id', 'customer_name', 'segment']].copy()
    dim_df.columns = ['customer_id', 'customer_name', 'segment']

    # Bước 5: Làm sạch và chuẩn hóa
    dim_df['customer_id'] = dim_df['customer_id'].astype(str)
    dim_df['customer_name'] = dim_df['customer_name'].fillna('').str.strip()
    dim_df['segment'] = dim_df['segment'].fillna('').str.title()
    dim_df = dim_df.drop_duplicates(subset=['customer_id'])

    # Bước 6: Ghi vào warehouse
    warehouse_operator.save_data_to_postgres(
        df=dim_df,
        table_name='dim_customer',
        schema='warehouse',
        if_exists='append'  # safe vì đã truncate
    )

    print("✔ Đã transform và lưu dữ liệu vào warehouse.dim_customer")
