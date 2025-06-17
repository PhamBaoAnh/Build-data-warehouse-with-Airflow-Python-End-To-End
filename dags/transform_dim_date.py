import pandas as pd
from postgresql_operator import PostgresOperators

def transform_dim_date():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')

    # ❗ Xóa bảng fact_sales trước (nếu có khóa ngoại tới order_date)
    warehouse_operator.run_sql('TRUNCATE TABLE "warehouse"."fact_sales" RESTART IDENTITY CASCADE')

    # ✅ Xóa sạch bảng dim_date trước khi ghi lại
    warehouse_operator.run_sql('TRUNCATE TABLE "warehouse"."dim_date" RESTART IDENTITY CASCADE')

    # Đọc dữ liệu từ bảng staging
    df_order = staging_operator.get_data_to_pd('SELECT order_date FROM "staging"."stg_Orders"')

    # Loại bỏ bản ghi null và trùng
    df_order = df_order.dropna(subset=['order_date']).drop_duplicates()

    # Chuyển đổi kiểu ngày
    df_order['order_date'] = pd.to_datetime(df_order['order_date'])

    # Tạo các trường dimension
    df_order['day'] = df_order['order_date'].dt.day
    df_order['month'] = df_order['order_date'].dt.month
    df_order['year'] = df_order['order_date'].dt.year
    df_order['weekday'] = df_order['order_date'].dt.weekday + 1  # Thứ 2 = 1
    df_order['quarter'] = df_order['order_date'].dt.quarter

    # Sắp xếp lại cột đúng chuẩn
    dim_df = df_order[['order_date', 'day', 'month', 'year', 'weekday', 'quarter']]
    dim_df = dim_df.drop_duplicates(subset=['order_date'])

    # Ghi dữ liệu vào warehouse
    warehouse_operator.save_data_to_postgres(
        df=dim_df,
        table_name='dim_date',
        schema='warehouse',
        if_exists='append'
    )

    print("✔ Đã transform và lưu dữ liệu vào warehouse.dim_date")
