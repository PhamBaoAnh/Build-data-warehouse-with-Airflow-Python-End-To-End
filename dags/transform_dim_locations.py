import pandas as pd
from postgresql_operator import PostgresOperators
import hashlib

def generate_location_id(row):
    # Tạo ID ổn định từ giá trị kết hợp của city/state/country
    location_str = f"{row['city']}_{row['state']}_{row['country']}"
    return abs(int(hashlib.md5(location_str.encode()).hexdigest(), 16)) % (10**8)

def transform_dim_locations():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')

    # ❗ Xóa bảng fact trước để tránh lỗi khóa ngoại
    warehouse_operator.run_sql('TRUNCATE TABLE "warehouse"."fact_sales" RESTART IDENTITY CASCADE')

    # ✅ Xóa sạch bảng dim_location
    warehouse_operator.run_sql('TRUNCATE TABLE "warehouse"."dim_location" RESTART IDENTITY CASCADE')

    # Đọc dữ liệu từ staging
    df_order = staging_operator.get_data_to_pd('SELECT * FROM "staging"."stg_Orders"')

    # Lấy các cột cần thiết
    dim_df = df_order[['ship_city', 'ship_state', 'ship_country']].copy()

    # Làm sạch dữ liệu
    dim_df['ship_city'] = dim_df['ship_city'].fillna('').str.strip()
    dim_df['ship_state'] = dim_df['ship_state'].fillna('').str.strip()
    dim_df['ship_country'] = dim_df['ship_country'].fillna('').str.strip()

    # Đổi tên để khớp với định danh bảng dim_location
    dim_df.columns = ['city', 'state', 'country']

    # Loại bỏ bản ghi trùng lặp
    dim_df = dim_df.drop_duplicates()

    # Sinh location_id ổn định từ hash (đảm bảo đồng nhất mỗi lần chạy)
    dim_df['location_id'] = dim_df.apply(generate_location_id, axis=1)

    # Đặt lại thứ tự cột
    dim_df = dim_df[['location_id', 'city', 'state', 'country']]

    # Ghi vào warehouse
    warehouse_operator.save_data_to_postgres(
        df=dim_df,
        table_name='dim_location',
        schema='warehouse',
        if_exists='append'
    )

    print("✔ Đã transform và lưu dữ liệu vào warehouse.dim_location")
