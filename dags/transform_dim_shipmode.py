import pandas as pd
from postgresql_operator import PostgresOperators

def transform_dim_shipmode():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')

    # ✅ Xóa dữ liệu cũ để tránh trùng khóa chính
    warehouse_operator.run_sql('TRUNCATE TABLE "warehouse"."dim_shipmode" RESTART IDENTITY CASCADE')

    # Đọc dữ liệu từ bảng stg_Orders
    df_order = staging_operator.get_data_to_pd('SELECT * FROM "staging"."stg_Orders"')
    print(f"[DEBUG] Số dòng stg_Orders: {len(df_order)}")

    if df_order.empty:
        print("[WARN] Bảng staging.stg_Orders không có dữ liệu!")
        return

    # Lấy cột ship_mode
    dim_df = df_order[['ship_mode']].copy()

    # Làm sạch dữ liệu
    dim_df['ship_mode'] = dim_df['ship_mode'].fillna('').str.strip()

    # Loại bỏ bản ghi trùng và dòng trống
    dim_df = dim_df.drop_duplicates()
    dim_df = dim_df[dim_df['ship_mode'] != '']

    # Tạo shipmode_id tự động
    dim_df = dim_df.reset_index(drop=True)
    dim_df.insert(0, 'shipmode_id', dim_df.index + 1)

    # Ghi dữ liệu vào warehouse
    warehouse_operator.save_data_to_postgres(
        df=dim_df,
        table_name='dim_shipmode',
        schema='warehouse',
        if_exists='append'
    )

    print("✔ Đã transform và lưu dữ liệu vào warehouse.dim_shipmode")
