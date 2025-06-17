import pandas as pd
from postgresql_operator import PostgresOperators

def transform_fact_sales():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')

    # ✅ Xoá toàn bộ dữ liệu cũ để tránh trùng order_id
    warehouse_operator.run_sql('TRUNCATE TABLE warehouse.fact_sales CASCADE')

    # Lấy dữ liệu từ staging
    df_order = staging_operator.get_data_to_pd('SELECT * FROM staging."stg_Orders"')
    df_order_detail = staging_operator.get_data_to_pd('SELECT * FROM staging."stg_Order_Details"')

    # Chuẩn hóa tên cột
    df_order.columns = df_order.columns.str.lower()
    df_order_detail.columns = df_order_detail.columns.str.lower()

    # Chuyển order_date về datetime
    df_order['order_date'] = pd.to_datetime(df_order['order_date'])

    # Giữ bản ghi mới nhất theo order_id
    df_order = df_order.sort_values('order_date').drop_duplicates(subset='order_id', keep='last')

    # Merge order với order_detail
    df = df_order.merge(df_order_detail, how='inner', on='order_id')

    # Lấy dữ liệu từ dimension tables
    df_location = warehouse_operator.get_data_to_pd(
        'SELECT location_id, city, state, country FROM warehouse.dim_location'
    )
    df_shipmode = warehouse_operator.get_data_to_pd(
        'SELECT shipmode_id, ship_mode FROM warehouse.dim_shipmode'
    )
    df_products = warehouse_operator.get_data_to_pd(
        'SELECT product_id FROM warehouse.dim_product'
    )
    df_customers = warehouse_operator.get_data_to_pd(
        'SELECT customer_id FROM warehouse.dim_customer'
    )

    # Merge với dim_location
    df = df.merge(df_location, how='left',
                  left_on=['ship_city', 'ship_state', 'ship_country'],
                  right_on=['city', 'state', 'country'])

    # Merge với dim_shipmode
    df = df.merge(df_shipmode, how='left', on='ship_mode')

    # Merge với dim_product
    df = df.merge(df_products, how='left', on='product_id')

    # Merge với dim_customer
    df = df.merge(df_customers, how='left', on='customer_id')

    # 🛡️ Loại bỏ các dòng không đầy đủ khóa ngoại
    df = df.dropna(subset=[
        'customer_id', 'product_id', 'location_id', 'shipmode_id'
    ])

    # Chọn và chuẩn hóa tên cột
    fact_df = df[[  
        'order_id',
        'order_date',
        'customer_id',
        'product_id',
        'location_id',
        'shipmode_id',
        'discount',
        'profit',
        'quantity',
        'sales'
    ]].copy()

    # Loại bỏ trùng order_id
    fact_df = fact_df.drop_duplicates(subset=['order_id'])

    # Ghi vào warehouse
    warehouse_operator.save_data_to_postgres(
        df=fact_df,
        table_name='fact_sales',
        schema='warehouse',
        if_exists='append'
    )

    print("✔ Đã transform và lưu dữ liệu vào warehouse.fact_sales")
