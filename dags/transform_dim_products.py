import pandas as pd
from postgresql_operator import PostgresOperators

def transform_dim_products():
    staging_operator = PostgresOperators('postgres')
    warehouse_operator = PostgresOperators('postgres')

    # ✅ Nên xóa fact_sales trước để tránh lỗi khi làm mới dim_product
    warehouse_operator.run_sql('TRUNCATE TABLE "warehouse"."fact_sales" RESTART IDENTITY CASCADE')
    warehouse_operator.run_sql('TRUNCATE TABLE "warehouse"."dim_product" RESTART IDENTITY CASCADE')

    # Đọc dữ liệu từ staging
    df_product = staging_operator.get_data_to_pd('SELECT * FROM "staging"."stg_Product"')
    df_sub_category = staging_operator.get_data_to_pd('SELECT * FROM "staging"."stg_Sub_Category"')
    df_category = staging_operator.get_data_to_pd('SELECT * FROM "staging"."stg_Category"')

    # Merge Sub_Category
    df_merged = df_product.merge(
        df_sub_category,
        how='left',
        on='sub_category_id'
    )

    # Merge Category
    df_merged = df_merged.merge(
        df_category,
        how='left',
        on='category_id'
    )

    # Chọn và đổi tên cột khớp bảng dim_product
    dim_df = df_merged[['product_id', 'product_name', 'sub_category_name', 'category_name']].copy()
    dim_df.columns = ['product_id', 'product_name', 'sub_category', 'category']

    # Làm sạch dữ liệu
    dim_df['product_id'] = dim_df['product_id'].astype(str).str.strip()
    dim_df['product_name'] = dim_df['product_name'].fillna('').str.strip()
    dim_df['sub_category'] = dim_df['sub_category'].fillna('Unknown').str.strip()
    dim_df['category'] = dim_df['category'].fillna('Unknown').str.strip()

    # Loại bỏ trùng lặp theo product_id
    dim_df = dim_df.drop_duplicates(subset=['product_id'])

    # Ghi dữ liệu vào schema warehouse
    warehouse_operator.save_data_to_postgres(
        df=dim_df,
        table_name='dim_product',
        schema='warehouse',
        if_exists='append'
    )

    print("✔ Đã transform và lưu dữ liệu vào warehouse.dim_product")
