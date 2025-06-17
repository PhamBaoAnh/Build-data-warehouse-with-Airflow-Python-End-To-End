from airflow.hooks.mysql_hook import MySqlHook
from support_processing import TemplateOperatorDB
import logging
from contextlib import closing
import os

class MySQLOperators:
    def __init__(self, conn_id="mysql"):
        try:
            self.mysqlhook = MySqlHook(mysql_conn_id=conn_id)
        except Exception as e:
            logging.error(f"Can't connect to {conn_id} database: {str(e)}")

    def get_data_to_pd(self, query=None):
        return self.mysqlhook.get_pandas_df(query)

    def get_records(self, query):
        return self.mysqlhook.get_records(query)

    def execute_query(self, query):
        try:
            with closing(self.mysqlhook.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.execute(query)
                    conn.commit()
        except Exception as e:
            logging.error(f"Can't execute query: {query} | Error: {str(e)}")

    def insert_data_into_table(self, table_name, data, create_table_like=""):
        if create_table_like:
            try:
                with closing(self.mysqlhook.get_conn()) as conn:
                    with closing(conn.cursor()) as cur:
                        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} LIKE {create_table_like};"
                        cur.execute(create_query)
                        conn.commit()
            except Exception as e:
                logging.error(f"Can't create table: {table_name} | Error: {str(e)}")
        try:
            self.mysqlhook.insert_rows(table_name, data)
        except Exception as e:
            logging.error(f"Can't insert data into {table_name} | Error: {str(e)}")

    def truncate_all_data_from_table(self, table_name):
        try:
            with closing(self.mysqlhook.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.execute(f"TRUNCATE TABLE {table_name};")
                    conn.commit()
        except Exception as e:
            logging.error(f"Can't truncate table {table_name} | Error: {str(e)}")
