from datetime import datetime, timedelta
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import os

logger = logging.getLogger(__name__)

def start_func():
    logger.info("DAG started")

def load_csv_to_postgres(file_name, table_name, primary_key, **context):
    csv_path = f"/opt/airflow/data/{file_name}"
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Файл не найден: {csv_path}")
        

        df = None
        for enc in ['cp1251', 'windows-1251', 'latin1', 'utf-8']:
            try:
                df = pd.read_csv(csv_path, sep=',', encoding=enc)
                logger.info(f"Файл {csv_path} прочитан с кодировкой {enc}")
                break
            except UnicodeDecodeError:
                continue
        else:
            raise UnicodeDecodeError(f"Не удалось прочитать файл {csv_path} ни в одной из кодировок")
        
        logger.info(f"Файл {csv_path} прочитан")
                
        target_columns = list(df.columns)
        logger.info(f"Найдены колонки в файле: {target_columns}")
        logger.info(f"Прочитано {len(df)} строк из {csv_path}")

        records = [tuple(None if pd.isna(val) else val for val in row) 
                   for row in df[target_columns].itertuples(index=False)]

        columns_str = ', '.join(target_columns)
        where_conditions = ' AND '.join([f"{pk} = %s" for pk in primary_key])

        inserted_count = 0
        for record in records:

            pk_values = []
            for pk in primary_key:
                idx = target_columns.index(pk)
                pk_values.append(record[idx])

            check_sql = f"""
                    select count(*) from {table_name}
                    where {where_conditions}
            """
            
            result = pg_hook.get_first(check_sql, parameters=tuple(pk_values))
            exists = result[0] > 0

            if not exists:
                insert_sql = f"""
                    INSERT INTO {table_name} ({columns_str})
                    SELECT {', '.join(['%s'] * len(target_columns))}
                """
                pg_hook.run(insert_sql, parameters=record)
                inserted_count += 1
        
        logger.info(f"Таблица {table_name}: загружено {inserted_count} записей")
            
    except Exception as e:
        logger.error(f"Ошибка при загрузке {csv_path}: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='task2.2_deal_product_to_postgresql_dag',
    default_args=default_args,
    schedule='@once',
    catchup=False,
) as dag:
    
    start = PythonOperator(
        task_id="start",
        python_callable=start_func
    )
    
    load_deal = PythonOperator(
        task_id="load_deal_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={"file_name": "deal_info.csv", "table_name": "rd.deal_info", "primary_key": ["deal_rk", "effective_from_date"]}
    )
    
    load_product = PythonOperator(
        task_id="load_product_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={"file_name": "product_info.csv", "table_name": "rd.product", "primary_key": ["product_rk", "effective_from_date"]}
    )

    start >> load_deal >> load_product