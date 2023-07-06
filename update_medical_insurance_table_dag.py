import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import os
from parser.site_parser import start_parse_site

ch = BaseHook.get_connection("ClickHouse")
connection = {'host': ch.host, 'port': ch.port, 'database': ch.schema, 'user': ch.login, 'password': ch.password}

@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 7, 6, tz="UTC"),
    catchup=False,
    tags=['Парсер']
)
def medical_insurance_update_dag():
    """
    Функция update_medical_insurance_op является задачей в DAG. 
    Она вызывает функцию start_parse_site, которая выполняет парсинг сайта и обновляет данные в таблице medical_insurance в базе данных ClickHouse.
    Параметры подключения к базе данных передаются через переменные connection. В данном примере парсится 1000 строк данных.

    """
    @task()
    def update_medical_insurance_op(ds=None, **kwargs):
        start_parse_site('clickhouse+http://{user}:{password}@{host}:{port}/{database}?socket_timeout=3600000'.format(**connection), 
                         table_name = 'medical_insurance', 
                         how_many_rows_need = 1000)
        

    update_medical_insurance_op()
    

dag = medical_insurance_update_dag()
