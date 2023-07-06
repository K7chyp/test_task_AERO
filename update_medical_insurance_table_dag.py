import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import os
from parser.site_parser import start_parse_site

SAVE_PATH = os.getcwd() + "/data/"
SCHEMA = 'medical_base'
ch = BaseHook.get_connection("ClickHouse")
connection = {'host': ch.host, 'port': ch.port, 'database': ch.schema, 'user': ch.login, 'password': ch.password}

@dag(
    schedule_interval='@daily"',
    start_date=pendulum.datetime(2022, 7, 26, tz="UTC"),
    catchup=False,
    tags=['Парсер']
)
def medical_insurance_update_dag():
    """
    """
    @task()
    def update_medical_insurance_op(ds=None, **kwargs):
        start_parse_site('clickhouse+http://{user}:{password}@{host}:{port}/{database}?socket_timeout=3600000'.format(**connection), 
                         table_name = 'medical_insurance', 
                         how_many_rows_need = 1000)
        

    update_medical_insurance_op()
    

dag = medical_insurance_update_dag()