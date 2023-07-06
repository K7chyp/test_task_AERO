import pandas as pd
import requests
from sqlalchemy import create_engine

LINK = 'https://random-data-api.com/api/cannabis/random_cannabis'
PARAMS = {'size': 10}


def safty_get_link(link, params, *args, **kwargs):
    """
    - link (строка) - URL-адрес для отправки GET-запроса.
    - params (словарь) - параметры запроса.
    - *args - произвольное количество позиционных аргументов.
    - **kwargs - произвольное количество именованных аргументов.

    Описание функции:
    Функция отправляет GET-запрос по указанному URL-адресу с переданными параметрами. 
    Если код ответа находится в диапазоне от 200 до 301, то возвращается JSON-ответ. 
    Если после 5 попыток не удалось получить ответ с кодом в указанном диапазоне, то возвращается пустой словарь {}.
    """
    for _ in range(5):
        response = requests.get(link, params=params)
        if response.status_code in range(200, 301):
            return response.json()
    return {}


def start_parse_site(clickhouse_url, table_name, how_many_rows_need, save_path='', filename='result.csv', *args, **kwargs):
    """
        Функция start_parse_site принимает следующие аргументы:
        - clickhouse_url (строка) - URL-адрес для подключения к ClickHouse.
        - table_name (строка) - имя таблицы, в которую будут сохраняться данные.
        - how_many_rows_need (целое число) - количество строк, которое необходимо получить.
        - save_path (строка, по умолчанию '') - путь для сохранения результата.
        - filename (строка, по умолчанию 'result.csv') - имя файла для сохранения результата.
        - *args - произвольное количество позиционных аргументов.
        - **kwargs - произвольное количество именованных аргументов.

        Описание функции:
        Функция создает подключение к ClickHouse с использованием указанного URL-адреса. Затем инициализирует пустой DataFrame result_df. Далее, пока количество полученных строк меньше, чем требуемое количество строк, выполняется следующий цикл:
        1. Вызывается функция safty_get_link для получения данных.
        2. Если получены данные, то они сохраняются в таблицу ClickHouse с помощью метода to_sql из библиотеки Pandas.
        3. Обновляется переменная how_many_rows_got с количеством строк в result_df.
        4. Выводится текущий прогресс работы в формате "WORK {процент выполнения} %".
        5. После завершения цикла, выполняется закрытие подключения к ClickHouse.

    """
    engine = create_engine(clickhouse_url)
    result_df = pd.DataFrame()
    how_many_rows_got = 0
    while how_many_rows_need > how_many_rows_got:
        result = safty_get_link(LINK, PARAMS)
        if result:
            result_df = pd.DataFrame(result)
            result_df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
            how_many_rows_got += len(result_df)
            
        print(f"WORK {how_many_rows_got / how_many_rows_need} %")
    engine.dispose()



