import csv
import datetime as dt
from memory_profiler import profile
import pandas as pd
import sqlite3


def create_table_to_db(db):
    """Функция создает пустую таблицу с полями: timestamp, player_id, event_id,
    error_id, json_server, json_client"""
    try:
        sqlite_connection = sqlite3.connect(db)
        sqlite_create_table_query = '''CREATE TABLE result (
                                    timestamp INTEGER NOT NULL,
                                    player_id INTEGER NOT NULL,
                                    event_id INTEGER NOT NULL,
                                    error_id TEXT NOT NULL UNIQUE,
                                    json_server TEXT NOT NULL,
                                    json_client TEXT NOT NULL);'''

        cursor = sqlite_connection.cursor()
        print("База данных подключена к SQLite")
        cursor.execute(sqlite_create_table_query)
        sqlite_connection.commit()
        print("Таблица SQLite создана")

        cursor.close()

    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


def get_timestamp_limits_from_date(date: str):
    """Функция получает на вход дату в формате 'ГГГГ-ММ-ДД' и возвращает
    кортеж с минимальным и максимальным значением 'timestamp'"""
    date_start = dt.datetime.strptime(date, "%Y-%m-%d")
    date_stop = date_start + dt.timedelta(days=1)
    min_value = int(dt.datetime.timestamp(date_start))
    max_value = int(dt.datetime.timestamp(date_stop))
    return min_value, max_value


def upload_events_from_csv_files(date, file_path):
    """Функция загружает журнал событий из csv-файла за определенную дату
    и возвращает данные класса 'Dict'"""
    try:
        with open(file_path, 'r', newline='') as csv_file:
            reader = csv.reader(csv_file, delimiter=',')
            minimum, maximum = get_timestamp_limits_from_date(date)
            column_names = reader.__next__()
            reader = tuple(
                el for el in reader if minimum <= int(el[0]) <= maximum)
            dct = {column_names[i]: [el[i] for el in reader]
                   for i in range(len(column_names))}
            return dct
    except FileNotFoundError:
        print('csv файл не найден!')


def get_merged_table(date, file_path_1, file_path_2):
    """Функция конвертирует данные из двух массивов 'Dict' в объекты
    'DataFrame', объединяет их по полю по error_id
    и возвращает объект 'DataFrame'"""
    server = pd.DataFrame(upload_events_from_csv_files(date, file_path_1))
    client = pd.DataFrame(upload_events_from_csv_files(date, file_path_2))
    merged_table = pd.merge(server, client, on=['error_id', 'timestamp'])
    return merged_table


def get_cheaters_on_before_day(db, date):
    """Функция загружает данные из таблицы 'cheaters'
    за определенный период времени и возвращает объект 'DataFrame'"""
    minimum = dt.datetime.fromtimestamp(
        get_timestamp_limits_from_date(date)[0])
    try:
        sqlite_connection = sqlite3.connect(db)
        print('База данных подключена к SQLite')
        sqlite_query = f"SELECT player_id FROM cheaters " \
                       f"WHERE ban_time < '{minimum}'"
        cheaters = pd.read_sql_query(sqlite_query, sqlite_connection)
        print('Данные из cheaters загружены')

    except sqlite3.Error as error:
        print('Ошибка при подключении к SQLite', error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print('Соединение с SQLite закрыто')
    print(cheaters.info())
    return cheaters


def get_result_table(table_1, table_2):
    """Функция получает на вход два 'DataFrame' и возвращает отфильтрованный
    'DataFrame' по полю 'player_id'"""
    result_table = table_1[-table_1['player_id'].apply(int).isin(table_2['player_id'])]
    table_1.info()
    result_table.info()
    return result_table


def write_result_table_to_database(db, table):
    """Функция записывает результат в БД в таблицу 'result'"""
    try:
        sqlite_connection = sqlite3.connect(db)
        sqlite_query = '''INSERT INTO result (
                       timestamp, event_id, error_id,
                       json_server, player_id, json_client)
                       VALUES (?, ?, ?, ?, ?, ?);'''

        cursor = sqlite_connection.cursor()
        print('База данных подключена к SQLite')
        elements = [tuple(table.values[i]) for i in range(len(table))]
        cursor.executemany(sqlite_query, elements)
        sqlite_connection.commit()
        print('Данные успешно загружены')
        cursor.close()

    except sqlite3.Error as error:
        print('Ошибка при выгрузке данных в SQLite', error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print('Соединение с SQLite закрыто')


@profile
def main():
    date = '2021-02-20'
    db = "cheaters.db"
    server = 'server.csv'
    client = 'client.csv'
    create_table_in_db(db)
    merged_table = get_merged_table(date, server, client)
    cheaters = get_cheaters_on_before_day(db, date)
    result_table = get_result_table(merged_table, cheaters)
    write_result_table_to_database(db, result_table)


if __name__ == '__main__':
    main()
