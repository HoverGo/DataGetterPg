import psycopg2
import pandas as pd
import json
from decimal import Decimal


def open_connection(dbname, user, password, host, port):
    """

    Подключение к базе данных
    Для подключения необходимо ввести все входные данные

    """
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        return conn
    except:
        return None


def close_connection(conn) -> bool:
    """
    Закрытие соединения с базой данных

    """
    try:
        conn.close()
        return True
    except:
        return False


def get_tablenames(cursor) -> list:
    """
    Обращание к информационной схеме базы данных для получения
    названий всех таблиц

    """

    # Выполнение SQL-инструкции
    cursor.execute(
        """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
    )
    # Получение списка кортежей с названием всех таблиц в базе данных
    tablenames_data = cursor.fetchall()

    # т.к. вывод fetchall представляет собой кортеж, необходимо получить из него нулевой и едниственный элемент с названием таблицы
    tablenames = [tablename[0] for tablename in tablenames_data]
    return tablenames


def get_data_from_tables(cursor, tablenames: list) -> dict:
    """
    Получение данных из всех таблиц в базе данных

    """
    all_data = {}

    for tablename in tablenames:
        """
        Получение всех строк из таблицы в базе данных
        Каждый элемент является кортежем, который представляет собой одну строку из таблицы
        """
        cursor.execute(f"SELECT * FROM {tablename}")
        records = cursor.fetchall()
        # Получение названий столбцов текущей выбранной таблицы
        column_names = [desc[0] for desc in cursor.description]
        # Создание датафрейма на основе данных и названий столбцов (аналог таблицы)
        df = pd.DataFrame(records, columns=column_names)
        # orient="records" преобразовывает данные в список словарей, где каждый словарь - одна строка
        all_data[tablename] = df.to_dict(orient="records")

    return all_data


def write_tabledata_to_json_file(filename, data: dict):
    """
    Записывает данные в заданный файл
    """
    with open(filename, "w", encoding="utf-8") as f:
        """
        ensure_ascii = False - символы выходящие за пределы кодировки не будут преобразованы
        indent = 4 - количество пробелов для отступов в json-файле
        default - определяет, как должны обрабатываться объекты, которые не могут быть сериализованы JSON по умолчанию,
        как например объекты Decimal
        """
        json.dump(data, f, ensure_ascii=False, indent=4, default=decimal_default)


def decimal_default(obj):
    """
    Проверяет является ли объект типом Decimal и если так, то преобразует его в float, а иначе в str

    """
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)


def main():
    """
    Основная функция для работы с программой

    """

    conn = open_connection("computer", "postgres", "SideGame", "Localhost", "5432")

    if conn:
        print("Подключение к базе данных выполнено успешно")

        cursor = conn.cursor()

        tablenames = get_tablenames(cursor)

        all_data = get_data_from_tables(cursor, tablenames)

        write_tabledata_to_json_file("data.json", all_data)

        close_connection(conn)
        cursor.close()
    else:
        print("Не удалось подключиться к базе данных")


if __name__ == "__main__":
    main()
