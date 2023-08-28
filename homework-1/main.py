"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2

conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='11092003')
try:
    with conn:
        with conn.cursor() as cur:
            # Считываем данные из customers_data.csv
            with open('north_data/customers_data.csv') as f:
                reader = list(csv.reader(f))

                # Заполняем данными таблицу customers
                for row in range(1, len(reader)):
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", (reader[row][0], reader[row][1], reader[row][2]))

            # Считываем данные из employees_data.csv
            with open('north_data/employees_data.csv') as f:
                reader = list(csv.reader(f))

                # Заполняем данными таблицу employees
                for row in range(1, len(reader)):
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (reader[row][0], reader[row][1], reader[row][2], reader[row][3], reader[row][4], reader[row][5],))

            # Считываем данные из orders_data.csv
            with open('north_data/orders_data.csv') as f:
                reader = list(csv.reader(f))

                # Заполняем данными таблицу orders
                for row in range(1, len(reader)):
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (reader[row][0], reader[row][1], reader[row][2], reader[row][3], reader[row][4]))

except Exception as error:
    raise Exception('Возникла ошибка при подключении к базе данных.')
finally:
    conn.close()
