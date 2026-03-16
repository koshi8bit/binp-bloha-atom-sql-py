import psycopg2
import datetime
import pyperclip
from kks import kks_to_sql

def get_all_channels(cursor):
    cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE 'DBAVl_archIEC104%';
        """)
    for row in cursor:
        print(row[0])


def check_connection(cursor):
    cursor.execute("SELECT version();")
    result = cursor.fetchall()
    print("Server version:", result)

def get_types(cursor):
    run_test(cursor, """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'DBAVl_archIEC104_6_JKA20CE01_XQ01'     
""")

def run_test(cursor, query):
    print("\n")
    print(query)
    if query == "\n\n":
        print("empty query")
        return

    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    print(columns)

    for row in cursor:
        print(row)


def get_values(cursor, query, width=10, precision=3):
    print("\n")
    print(query)
    if query == "\n\n":
        print("empty query")
        return

    cursor.execute(query)

    # for row in cursor:
    #     print(row)
    str_to_clipboard = f"{query}\n"
    threshold = 1_000_000
    for row in cursor:
        val = row[2]
        time_dt = row[0].replace(microsecond=row[1])
        alarm = "(!)" if row[0]==1 else ""
        f = 'f' if -threshold < val < threshold else 'e'
        # tmp_str = f"{time_dt.isoformat(timespec="milliseconds")} {val:{width}.{precision}{f}}"
        tmp_str = f"{time_dt.strftime("%Y-%m-%d  %H:%M:%S.%f")[:-3]} {val:{width}.{precision}{f}} {alarm}"
        str_to_clipboard = str_to_clipboard + tmp_str + "\n"
        print(tmp_str)
    pyperclip.copy(str_to_clipboard)



def get_data(cursor, kks_sql, width=10, precision=3,
             date_begin=datetime.datetime.now().date(),
             date_end=datetime.datetime.now().date(),
             time_begin=datetime.time(hour=0, minute=0, second=0, microsecond=0),
             time_end=datetime.time(hour=23, minute=59, second=59, microsecond=999)):

    begin = datetime.datetime.combine(date_begin, time_begin).strftime("%Y-%m-%d %H:%M:%S+03")
    end = datetime.datetime.combine(date_end, time_end).strftime("%Y-%m-%d %H:%M:%S+03")

    get_values(cursor, f"""SELECT \"TM\",\"TMU\",\"VAL\",\"ALARM\" FROM "{kks_sql}" WHERE "TM">'{begin}' AND "TM"<'{end}';""", width, precision)


def main():
    try:
        # Подключение к серверу
        connection = psycopg2.connect(
            host="192.168.1.2",
            database="ArchRNF",
            user="binp",
            password="binp",
            port="5432"
        )

        cursor = connection.cursor()

        check_connection(cursor)
        # get_all_channels(cursor)
        get_types(cursor)
        get_data(cursor, kks_to_sql("MAG01CE01_XQ"), 12, 1,
                 # date_begin=datetime.date(year=2026, month=3, day=15),
                 # date_end=datetime.date(year=2026, month=3, day=15),
                 # time_begin=datetime.time(hour=12, minute=46, second=40, microsecond=0),
                 # time_end=datetime.time(hour=12, minute=47, second=0, microsecond=0),
                 )
######################################################
#         run_test(cursor, f"""
# SELECT * FROM "DBAVl_archIEC104_6_MAG01GW11_XG01" WHERE "TM">'2026-03-16 00:00:00+03' AND "TM"<'2026-03-16 23:59:59+03';
# """)
######################################################

    except Exception as error:
        print(error)

    finally:
        if connection:
            cursor.close()
            connection.close()


main()
