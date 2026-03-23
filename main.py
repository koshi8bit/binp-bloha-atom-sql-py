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


def get_values(cursor, query, width=10, precision=3, callback=None):
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
        alarm = " (!)" if row[3]==1.0 else ""
        callback_str = callback(val) if callback else ""
        f = 'f' if -threshold < val < threshold else 'e'
        tmp_str = f"{time_dt.strftime("%Y-%m-%d  %H:%M:%S.%f")[:-3]} {val:{width}.{precision}{f}}{alarm} {callback_str}"
        str_to_clipboard = str_to_clipboard + tmp_str + "\n"
        print(tmp_str)
    pyperclip.copy(str_to_clipboard)



def get_data(cursor, kks_sql, width=10, precision=3,
             date_begin=datetime.datetime.now().date(),
             date_end=datetime.datetime.now().date(),
             time_begin=datetime.time(hour=0, minute=0, second=0, microsecond=0),
             time_end=datetime.time(hour=23, minute=59, second=59, microsecond=999),
             callback=None):

    begin = datetime.datetime.combine(date_begin, time_begin).strftime("%Y-%m-%d %H:%M:%S+03")
    end = datetime.datetime.combine(date_end, time_end).strftime("%Y-%m-%d %H:%M:%S+03")

    get_values(cursor, f"""SELECT \"TM\",\"TMU\",\"VAL\",\"ALARM\" FROM "{kks_sql}" WHERE "TM">'{begin}' AND "TM"<'{end}';""", width, precision, callback)


def parce_paramerus_status(val):
    return str(val)


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
        # get_types(cursor)
        get_data(cursor, kks_to_sql("LVC60CE01_XQ01"), 12, 0,
                 # date_begin=datetime.date(year=2026, month=3, day=22),
                 # date_end=datetime.date(year=2026, month=3, day=22),
                 time_begin=datetime.time(hour=17, minute=45, second=00, microsecond=0),
                 # time_end = datetime.time(hour=14, minute=28, second=00, microsecond=0),
                 callback=parce_paramerus_status,
                 )
#####################################################
#         get_values(cursor, f"""
# SELECT "TM","TMU","VAL","ALARM" FROM "DBAVl_archIEC104_1_HVC20CE01_XQ01" WHERE "TM">'2026-03-21 14:30:40+03' AND "TM"<'2026-03-21 23:59:59+03' AND "VAL">2 AND "VAL"<3
# """)
######################################################

    except Exception as error:
        print(error)

    finally:
        if connection:
            cursor.close()
            connection.close()


main()
