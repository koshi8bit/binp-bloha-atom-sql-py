import psycopg2
import datetime
import pyperclip

def get_all_channels(cursor):
    cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE 'DBAVl_archIEC104%';
        """)
    for row in cursor:
        print(row)


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

def get_data(cursor):
    t = "SELECT * FROM \"DBAVl_archIEC104_6_JKA20CE01_XQ01\" WHERE \"TM\">'2026-02-06 04:03:04+03' AND \"TM\"<'2026-02-07 12:03:04+03';"
    print(t)
    cursor.execute(t)
    for row in cursor:
        print(row)

def get_data_short(cursor):
    t = "SELECT \"TM\",\"VAL\" FROM \"DBAVl_archIEC104_6_JKA20CE01_XQ01\" WHERE \"TM\">'2026-02-06 04:03:04+03' AND \"TM\"<'2026-02-07 12:03:04+03';"
    # SELECT "TM","VAL" FROM "DBAVl_archIEC104_6_JKA20CE01_XQ01" WHERE "TM">'2026-02-06 04:03:04+03' AND "TM"<'2026-02-07 12:03:04+03';
    print(t)
    cursor.execute(t)
    columns = [desc[0] for desc in cursor.description]
    print(columns)
    for row in cursor:
        print(row)

def run_test(cursor, query, width=10, precision=3):
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
        val = row[1]
        f = 'f' if -threshold < val < threshold else 'e'
        tmp_str = f"{row[0].isoformat(timespec="milliseconds")} {val:{width}.{precision}{f}}"
        str_to_clipboard = str_to_clipboard + tmp_str + "\n"
        print(tmp_str)
    pyperclip.copy(str_to_clipboard)



def get_today_data(cursor, kks, width=10, precision=3,
                   time_begin=datetime.time(hour=0, minute=0, second=0, microsecond=0),
                   time_end=datetime.time(hour=23, minute=59, second=59, microsecond=999)):

    now = datetime.datetime.now()
    begin = (now.replace(
        hour=time_begin.hour, minute=time_begin.minute, second=time_begin.second, microsecond=time_begin.microsecond)
        .strftime("%Y-%m-%d %H:%M:%S+03"))
    end = (now.replace(
        hour=time_end.hour, minute=time_end.minute, second=time_end.second, microsecond=time_end.microsecond)
        .strftime("%Y-%m-%d %H:%M:%S+03"))

    run_test(cursor, f"""SELECT \"TM\",\"VAL\" FROM "{kks}" WHERE "TM">'{begin}' AND "TM"<'{end}';""", width, precision)


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
        get_all_channels(cursor)
        # get_types(cursor)
        # get_data(cursor)
        # get_data_short(cursor)
        get_today_data(cursor, "DBAVl_archIEC104_6_MAG01CE01_XQ01", 12, 1,
                       # time_begin=datetime.time(hour=12, minute=46, second=40, microsecond=0),
                       # time_end=datetime.time(hour=12, minute=47, second=0, microsecond=0)
        )
######################################################
#         run_test(cursor, f"""
# SELECT \"TM\",\"VAL\" FROM "DBAVl_archIEC104_2_MAJ30CP01_XQ01" WHERE "TM">'2026-03-11 09:03:04+03' AND "TM"<'2026-03-11 23:03:04+03';
# """)
######################################################

    except Exception as error:
        print(error)

    finally:
        if connection:
            cursor.close()
            connection.close()





main()
