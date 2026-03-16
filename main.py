import psycopg2
import datetime


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
    for row in cursor:
        print(row[0].isoformat(timespec="milliseconds"), f"{row[1]:{width}.{precision}f}")


def get_today_data(cursor, kks, width=10, precision=3):
    begin = (datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
             .strftime("%Y-%m-%d %H:%M:%S+03"))
    end = (datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=999)
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
        get_today_data(cursor, "DBAVl_archIEC104_6_MAG01CE01_XQ01", 10, 2)
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
