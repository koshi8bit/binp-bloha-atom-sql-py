import psycopg2


def get_all_channels(cursor):
    cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
        """)
    for row in cursor:
        print(row)


def check_connection(cursor):
    cursor.execute("SELECT version();")
    result = cursor.fetchall()
    print("Server version:", result)

def run_test(cursor, query):
    cursor.execute(query)
    for row in cursor:
        print(row)


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

#######################################################
        run_test(cursor, """
            SELECT table_name
            FROM information_schema.tables
""")
#######################################################
        cursor.execute(
            "SELECT * FROM \"DBAVl_archIEC104_6_JKA20CE01_XQ01\" WHERE \"TM\">'2026-02-06 04:03:04+03' AND \"TM\"<'2026-02-07 12:03:04+03';")
        for row in cursor:
            print(row)

    except Exception as error:
        print("Ошибка подключения:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()


main()
