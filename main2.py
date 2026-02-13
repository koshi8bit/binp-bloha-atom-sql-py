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
    if query == "\n\n":
        return
    cursor.execute(query)
    for row in cursor:
        print(row)

def get_types(cursor):
    run_test(cursor, """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'DBAVl_archIEC104_6_JKA20CE01_XQ01'     
""")

def get_data(cursor):
    cursor.execute(
        "SELECT * FROM \"DBAVl_archIEC104_6_JKA20CE01_XQ01\" WHERE \"TM\">'2026-02-06 04:03:04+03' AND \"TM\"<'2026-02-07 12:03:04+03';")
    for row in cursor:
        print(row)

def get_data_short(cursor):
    cursor.execute(
        "SELECT \"TM\",\"VAL\" FROM \"DBAVl_archIEC104_6_JKA20CE01_XQ01\" WHERE \"TM\">'2026-02-06 04:03:04+03' AND \"TM\"<'2026-02-07 12:03:04+03';")
    columns = [desc[0] for desc in cursor.description]
    print(columns)
    for row in cursor:
        print(row)


def main():
    try:
        # Подключение к серверу
        connection = psycopg2.connect(
            host="192.168.1.2",
            database="UPN276_prm",
            user="binp",
            password="binp",
            port="5432"
        )

        cursor = connection.cursor()
        run_test(cursor, """
                    SELECT *
                    FROM "Archive_val"
        """)

        # check_connection(cursor)
        # get_all_channels(cursor)
        # get_types(cursor)
        # get_data(cursor)
        # get_data_short(cursor)

#######################################################
        run_test(cursor, """
            SELECT *
            FROM "Archive_val"
""")
#######################################################

    except Exception as error:
        print(error)

    finally:
        if connection:
            cursor.close()
            connection.close()





main()
