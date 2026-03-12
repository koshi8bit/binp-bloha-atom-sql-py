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
    print(query)
    if query == "\n\n":
        return
    cursor.execute(query)
    for row in cursor:
        print(row)

def get_names(cursor):
    query = "SELECT * FROM \"Archive_val\""
    print(query)
    if query == "\n\n":
        return
    cursor.execute(query)
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
        get_names(cursor)
        # run_test(cursor, """
        #             SELECT *
        #             FROM "Archive_val"
        # """)

        # check_connection(cursor)
        # get_all_channels(cursor)
        # get_types(cursor)
        # get_data(cursor)
        # get_data_short(cursor)

#######################################################
#         run_test(cursor, """
#             SELECT * FROM "DBAVl_archIEC104_6_MAJ10AP02_XG01" WHERE "TM">'2026-02-06 04:03:04+03' AND "TM"<'2026-02-07 12:03:04+03';
# """)
#######################################################

    except Exception as error:
        print(error)

    finally:
        if connection:
            cursor.close()
            connection.close()

main()
