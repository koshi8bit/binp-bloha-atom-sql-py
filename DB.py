import psycopg2
import datetime
import pyperclip

class DB:
    def __init__(self, ip: str):
        self.connection = psycopg2.connect(
            host=ip,
            database="ArchRNF",
            user="binp",
            password="binp",
            port="5432"
        )

        self.cursor = self.connection.cursor()
        self.check_connection(self.cursor)

    def __del__(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()

    def check_connection(self, cursor):
        self.cursor.execute("SELECT version();")
        result = self.cursor.fetchall()
        print("Server version:", result)

    def get_data(self, kks_sql, width=10, precision=3,
                 date_begin=datetime.datetime.now().date(),
                 date_end=datetime.datetime.now().date(),
                 time_begin=datetime.time(hour=0, minute=0, second=0, microsecond=0),
                 time_end=datetime.time(hour=23, minute=59, second=59, microsecond=999),
                 condition="",
                 callback=None):

        begin = datetime.datetime.combine(date_begin, time_begin).strftime("%Y-%m-%d %H:%M:%S+03")
        end = datetime.datetime.combine(date_end, time_end).strftime("%Y-%m-%d %H:%M:%S+03")

        self.get_values(self.cursor,
                   f"""SELECT \"TM\",\"TMU\",\"VAL\",\"ALARM\" FROM "{kks_sql}" WHERE "TM">'{begin}' AND "TM"<'{end}'{condition};""",
                   width, precision, callback)

    def get_values(self, query, width=10, precision=3, callback=None):
        print("\n")
        print(query)
        if query == "\n\n":
            print("empty query")
            return

        self.cursor.execute(query)

        # for row in cursor:
        #     print(row)
        str_to_clipboard = f"{query}\n"
        threshold = 1_000_000
        for row in self.cursor:
            val = row[2]
            time_dt = row[0].replace(microsecond=row[1])
            alarm = " (!)" if row[3]==1.0 else ""
            callback_str = f"     {callback(val)}" if callback else ""
            f = 'f' if -threshold < val < threshold else 'e'
            tmp_str = f"{time_dt.strftime("%Y-%m-%d  %H:%M:%S.%f")[:-3]} {val:{width}.{precision}{f}}{alarm} {callback_str}"
            str_to_clipboard = str_to_clipboard + tmp_str + "\n"
            print(tmp_str)
        pyperclip.copy(str_to_clipboard)


##########################
    def get_all_channels(self):
        self.cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name LIKE 'DBAVl_archIEC104%';
            """)
        for row in self.cursor:
            print(row[0])

    def run_test(self, query):
        print("\n")
        print(query)
        if query == "\n\n":
            print("empty query")
            return

        self.cursor.execute(query)
        columns = [desc[0] for desc in self.cursor.description]
        print(columns)

        for row in self.cursor:
            print(row)
