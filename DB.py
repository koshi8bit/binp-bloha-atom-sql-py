import psycopg2

class DB:
    def __init__(self, ip: str = "192.168.1.2"):
        self.connection = psycopg2.connect(
            host=ip,
            database="ArchRNF",
            user="binp",
            password="binp",
            port="5432"
        )

        self.cursor = self.connection.cursor()
        self.check_connection(self.cursor)

    def check_connection(self, cursor):
        self.cursor.execute("SELECT version();")
        result = self.cursor.fetchall()
        print("Server version:", result)
