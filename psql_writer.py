from config import read_db_config
from psycopg2 import connect,Error

class PSQL_Writer:

    def __init__(self):
        self.conn = self.connect()

    def connect(self):
        try:
            dbconfig = read_db_config(section='postgresql')
            conn = connect(**dbconfig)
            if conn is not None:
                print('Connected to PSQL database')
        except Error as e:
            print(e)
        return conn

    def test_connection(self):
        try:
            if self.conn is not None:
                print("Connected to PSQL database")
        except Error as e:
            print(e)
        
    def disconnect(self):
        try:
            if self.conn is not None:
                self.conn.close()
                print('Disconnected from PSQL database')
        except Error as e:
            print(e)


    def iter_row(self,cursor, fetch_size):
        while True:
            rows = cursor.fetchmany(fetch_size)
            if not rows:
                break
            for row in rows:
                yield row


    def query_with_fetchmany(self,query,fetch_size=10):

        try:
            cursor = self.conn.cursor()
            cursor.execute(query)

            result_list = [row for row in iter_row(cursor, fetch_size)]


        except Error as e:
            print(e)

        finally:
            cursor.close()
            self.disconnect(self.conn)
        
        return result_list

    def query_with_fetchone(self,query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query,)

            row = cursor.fetchone()
            result_list = []
            while row is not None:
                result_list.append(row)
                row = cursor.fetchone()

        except Error as e:
            print(e)

        finally:
            cursor.close()

        return result_list

    def query_with_fetchall(self,query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            result_list = [row for row in rows]


        except Error as e:
            print(e)

        finally:
            cursor.close()

        return result_list
