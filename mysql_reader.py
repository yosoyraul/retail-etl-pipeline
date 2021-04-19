from mysql.connector import connect, Error
from config import read_db_config

class MySQL_Reader:

    def __init__(self):
        self.conn = self.connect()

    def connect(self):
        try:
            dbconfig = read_db_config(section='mysql')
            conn = connect(**dbconfig,use_pure=True)
            if conn.is_connected():
                logging.info('Connected to MySQL database')
        except Error as e:
            logging.error(e)
        return conn

    def test_connection(self):
        try:
            if self.conn.is_connected():
                logging.info("Connected to MySQL database")
        except Error as e:
            logging.error(e)
        
    def disconnect(self):
        try:
            if self.conn is not None and self.conn.is_connected():
                self.conn.close()
                logging.info('Disconnected from MySQL database')
        except Error as e:
            logging.error(e)


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
            logging.error(e)

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
            logging.error(e)

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
            logging.error(e)

        finally:
            cursor.close()

        return result_list
