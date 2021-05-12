from config import read_db_config
from psycopg2 import connect,Error

class PSQL_Writer:

    def __init__(self,logger):
        self.logger = logger
        self.conn = self.connect()
        


    def connect(self):
        try:
            dbconfig = read_db_config(section='postgresql')
            conn = connect(**dbconfig)
            if conn is not None:
                self.logger.info('Connected to PSQL database')
        except Error as e:
            self.logger.error(e)
        return conn

    def test_connection(self):
        try:
            if self.conn is not None:
                self.logger.info("Connected to PSQL database")
        except Error as e:
            self.logger.error(e)
        
    def disconnect(self):
        try:
            if self.conn is not None:
                self.conn.close()
                self.logger.info('Disconnected from PSQL database')
        except Error as e:
            self.logger.error(e)

    def query_executemany(self,query,lists):
        try:
            cursor = self.conn.cursor()
            cursor.executemany(query,lists)
            self.conn.commit()
 
        except Error as e:
            self.logger.error(e)
            self.conn.rollback()

        finally:
            cursor.close()

    def copy_from_file(self,f,cols,tbl):
        try:
            cursor = self.conn.cursor()
            cursor.copy_from(f,tbl,sep="\t",columns=cols)
            self.conn.commit()
        except Error as e:
            self.logger.error(e)
            self.conn.rollback()
        finally:
            cursor.close()
