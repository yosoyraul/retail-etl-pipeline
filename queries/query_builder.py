

class Query_Builder:
    def __init__(self,logger):
        self.logger = logger


    def select_where_like(self,cols,tbl,filter,date):
        query = "SELECT {} FROM {} WHERE {} LIKE CONCAT('{}','%')".format(cols,tbl,filter,date)
        self.logger.info(query)
        return query 

    def select_where_in(self,cols,tbl,filter,subquery):
        query = "SELECT {} FROM {} WHERE {} IN ({})".format(cols,tbl,filter,subquery)
        self.logger.info(query)
        return query

    def select(self,cols,tbl):
        query = "SELECT {} FROM {}".format(cols,tbl)
        self.logger.info(query)
        return query

    def insert(self,vals,cols,tbl):
        query = "INSERT INTO {}({}) VALUES({})".format(tbl,cols,vals)
        self.logger.info(query)
        return query