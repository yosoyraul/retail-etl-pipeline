

class Query_Builder:

    def select_where_like(self,cols,tbl,filter,date):
        query = "SELECT {} FROM {} WHERE {} LIKE CONCAT('{}','%')".format(cols,tbl,filter,date)
        return query 

    def select_where_in(self,cols,tbl,filter,subquery):
        query = "SELECT {} FROM {} WHERE {} IN ({})".format(cols,tbl,filter,subquery)
        return query

    def select(self,cols,tbl):
        query = "SELECT {} FROM {}".format(cols,tbl)
        return query

    def insert(self,cols,tbl):
        query = "INSERT INTO {} {} VALUES(%s)".format(tbl,cols)
        print(query)