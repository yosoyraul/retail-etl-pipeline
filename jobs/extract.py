import pandas as pd

def extract_like_filter(query_builder,reader,cols,tbl,filter,date):
        fields = ','.join(cols)
        query = query_builder.select_where_like(fields,tbl,filter,date)
        result = reader.query_with_fetchall(query)
        df = pd.DataFrame(result,columns=cols)
        return df

def extract_in_filter(query_builder,reader,col,tbl1,filter1,date,cols,tbl2,filter2):
        fields2 = ','.join(cols)
        subquery =query_builder.select_where_like(col,tbl1,filter1,date)
        query = query_builder.select_where_in(fields2,tbl2,filter2,subquery)
        result = reader.query_with_fetchall(query)
        df = pd.DataFrame(result,columns=cols)
        return df

def extract_select(query_builder,reader,cols,tbl):
        fields = ','.join(cols)
        query = query_builder.select(fields,tbl)
        result = reader.query_with_fetchall(query)
        df = pd.DataFrame(result,columns= cols) 
        return df

