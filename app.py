from mysql_reader import MySQL_Reader
from query_builder import Query_Builder
from argparse import ArgumentParser
from util import query_paramsDf
import logging
import extract
import transform

def main(date,tbl):
    reader = MySQL_Reader()
    query_builder = Query_Builder()
    is_to_be_loaded = query_paramsDf['to_be_loaded']=='True'   
    is_filter_col = query_paramsDf['filter_col']!='None'
    if date:
        has_subquery = query_paramsDf['subtables']!='None'
        query_type1 = query_paramsDf[is_filter_col & ~has_subquery]
        query_type2 = query_paramsDf[is_filter_col & has_subquery]

        for i,row in query_type1.iterrows():
            df1 = extract.extract_like_filter(query_builder,reader,row['columns'],row['tables'],row['filter_col'],date)
            
        for i,row in query_type2.iterrows():
            df2 = extract.extract_in_filter(
                query_builder,
                reader,
                row['sub_columns'],
                row['subtables'],
                row['subfilter_col'],
                date,
                row['columns'],
                row['tables'],
                row['filter_col']
                )
        products_rev_daily = transform.products_daily(df1,df2)
        orders_rev_daily = transform.orders_daily(df1,df2)
        print(products_rev_daily.head())
        print(orders_rev_daily.head())    

    if tbl:
        query_type3 = query_paramsDf[~is_filter_col]
        for i,row in query_type3.iterrows():
            df3 = extract.extract_select(query_builder,reader,row['columns'],row['tables'])


    reader.disconnect()
    return 0

if __name__=='__main__':
    parser = ArgumentParser()
    parser.add_argument("-d","--date",help="set date for getting orders and order_items")
    parser.add_argument("-t","--tables",action='store_true',help="get master tables products,customers,categories,departments")
    args = parser.parse_args()
    main(args.date,args.tables)
 