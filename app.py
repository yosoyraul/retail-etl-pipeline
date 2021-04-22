from mysql_reader import MySQL_Reader
from psql_writer import PSQL_Writer
from query_builder import Query_Builder
from argparse import ArgumentParser
from util import source_paramsDf, target_paramsDf
import load
import logging
import extract
import transform

def main(date,tbl):
    formatter = logging.Formatter('%(asctime)s [%(message)s]')
    logger = logging.getLogger('retail-etl-info')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    fh = logging.FileHandler('retail-etl.log')
    logger.addHandler(fh)
    logger.addHandler(ch)
    reader = MySQL_Reader(logger)
    writer = PSQL_Writer(logger)
    query_builder = Query_Builder(logger)
    is_to_be_loaded = source_paramsDf['to_be_loaded']=='True'   
    has_filter_col = source_paramsDf['filter_col']!='None'
    if date:
        has_subquery = source_paramsDf['subtables']!='None'
        orders_query = source_paramsDf[has_filter_col & ~has_subquery]
        orderItems_query = source_paramsDf[has_filter_col & has_subquery]

        for i,row in orders_query.iterrows():
            logging.info(f"Reading data from {row['tables']}")
            ordersDf = extract.extract_like_filter(query_builder,reader,row['columns'],row['tables'],row['filter_col'],date)
            
        for i,row in orderItems_query.iterrows():
            logger.info(f"Reading data from {row['tables']}")
            orderItemsDf = extract.extract_in_filter(
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
        res = {}
        res['products_revenue_daily_fact'] = transform.products_daily(ordersDf,orderItemsDf)
        res['orders_revenue_daily_fact'] = transform.orders_daily(ordersDf,orderItemsDf)  
        for i,row in target_paramsDf.iterrows():
            if row['tables'] in res:
                logger.info(f"Writing data to {row['tables']}")
                load.load_insert(query_builder,writer,row['columns'],row['tables'],res[row['tables']])

    if tbl:
        dfs = {}
        res = {}
        query_type3 = source_paramsDf[~has_filter_col]
        for i,row in query_type3.iterrows():
                logger.info(f"Reading data from {row['tables']}")
                dfs[row['tables']] = extract.extract_select(query_builder,reader,row['columns'],row['tables'])

        res['customers_dim'] = transform.customers_master(dfs['customers'])

        productsDf = dfs['products']
        categoriesDf = dfs['categories']
        departmentsDf = dfs['departments']
        res['products_dim'] = transform.products_master(productsDf,categoriesDf,departmentsDf)
        for i,row in target_paramsDf.iterrows():
            if row['tables'] in res:
                logger.info(f"Writing data to {row['tables']}")
                load.load_bulk(writer,row['columns'],row['tables'],res[row['tables']])

    writer.disconnect()
    reader.disconnect()
    return 0

if __name__=='__main__':
    parser = ArgumentParser()
    parser.add_argument("-d","--date",help="set date for getting orders and order_items")
    parser.add_argument("-t","--tables",action='store_true',help="get master tables products,customers,categories,departments")
    args = parser.parse_args()
    main(args.date,args.tables)
 