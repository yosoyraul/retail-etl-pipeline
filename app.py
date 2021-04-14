from mysql_reader import MySQL_Reader
from query_builder import Query_Builder
from argparse import ArgumentParser
import extract
import transform

def main(date,tbl):
    reader = MySQL_Reader()
    query_builder = Query_Builder()

    if date:
        orders = extract.extract_orders(query_builder,reader,date)
        order_items = extract.extract_order_items(query_builder,reader,date)
        products_rev_daily = transform.products_daily(orders,order_items)
        orders_rev_daily = transform.orders_daily(orders,order_items)
    if tbl:
        customers = extract.extract_customers(query_builder,reader)
        products = extract.extract_products(query_builder,reader)
        categories = extract.extract_categories(query_builder,reader)
        departments = extract.extract_departments(query_builder,reader)


    reader.disconnect()
    return 0

if __name__=='__main__':
    parser = ArgumentParser()
    parser.add_argument("-d","--date",help="set date for getting orders and order_items")
    parser.add_argument("-t","--tables",action='store_true',help="get master tables products,customers,categories,departments")
    args = parser.parse_args()
    main(args.date,args.tables)
 