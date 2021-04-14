TBL_1 = "orders"
FILTER_1 = "order_date"
COLS_1 = "order_id, order_date, order_status"

TBL_2 = "order_items"
FILTER_2 = "order_item_order_id"
COLS_2 = "order_item_order_id, order_item_product_id, order_item_subtotal "

class Query_Builder:

    def get_orders_by_date(self,date,cols=COLS_1,tbl=TBL_1,filter=FILTER_1):
        query = "SELECT {} FROM {} WHERE {} LIKE CONCAT('{}','%')".format(cols,tbl,filter,date)
        return query 

    def get_order_items_by_order_id(self,date):
        subquery = self.get_orders_by_date(date=date,cols='order_id')
        query = "SELECT {} FROM {} WHERE {} IN ({})".format(COLS_2,TBL_2,FILTER_2,subquery)
        return query

    def get_all_from_table(self,tbl):
        query = "SELECT * FROM {}".format(tbl)
        return query