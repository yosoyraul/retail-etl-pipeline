import pandas as pd

def extract_orders(query_builder,reader,date):
        query = query_builder.get_orders_by_date(date)
        result = reader.query_with_fetchall(query)
        orders = pd.DataFrame(result,columns=['order_id','order_date','order_status'])
        return orders

def extract_order_items(query_builder,reader,date):
        query = query_builder.get_order_items_by_order_id(date)
        result = reader.query_with_fetchall(query)
        order_items = pd.DataFrame(result,columns=['order_item_order_id','order_item_product_id','order_item_subtotal'])
        return order_items

def extract_customers(query_builder,reader):
        query = query_builder.get_all_from_table("customers")
        result = reader.query_with_fetchall(query)
        customers = pd.DataFrame(result,
            columns=[
                'customer_id',
                'customer_fname',
                'customer_lname',
                'customer_email',
                'customer_password',
                'customer_street',
                'customer_city',
                'customer_state',
                'customer_zipcode']
                ) 
        return customers

def extract_products(query_builder,reader):
        query = query_builder.get_all_from_table("products")
        result = reader.query_with_fetchall(query)
        products = pd.DataFrame(result,
            columns=[
                'product_id',
                'product_category_id',
                'product_name',
                'product_description',
                'product_price',
                'product_image'
            ]
                ) 
        return products

def extract_categories(query_builder,reader):
        query = query_builder.get_all_from_table("categories")
        result = reader.query_with_fetchall(query)
        categories = pd.DataFrame(result,
            columns=[
                'category_id',
                'category_department_id'
                'category_name',
                                ]
                )
        return categories

def extract_departments(query_builder,reader):
        query = query_builder.get_all_from_table("departments")
        result = reader.query_with_fetchall(query)
        departments = pd.DataFrame(result,
            columns=[
                'department_id',
                'department_name',
                                ]
                )
        return departments   