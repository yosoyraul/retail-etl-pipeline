import pandas as pd
from functools import reduce

def get_product_revenue(df):
    is_complete = df['order_status']=='COMPLETE'
    is_closed = df['order_status']=='CLOSED'
    result = df[is_complete | is_closed]. \
        groupby(by=['date_id','product_id']).agg({'order_item_subtotal':'sum'}).round(2)
    result.rename(columns = {
        'order_item_subtotal':'product_revenue'
        },inplace=True)
    return result

def get_product_outstanding(df):
    is_pending = df['order_status']=='PENDING'
    is_pending_payment = df['order_status']=='PENDING_PAYMENT'
    is_processing = df['order_status']=='PROCESSING'
    result = df[is_pending | is_processing | is_pending_payment]. \
        groupby(by=['date_id','product_id']).agg({'order_item_subtotal':'sum'}).round(2)
    result.rename(columns={
        'order_item_subtotal':'outstanding_revenue'
    },inplace=True)
    return result    

def products_daily(orders,orderItems):
    orders_join_orderItems = pd.merge(orders,orderItems,left_on=['order_id'],right_on=['order_item_order_id'])
    orders_join_orderItems.rename(columns={
        'order_date':'date_id',
        'order_item_product_id':'product_id'
    },inplace=True)
    revenue = get_product_revenue(orders_join_orderItems)
    outstanding = get_product_outstanding(orders_join_orderItems)
    result = pd.merge(revenue,outstanding,left_index=True,right_index=True,how='outer')
    result['product_revenue']=result['product_revenue'].fillna(0.00)
    result['outstanding_revenue']=result['outstanding_revenue'].fillna(0.00)
    result.reset_index(inplace=True)
    result['date_id'] = result['date_id'].apply(lambda x: x.strftime("%Y%m%d"))
    result['batch_date'] = pd.to_datetime('now').replace(microsecond=0)
    return result

def get_total_order_count(df):
    result = df.groupby('date_id').order_id.nunique().to_frame()
    result.rename(columns={'order_id':'total_order_count'},inplace=True)
    return result

def get_revenue_order_count(df):
    is_complete = df['order_status']=='COMPLETE'
    is_closed = df['order_status']=='CLOSED'  
    result = df[is_complete | is_closed].groupby('date_id').order_id.nunique().to_frame()
    result.rename(columns={'order_id':'revenue_order_count'},inplace=True)
    return result

def get_canceled_order_count(df):
    is_canceled = df['order_status']=='CANCELED'  
    result = df[is_canceled].groupby('date_id').order_id.nunique().to_frame()
    result.rename(columns={'order_id':'canceled_order_count'},inplace=True)
    return result    

def get_outstanding_order_count(df):
    is_pending = df['order_status']=='PENDING'
    is_pending_payment = df['order_status']=='PENDING_PAYMENT'
    is_processing = df['order_status']=='PROCESSING'
    result = df[is_pending | is_pending_payment | is_processing].groupby('date_id').order_id.nunique().to_frame()
    result.rename(columns={'order_id':'outstanding_order_count'},inplace=True)
    return result

def get_revenue(df):
    is_complete = df['order_status']=='COMPLETE'
    is_closed = df['order_status']=='CLOSED'  
    result = df[is_complete | is_closed].groupby('date_id').agg({'order_item_subtotal':'sum'}).round(2)
    result.rename(columns={'order_item_subtotal':'revenue'},inplace=True)
    return result

def orders_daily(orders,orderItems):
    orders_join_orderItems = pd.merge(orders,orderItems,left_on=['order_id'],right_on=['order_item_order_id'],how='left')
    orders_join_orderItems.rename(columns={
        'order_date':'date_id',
        'order_item_product_id':'product_id'
    },inplace=True)
    revenue = get_revenue(orders_join_orderItems)
    total_order_count = get_total_order_count(orders_join_orderItems)
    revenue_order_count = get_revenue_order_count(orders_join_orderItems)
    canceled_order_count = get_canceled_order_count(orders_join_orderItems)
    outstanding_order_count = get_outstanding_order_count(orders_join_orderItems)
    dfs = [revenue,total_order_count,revenue_order_count,canceled_order_count,outstanding_order_count]
    result = reduce(lambda  left,right: pd.merge(left,right,on=['date_id'],
                                            how='outer'), dfs)
    result.reset_index(inplace=True)
    result['date_id'] = result['date_id'].apply(lambda x: x.strftime("%Y%m%d"))
    result['batch_date'] = pd.to_datetime('now').replace(microsecond=0)
    return result

def products_master(products,categories,departments):
    join1Df = pd.merge(products,categories,left_on='product_category_id',right_on='category_id')
    result = pd.merge(join1Df,departments,left_on='category_department_id',right_on='department_id')
    result.rename(columns={'product_category_id':'category_id'},inplace=True)
    result['batch_date'] = pd.to_datetime('now').replace(microsecond=0)
    return result

def customers_master(customers):
    result = customers
    result['batch_date'] = pd.to_datetime('now').replace(microsecond=0)
    return result


    