import os

def load_insert(query_builder,writer,cols,tbl,df):
    fields = ','.join(cols)
    vals = ",".join(["%s" for i in range(len(cols))])
    lists = df.values.tolist()
    for row in lists:
        row[0] = int(row[0])
    tuples = [tuple(row) for row in lists ]
    query = query_builder.insert(vals,fields,tbl)
    writer.query_executemany(query,tuples)

def load_bulk(writer,cols,table,df):
    tmp_df = './tmp_dataframe.csv'
    df.to_csv(tmp_df,index=False,header=False,sep='\t')
    with open(tmp_df,'r') as f:
        writer.copy_from_file(f,cols,table)
    os.remove(tmp_df)


