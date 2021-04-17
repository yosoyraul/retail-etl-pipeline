

def load(query_builder,writer,cols,tbl,df):
    fields = ','.join(cols)
    lists = df.values.tolist()
    for row in lists:
        row[0] = int(row[0])
    tuples = [tuple(row) for row in lists ]
    query = query_builder.insert(fields,tbl)
    for row in tuples:
        print(row)

