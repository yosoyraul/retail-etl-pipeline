import xmltodict
import pandas as pd

with open('query_parameters.xml') as fd:
    doc = xmltodict.parse(fd.read())

query_params = [
    (
        query['table_name'],
        query['source_columns']['column'],
        query['filter_col'],
        query['subquery']['subtable_name'],
        query['subquery']['sub_columns']['column'],
        query['subquery']['subfilter_col'],
        query['to_be_loaded']
    ) 
    for query in doc['queries']['query']
    ]
query_paramsDf = pd.DataFrame(query_params,columns=['tables','columns','filter_col','subtables','sub_columns','subfilter_col','to_be_loaded'])