import xmltodict
import pandas as pd

with open('query_parameters.xml') as fd:
    doc = xmltodict.parse(fd.read())

source_params = [
    (
        query['table_name'],
        query['source_columns']['column'],
        query['filter_col'],
        query['subquery']['subtable_name'],
        query['subquery']['sub_columns']['column'],
        query['subquery']['subfilter_col'],
        query['to_be_loaded']
    ) 
    for query in doc['queries']['source_query']
    ]
source_paramsDf = pd.DataFrame(source_params,columns=['tables','columns','filter_col','subtables','sub_columns','subfilter_col','to_be_loaded'])

target_params = [(query['table_name'],query['target_columns']['column']) for query in doc['queries']['target_query']]
target_paramsDf = pd.DataFrame(target_params,columns=['tables','columns'])
