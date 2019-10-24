"""
TRIADB Modules Testing:
    creating an instance of top level management information system object (MIS)
    get_tables(), get_fields(), get() methods of triadb.subsystems.DataModelSystem
    switching and setting DataResourceSystem objects

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MIS
mis = MIS(debug=1)
mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)
mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

# Get an overview of the DataResourcesSystem
mis.get(what='datasets')

# Initialize MIS with a data resource
datres = mis.set_drs(242, 0)
print(mis)
print(datres, '\n', mis.drs)
# General Overview
datres.get(what='overview')
# Data Resource Overview
datres.get()

# Tables and Fields
datres.get_tables()
datres.get_tables(out='objects')
datres.get_tables(out='keys')
datres.get_tables(out='dict')
datres.get_tables(select='dim4, dim3, dim2, cname, alias, ntype, ctype')

datres.get_fields(extras='table')
mis.set_drs(242, 1).get_fields()
mis.set_drs(242, 1).get_fields(out='values', field='cname', key='nID')
print(mis.drs)

# Difference between set_drs() and switch() methods
# Use switch() only when you change to a different dataset, i.e. mis.drs object is updated
# Use set_drs() for objects of the same dataset, i.e. mis.drs object remains the same
datres.switch(363, 0)
datres.get_tables()
supplier = mis.set_drs(363, 8)
supplier.get_fields(select='dim3, dim2, cname', extras='table', index='dim3, dim2')
supplier.get_fields(out='values', field='cname', key='nID')
mis.set_drs(363, 73).node.to_dict()

# Check to see that mis.drs has not changed
print(mis.drs)

# Public properties
supplier.check_mapping()
print(f'key:{supplier.key}\nctype:{supplier.ctype}\nimported:{supplier.imported}\nloaded:{supplier.loaded}')

# Fetch rows of the TSV/CSV data resource with Pandas
mis.drs.switch(484, 0)
mis.set_drs(484, 1).get_rows_with_pandas()

