"""
TRIADB-TriaClick Demo with Northwind Traders DataModel/DataSet
Associative Entity Set Operations (Filtering)

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MIS
mis = MIS(debug=1)
mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

eng = mis.restart(500, 363, reset=True)
print(mis)

# Make multiple values selection for Customer Country
mis.select('Brazil, Mexico, Argentina', In=True, alias='c_country')