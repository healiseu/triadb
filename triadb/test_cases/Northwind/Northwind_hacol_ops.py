"""
TRIADB-TriaClick Demo with Northwind Traders DataModel/DataSet
HyperAtom Collection (HACOL) Operations

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MIS

mis = MIS(debug=1)

mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

# Set DataModel
eng = mis.restart(500, reset=True)

# Make multiple values selection for Customer Country
mis.select('Brazil, Mexico, Argentina', In=True, alias='c_country')

# ------------------------------------------------------------------------
# Testing queries
# -------------------------------------------------------------------------
# Set HyperAtom Collection on an attribute of the data model, e.g. order quantity
eng.set_hacol(alias='odet_quantity')
print(eng.hacol.str)
mis.get_items(alias='odet_quantity', highlight=False)
mis.get_items(alias='odet_quantity', order_by='$v DESC', excluded=False, highlight=False)

eng.set_hacol(alias='o_shipped')
eng.hacol.cql.Over('$k, $v, $c').Where('toYear($v)=1996').Order('$v DESC').Exe().Res
