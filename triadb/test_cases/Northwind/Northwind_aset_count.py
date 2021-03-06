"""
TRIADB-TriaClick Demo with Northwind Traders DataModel/DataSet
Associative Entity Set Count Operations

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MIS

mis = MIS(debug=1)

mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

# Set DataModel
eng = mis.restart(500, reset=False)

# ------------------------------------------------------------------------
# Counting
# -------------------------------------------------------------------------
# Set AssociativeSet on Catalog Entity
aset = eng.get_aset(alias='Ord')
ord_projection = 'o_id, e_id, c_id, sh_id, o_date, o_required, o_shipped, o_region, o_postal'
aset.count(estimate=False)
aset.count(coltype='set')
aset.count(coltype='set', projection=ord_projection)

aset.count(coltype='bag', order='key')
aset.count(coltype='bag', missing=True, order='key')

aset.count(coltype='bag', projection=ord_projection)
aset.count(coltype='bag', projection=ord_projection)
aset.count(coltype='bag', projection=ord_projection, missing=True, order='key')
