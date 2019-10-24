"""
TRIADB-TriaClick Demo with Supplier-Part-Catalog (SPC) DataModel/DataSet
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
eng = mis.restart(200, reset=True)
mis.dms.get_entities()
mis.dms.get_attributes()

# ------------------------------------------------------------------------
# Counting
# -------------------------------------------------------------------------
# Set AssociativeSet on Catalog Entity
aset = eng.get_aset(12)
aset.count(estimate=False)
aset.count(coltype='set')
aset.count(coltype='set', projection='c_price, c_quantity, c_date, c_check')

aset.count(coltype='bag', order='key')
aset.count(coltype='bag', missing=True, order='key')

aset.count(coltype='bag', projection='c_price, c_quantity, c_date, c_check')
aset.count(coltype='bag', projection='c_price, c_quantity, c_date, c_check', order='key')
aset.count(coltype='bag', projection='c_price, c_quantity, c_date, c_check', missing=True, order='key')

# Set AssociativeSet on Part Entity
aset = eng.get_aset(7)
aset.count()
aset.count(coltype='set')
aset.count(coltype='bag')
aset.count(coltype='bag', missing=True)

# Set AssociativeSet on Supplier Entity
aset = eng.get_aset(1)
aset.count(coltype='set')
aset.count(coltype='bag')
aset.count(coltype='bag', missing=True)
