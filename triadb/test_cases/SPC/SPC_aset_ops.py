"""
TRIADB-TriaClick Demo with Supplier-Part-Catalog (SPC) DataModel/DataSet
Associative Entity Set Operations (Selections, Filtering, Counting, Restarting...)

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MIS
mis = MIS(debug=3)

mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

# ===========================================================================
# Select DataModel, DataResource, Start MIS
# ===========================================================================
eng = mis.restart(500, reset=True)
print(mis)
print(eng)

mis.get_attributes(select='dim2, cname, alias')
mis.get_entities(select='dim2, cname, alias')
mis.get_asets()

# Selections Set
selections = [
    eng.set_hacol(alias='p_color').cql.Select().Where("$v='Red'"),
    eng.set_hacol(alias='c_price').cql.Select().Where('$v<20'),
    eng.set_hacol(alias='c_quantity').cql.Select().Where('$v=200'),
    eng.set_hacol(alias='c_check').cql.Select().Where('$v=0')
]
# Progressive (Single) filtering mode, filter ASETs each time with one selection
#
# Single Mode Filtering with one selection only
eng.filter_selections(selections[0])

# Single Mode Filtering with a set of selections
eng.filter_selections(selections[1:])

# Single Mode Filtering with one selection only
eng.filter_selections(selections[3])

# Reset Filtering
eng.restart()

# Multiple Mode Filtering, filter ASETs with a filter based on multiple selection criteria
eng.filter_selections(selections[:3], mode='multiple')
# Notice the WARNING
# *** WARNING *** Selection - CQL(200, 9)[prtcol].Where($v='Red') is skipped

# Continue in single filtering mode with those selections left
eng.filter_selections([selections[1], selections[3]])

# ------------------------------------------------
# Verification
# ------------------------------------------------
mis.get_asets()
# Check selections
mis.get_selections()
# Check count
mis.count_items()

# Check values of a specific Entity
mis.dms.switch(500, 12)
for alias in [attr.alias for attr in mis.dms.get_attributes(out='objects')]:
    print(mis.get_items(alias=alias, highlight=False, excluded=False), '\n')

# Get back associations represented as tuples
mis.get_tuples(13, 14, 15, 16, 17, 18, aset_dim2=12, hb2=True, hb1=True)