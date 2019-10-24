"""
TRIADB-TriaClick Demo with Supplier-Part-Catalog (SPC) DataModel/DataSet
How to build new associations using Python namedtuple from `collections`

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MIS

mis = MIS(debug=2)

mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

# Set DataModel
eng = mis.restart(200, reset=True)
print(mis)
print(eng)

mis.get_entities()
mis.get_attributes()

# Set AssociativeSet
part_aset = mis.engine.get_aset(alias='PRT')

# Construct Associations from dictionaries
prt0 = {'key':(7,0), 'p_id':223, 'p_name':'Left Handed...', 'p_color':'Red', 'p_weight':15.5, 'p_unit':'lb'}
prt4 = {'key':(7,4), 'p_id':227, 'p_name':'I Brake for Crop...'}
prt6 = {'key':(7,6), 'p_id':229, 'p_name':'Anti-Gravity Turbine Generator', 'p_color':'Magenta'}

[part_aset._association(**prt) for prt in [prt0, prt4, prt6]]