"""
TRIADB Modules Testing:
    creating an instance of top level management information system object (MIS)
    get_entities(), get_attributes(), get() methods of triadb.subsystems.DataModelSystem
    switching and setting DataModelSystem objects

(C) October 2019 By Athanassios I. Hatzis
"""

from triadb import MIS
mis = MIS(debug=1)
mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)
mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

# Get an overview of the DataModelSystem
mis.get(what='models')

# Initialize MIS with a data model
datmodel = mis.set_dms(alias='SPC')
print(mis)
print(datmodel, '\n', mis.dms)
# General Overview
datmodel.get(what='overview')
# Data Model Overview
datmodel.get()

# Entities and Attributes
datmodel.get_entities()
datmodel.get_attributes()
datmodel.get_attributes(select='dim4, dim3, dim2, alias')
datmodel.get_attributes(select='dim4, dim3, dim2, alias, vtype, junction', index='dim4, dim3, dim2')
datmodel.get_attributes(csvlist='s_id, s_address, s_city, s_country, p_name, p_color')
datmodel.get_attributes(junction=True)
datmodel.get_attributes(out='objects')
datmodel.get_attributes(out='keys')
datmodel.get_attributes(out='dict')
datmodel.get_attributes(out='values', field='alias', key='nID')

# Difference between set_dms() and switch() methods
# Use switch() only when you change to a different DataModel, i.e. mis.dms object is updated
# Use set_dms() for objects of the same datamodel, i.e. mis.dms object remains the same
datmodel.switch(alias='Northwind').get_attributes()
print(datmodel)
# Set a junction attribute
attr = mis.set_dms(74)
print(attr)
# or
print(mis.set_dms(alias='o_id'))
# Get parents of the junction attribute
print(attr.parents)
assert(attr.node.junction == 1)
# Public Properties
print(f'Node:{attr.node}\nkey:{attr.key}\ntype:{attr.type}\nvtype:{attr.vtype}')

# Check to see that mis.dms has not changed
print(mis.dms)