"""
TRIADB Modules Testing:
    get() method of triadb.connectors.MetaManagementConnector (MMC)
    orm Queries using Builder, QueryBuilder, Collection....

(C) October 2019 By Athanassios I. Hatzis
"""

from triadb import MetaManagementConnector, Node, DataModel, Entity, Attribute

mmc = MetaManagementConnector(dbms='mariadb', host='localhost', port=3306,
                              user='demo', password='demo', database='TRIADB', debug=4,
                              rebuild=False, erase=False)

# ..............................................................................................
# A) Return a single object as an instance of an ORM Model
# ..............................................................................................
# Get Node object
mmc.get(what='node', key=(2, 100, 1))
# Get System object
mmc.get(what='system', alias='DMS')

# Get DataModel object
mmc.get(what='model', alias='SPC')
mmc.get(200, what='model')
# Get Entity object
mmc.get(200, 1,             what='entity')
mmc.get(200, alias='SUP',   what='entity')
# Get Attribute object
mmc.get(200, 10,                what='attribute')
mmc.get(200, alias='p_color',   what='attribute')

# Get DataSet object
mmc.get(what='dataset', alias='SPC_MySQL')
mmc.get(121, what='dataset')
# Get Table object
mmc.get(121, 1, what='table')
# Get SDM object
mmc.get(121, 1, what='sdm')
# Get Field object
mmc.get(242, 4, what='field')

# ..............................................................................................
# B) Return Builder, QueryBuilder, Collection
# ..............................................................................................
# General Overview, Systems (DMS, DRS) Overview
mmc.get(what='overview', index='dim4, dim3, dim2')
mmc.get(what='dms', index='dim4, dim3, dim2')
mmc.get(what='drs', index='dim4, dim3, dim2')

# Systems
mmc.get(what='systems', out='objects')
mmc.get(what='systems', out='keys')
mmc.get(what='systems', out='values', field='cname')
mmc.get(what='systems', out='values', field='cname', key='nID')
mmc.get(what='systems', out='dict')
mmc.get(what='systems')
mmc.get(what='systems', select='nID, pID, dim4, dim3, dim2, uname, descr', index='nID')

# Models
mmc.get(what='models', out='objects')
mmc.get(what='models')

# All metadata from a specific Model
mmc.get(100, what='all')

# Entities of a Model
mmc.get(alias='SPC', what='entities')
mmc.get(100,         what='entities')

# Attributes of a Model
mmc.get(alias='SPC', what='attributes',
        select='dim4, dim3, dim2, cname, alias, ntype, ctype, vtype, counter', index='dim4, dim3, dim2')
mmc.get(100,         what='attributes',
        select='dim4, dim3, dim2, cname, alias, ntype, ctype, vtype, counter', index='dim4, dim3, dim2')
mmc.get(100, what='attributes', junction=True, out='objects')
mmc.get(200, what='attributes', csvlist='s_country, s_city, c_price, c_date')


# Entities of an Attribute
mmc.get(100, 16, what='entities')
mmc.get(100, 17, what='entities')

# Attributes of an Entity
mmc.get(100, 3, what='attributes')

# Datasets
mmc.get(what='datasets', out='objects')
mmc.get(what='datasets', select='dim3, cname, alias, ntype, ctype, counter, path, db', index='dim3')

# All metadata from a specific DataSet
mmc.get(121, what='all')

# SDMs of a DataSet
mmc.get(121, what='sdms', select='dim3, dim2, cname, ntype, ctype, counter, path, db', index='dim3, dim2')
mmc.get(121, what='sdms', out='objects')

# Tables of a DataSet
mmc.get(242, what='tables', select='dim3, dim2, cname, ntype, ctype, counter, path, db', index='dim3, dim2')
mmc.get(242, what='tables', out='objects')

# Fields of a DataSet
mmc.get(242, what='fields', select = 'nID, pID, aID, dim4, dim3, dim2, cname, ntype, ctype')
mmc.get(242, what='fields', out='objects')

# Fields of a Table
mmc.get(242, 2, what='fields', out='dataframe', select = 'nID, pID, aID, dim4, dim3, dim2, cname, ntype, ctype')




# =========================================================================================================
# Verification and Testing TRIADB Object Oriented Approach
# =========================================================================================================
# System objects
print(mmc.dms)
print(mmc.drs)
print(mmc.dls)


# -----------------------------------------------------------------------------------------------------------
# orm Queries using Builder, QueryBuilder, Collection....
# -----------------------------------------------------------------------------------------------------------
Node.all().all()
Node.find(2)
Node.get_node(key=(2, 100, 0))

Node.get_systems()                              # orm.Builder
Node.get_systems().get().all()                  # orm.Builder -> orm.Collection -> List of objects
Node.get_systems().get_query().get().all()      # orm.Builder -> orm.QueryBuilder -> orm.Collection -> Dictionaries

DataModel.get_data_model('SPC')
DataModel.get_data_model('SPC').entities.all()
DataModel.get_data_model('SPC').entities().where_alias('SUP').first()

DataModel.get_data_model('SPC').entities().get().all()
DataModel.get_data_model('SPC').attributes().get().all()
print(DataModel.get_data_model('SPC').sys)
Entity.get_node(key=(2, 100, 3)).attributes().get().all()
Attribute.get_node(key=(2, 100, 17)).entities().get().all()
