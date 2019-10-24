"""
TRIADB Modules Testing:
    creating an instance of top level management information system object (MIS)
    add_datamodel(), add_entity(), add_attribute() methods of triadb.subsystems.DataModelSystem
    adding data model components (Models, Entities, Attributes) with commands

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MIS

mis = MIS(debug=3, erase=False)

mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=3)

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=3)
print(mis)

# Create a new instance of Data Model
SPC = mis.set_dms()

SPC.add_datamodel(cname='Supplier Part Catalog', alias='SPC',
                  descr='Model with three entities that represent relations (tables) in a relational database')

# Add ENTities (Tail Nodes)
SPC.add_entity(cname='Supplier', alias='SUP', descr='The Supplier Entity of the data model')
SPC.add_entity(cname='Part',     alias='PRT', descr='The Part Entity of the data model')
SPC.add_entity(cname='Catalog',  alias='CAT', descr='The Catalog Entity of the data model')

# Add ATTRibutes (Head Nodes)
SPC.add_attribute('SUP', cname='sname',    alias='s_name',    vtype='String', descr='supplier name')
SPC.add_attribute('SUP', cname='saddress', alias='s_address', vtype='String', descr='supplier address')
SPC.add_attribute('SUP', cname='scity',    alias='s_city',    vtype='String', descr='supplier city')
SPC.add_attribute('SUP', cname='scountry', alias='s_country', vtype='String', descr='supplier country')
SPC.add_attribute('SUP', cname='sstatus',  alias='s_status',  vtype='UInt8',  descr='supplier status')

SPC.add_attribute('PRT', cname='pname',    alias='p_name',      vtype='String',  descr='part name')
SPC.add_attribute('PRT', cname='pcolor',   alias='p_color',     vtype='String',  descr='part color')
SPC.add_attribute('PRT', cname='pweight',  alias='p_weight',    vtype='Float32', descr='part weight')
SPC.add_attribute('PRT', cname='punit',    alias='p_unit',      vtype='String',  descr='part unit')

SPC.add_attribute('CAT', cname='cprice',    alias='c_price',    vtype='Float32', descr='catalog price')
SPC.add_attribute('CAT', cname='cquantiy',  alias='c_quantity', vtype='UInt16',  descr='catalog quantity')
SPC.add_attribute('CAT', cname='cdate',     alias='c_date',     vtype='Date',    descr='catalog date')
SPC.add_attribute('CAT', cname='ccheck',    alias='c_check',    vtype='UInt8',   descr='catalog check')

# Add junction Nodes
# Junction nodes are ATTRibutes that are linked to more than one ENTities
SPC.add_attribute(['SUP', 'CAT'], cname='sid',  alias='s_ID',  vtype='UInt16', descr='supplier id')
SPC.add_attribute(['PRT', 'CAT'], cname='pid',  alias='p_ID',  vtype='UInt16', descr='part id')