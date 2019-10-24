"""
TRIADB Modules Testing:
    add() method of triadb.connectors.MetaManagementConnector (MMC)
    adding data model components (Models, Entities, Attributes) with commands

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MetaManagementConnector

mmc = MetaManagementConnector(dbms='mariadb', host='localhost', port=3306,
                              user='demo', password='demo', database='TRIADB', debug=1,
                              rebuild=False, erase=True)

# =====================================================================
# 1st Data Model : Supplier-Part-Catalog
# =====================================================================
# Add a Data Model
spc = mmc.add('datamodel', cname='Supplier Part Catalog', alias='SPC',
              descr='Model with three entities that represent relations (tables) in a relational database')

# Add ENTities (Tail Nodes)
sup = mmc.add('entity', parent=spc, cname='Supplier', alias='SUP', descr='This is the supplier')
prt = mmc.add('entity', parent=spc, cname='Part',     alias='PRT', descr='The Part Entity of the data model')
cat = mmc.add('entity', parent=spc, cname='Catalog',  alias='CAT', descr='The Catalog Entity of the data model')


# Add ATTRibutes (Head Nodes)
mmc.add('attribute', parent=spc, tail=sup,
        cname='sname',    alias='s_name',    vtype='String', descr='supplier name')
mmc.add('attribute', parent=spc, tail=sup,
        cname='saddress', alias='s_address', vtype='String', descr='supplier address')
mmc.add('attribute', parent=spc, tail=sup,
        cname='scity',    alias='s_city',    vtype='String', descr='supplier city')
mmc.add('attribute', parent=spc, tail=sup,
        cname='scountry', alias='s_country', vtype='String', descr='supplier country')
mmc.add('attribute', parent=spc, tail=sup,
        cname='sstatus',  alias='s_status',  vtype='UInt8',  descr='supplier status')

mmc.add('attribute', parent=spc, tail=prt,
        cname='pname',    alias='p_name',      vtype='String',  descr='part name')
mmc.add('attribute', parent=spc, tail=prt,
        cname='pcolor',   alias='p_color',     vtype='String',  descr='part color')
mmc.add('attribute', parent=spc, tail=prt,
        cname='pweight',  alias='p_weight',    vtype='Float32', descr='part weight')
mmc.add('attribute', parent=spc, tail=prt,
        cname='punit',    alias='p_unit',      vtype='String',  descr='part unit')

mmc.add('attribute', parent=spc, tail=cat,
        cname='cprice',    alias='c_price',    vtype='Float32', descr='catalog price')
mmc.add('attribute', parent=spc, tail=cat,
        cname='cquantiy',  alias='c_quantity', vtype='UInt16',  descr='catalog quantity')
mmc.add('attribute', parent=spc, tail=cat,
        cname='cdate',     alias='c_date',     vtype='Date',    descr='catalog date')
mmc.add('attribute', parent=spc, tail=cat,
        cname='ccheck',    alias='c_check',    vtype='UInt8',   descr='catalog check')


# Add junction Nodes
# Junction nodes are ATTRibutes that are linked to two ENTities
mmc.add('attribute', parent=spc, tail=[sup, cat],
                     cname='sid',  alias='s_ID',  vtype='UInt16', descr='supplier id', junction=True)
mmc.add('attribute', parent=spc, tail=[sup, prt],
                     cname='pid',  alias='p_ID',  vtype='UInt16', descr='part id', junction=True)


# =====================================================================
# 2nd Data Model : AIRPORTS
# =====================================================================
#
# Add a Data Model
AIR = mmc.add('datamodel', cname='AIRPORTS', alias='AIR')

# Add ENTity (Tail Node)
AIRPORT = mmc.add('entity', parent=AIR, cname='Airport', alias='Airprt')

# Add ATTRibutes (Head Nodes)
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='id',        alias='air_id',    vtype='UInt16')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='ident',     alias='air_ident', vtype='UInt16')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='type',      alias='air_type',  vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='name',      alias='air_name',  vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='latitude',  alias='air_lat',   vtype='Float32')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='longidute', alias='air_long',  vtype='Float32')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='elevation', alias='air_elev',  vtype='UInt16')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='continent', alias='air_cont',  vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='country',   alias='air_cntr',  vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='region',    alias='air_region',     vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='municipality', alias='air_municip', vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='service',      alias='air_serv',  vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='gps_code',     alias='air_gps',   vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='iata_code',    alias='air_iata',  vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='local_code',   alias='air_local', vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='home_link',    alias='air_home',  vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='wikipedia_link', alias='air_wikip',  vtype='String')
mmc.add('attribute', parent=AIR, tail=AIRPORT, cname='keywords',       alias='air_kwords', vtype='String')

# =====================================================================
# 3rd Data Model : MOVIES METADATA
# =====================================================================
#
# Add a Data Model
MOVMETA_DM = mmc.add('datamodel', cname='MOVIES METADATA MODEL', alias='MOVMETA_DM')

# Add ENTity (Tail Node)
MOVMETA = mmc.add('entity', parent=MOVMETA_DM, cname='MOVIE METADATA', alias='MOVMETA')

# Add ATTRibutes (Head Nodes)
mmc.add('attribute', parent=MOVMETA_DM, tail=MOVMETA, cname='movie_title', alias='mov_title', vtype='String')
mmc.add('attribute', parent=MOVMETA_DM, tail=MOVMETA, cname='director_name', alias='mov_dir', vtype='String')
mmc.add('attribute', parent=MOVMETA_DM, tail=MOVMETA, cname='color', alias='mov_col', vtype='String')
mmc.add('attribute', parent=MOVMETA_DM, tail=MOVMETA, cname='duration', alias='mov_dur', vtype='UInt8')
mmc.add('attribute', parent=MOVMETA_DM, tail=MOVMETA, cname='actor_1_name ', alias='mov_actor', vtype='String')
mmc.add('attribute', parent=MOVMETA_DM, tail=MOVMETA, cname='language', alias='mov_lang', vtype='String')
mmc.add('attribute', parent=MOVMETA_DM, tail=MOVMETA, cname='country', alias='mov_cntr', vtype='String')
mmc.add('attribute', parent=MOVMETA_DM, tail=MOVMETA, cname='title_year', alias='mov_year', vtype='UInt8')