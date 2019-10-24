"""
TRIADB Modules Testing:
    creating an instance of top level management information system object (MIS)
    testing restart(), get_parts(), get_columns(), get_table_engines(), get_last_query_info() methods
    these are methods that operate on ClickHouse merge-tree table engines

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MIS

# ===========================================================================
# Create a Management Information System (MIS) object and connect to DBMS
# ===========================================================================
mis = MIS(debug=1)

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)

# ===========================================================================
# Select DataModel, DataResource, Start MIS
# ===========================================================================
mis.get(what='datasets')
mis.get(what='models')
mis.restart(200, 242)
print(mis)

# Parts, Columns, Engines, Info
mis.get_parts(alias='hatom')
mis.get_parts(alias='hatomStates')
mis.get_parts(alias='hatomStates', hb2=12)
mis.get_parts(alias='hlink')
mis.get_parts(alias='Float32')
mis.get_parts(table='DAT_242_1')

mis.get_columns(alias='hatom')
mis.get_columns(alias='hlink')
mis.get_columns(alias='hlink', aggregate=True)
mis.get_columns(table='DAT_242_1')
mis.get_columns(table='DAT_242_1', aggregate=True)

mis.get_table_engines(table='%200%')
mis.get_table_engines(engine='ReplacingMergeTree')

mis.get_last_query_info()


# Get an overview of the DataModelSystem
mis.get(what='models')