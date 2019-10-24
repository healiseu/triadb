"""
TRIADB-TriaClick Demo with Northwind Traders DataModel/DataSet
Associative Entity Relationship Diagram

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MIS

mis = MIS(debug=1)

mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

eng = mis.restart(500)
print(mis)

eng.aserd._get_labels('label', 'edges')
eng.aserd._get_labels('key', 'edges')

eng.aserd._get_labels('label', 'nodes')
eng.aserd._get_labels('key', 'nodes')

eng.aserd.nodes.data()
eng.aserd.edges.data()

eng.aserd.draw()
eng.aserd.draw(nattribute='key', eattribute='key')

for head, tail, edge in eng.aserd.get_bfs_edges(39):
    print(f'From: {mis.set_dms(head[1]).alias} ---{eng.set_hacol(edge[1]).alias}---> '
          f'To: {mis.set_dms(tail[1]).alias}')

'''
From: Emp ---e_id---> To: Ord
From: Ord ---o_id---> To: Odet
From: Ord ---sh_id---> To: Ship
From: Ord ---c_id---> To: Cust
From: Odet ---p_id---> To: Pro
From: Pro ---s_id---> To: Sup
From: Pro ---cat_id---> To: Cat
'''
