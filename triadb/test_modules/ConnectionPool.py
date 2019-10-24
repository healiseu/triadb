"""
TRIADB Modules Testing:
    creating instance of triadb.clients.ConnectionPool
    testing connection to database management systems (ClickHouse, MariaDB)

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import ConnectionPool

# Test Connections
chcon = ConnectionPool(dbms='clickhouse', host='localhost', port=9000,
                       user='demo', password='demo', database='TriaDB', trace=4)
chsql = chcon.sql
chcmd = chcon.cmd

mycon = ConnectionPool(dbms='mariadb', host='localhost', port=3306,
                       user='demo', password='demo', database='TRIADB', trace=3)
mysql = mycon.sql

# Test ClickHouse Client
print(chsql('SHOW DATABASES'))

# Test MariaDB Client
print(list(mysql('SHOW TABLES')))

