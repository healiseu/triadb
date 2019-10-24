"""
TRIADB Modules Testing:
    creating instance of triadb.connectors.MetaManagementConnector (MMC) with rebuild and erase flags

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MetaManagementConnector

mmc = MetaManagementConnector(dbms='mariadb', host='localhost', port=3306,
                              user='demo', password='demo', database='TRIADB', debug=1,
                              rebuild=True, erase=False)