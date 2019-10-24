"""
TRIADB Modules Testing:
    add() method of triadb.connectors.MetaManagementConnector (MMC)
    adding data sets

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import ETL, MetaManagementConnector


mmc = MetaManagementConnector(dbms='mariadb', host='localhost', port=3306,
                              user='demo', password='demo', database='TRIADB', debug=3,
                              rebuild=False, erase=True)

# =====================================================================
# Add MySQL databases
# =====================================================================
# Supplier-Part-Catalog MySQL database
mmc.add('dataset', cname='Supplier Part Catalogue in MySQL', alias='SPC_MySQL', ctype='MYSQL',
        db='SPC', descr='It is a toy database with three tables that form a many to many relationship ')

# Northwind Traders MySQL database
mmc.add('dataset', cname='Northwind Traders', alias='NORTHWIND', ctype='MYSQL',
        db='Northwind',
        descr='Northwind Traders Access database is a sample database that shipped with Microsoft Office')


# =====================================================================
# Add Flat File DataSets
# =====================================================================
# ETL.change_cwd('/dbstore/clickhouse/user_files/athan/FlatFiles')
ETL.change_cwd('/var/lib/clickhouse/user_files/demo/FlatFiles')

# BikeTrips CSV flat files
mmc.add('dataset', cname='Bike Trips Dataset', alias='BIKE', ctype='CSV',
        path=ETL.get_full_path('BikeTrips'), descr='about data set....')

# Supplier-Part-Catalog TSV flat files
mmc.add('dataset', cname='Supplier Part Catalogue TSV flat files', alias='SPC_TSV', ctype='TSV',
        path=ETL.get_full_path('SupplierPartCatalog'), descr='Supplier, Part, Catalog and denormalized data')

# Physicians TSV flat file
mmc.add('dataset', cname='Physicians Dataset', alias='PHYS', ctype='TSV',
        path=ETL.get_full_path('Physicians'), descr='info about the data set...')


# =====================================================================
# Add JSON Data Models
# =====================================================================

# This is a DataSet of JSON files, each file is a serialization of TRIADB data model
# ETL.change_cwd('/dbstore/clickhouse/user_files/athan')
ETL.change_cwd('/var/lib/clickhouse/user_files/demo')

mmc.add('dataset',  cname='JSON Data Models',  alias='JSON_DM', ctype='SDM', path=ETL.get_full_path('DataModels'),
        descr='Dictionary container for JSON file representation of TRIADB Data Models')
