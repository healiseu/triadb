"""
TRIADB Modules Testing:
    creating an instance of top level management information system object (MIS)
    testing `rebuild` and `erase` flags for rebuilding or erasing TriaDB (MariaDB database)

    This is the first script you run after installing `triadb` package
    to populate TriaDB meta-data management system with DataModels and DataSets
    (See README.rst)

(C) October 2019 By Athanassios I. Hatzis
"""

from triadb import ETL, MIS

mis = MIS(debug=2, rebuild=False, erase=False)

mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

# NEW DataResourceSystem, DataModelSystem
mis.set_drs()
mis.set_dms()
print(mis)

# ************************************************************************************************************
# Add Data Models
# ************************************************************************************************************

ETL.change_cwd('/var/lib/clickhouse/user_files/demo')

# This is a DataSet of JSON files, each file is a serialization of TRIADB data model
mis.drs.add_dataset(cname='JSON Data Models',  alias='JSON_DM', ctype='SDM', path=ETL.get_full_path('DataModels'),
                    descr='Dictionary container for JSON file representation of TRIADB Data Models')

# Add Data Models from a JSON DataSet
mis.dms.add_datamodels_from_json()

# ************************************************************************************************************
# Add Data Resources from MySQL database tables and flat files
# ************************************************************************************************************

# =====================================================================
# Add MySQL databases
# =====================================================================

# Supplier-Part-Catalog MySQL database

mis.drs.add_dataset(cname='Supplier Part Catalogue in MySQL', alias='SPC_MySQL', ctype='MYSQL', db='SPC',
                    descr='It is a toy database with three tables that form a many to many relationship ')

# Northwind Traders MySQL database
mis.drs.add_dataset(cname='Northwind Traders', alias='NORTHWIND', ctype='MYSQL', db='Northwind',
                    descr='Northwind Traders Access database is a sample database that shipped with Microsoft Office')


# =====================================================================
# Add Flat File DataSets
# =====================================================================
ETL.change_cwd('/var/lib/clickhouse/user_files/demo/FlatFiles')

# Supplier-Part-Catalog TSV flat files
mis.drs.add_dataset(cname='Supplier Part Catalogue TSV flat files', alias='SPC_TSV', ctype='TSV',
                    path=ETL.get_full_path('SupplierPartCatalog'),
                    descr='Supplier, Part, Catalog and denormalized data')

# ========================================================================
# Sanity test
# ========================================================================
print(mis.get(what='overview'))
