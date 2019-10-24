"""
TRIADB-TriaClick Demo with Supplier-Part-Catalog (SPC) DataModel/DataSet
Mapping and Loading SPC Data Resources on SPC data model

(C) October 2019 By Athanassios I. Hatzis

Data model here is a hypergraph of 3 entities with 15 attributes in total
Data resources are 3 MYSQL tables with 17 fields in total

NOTICE1: In the following mapping procedure,
all the fields from the three sets collectively are mapped onto attributes

NOTICE2: If you set rebuild=True flag it will rebuild ClickHouse TRIADB database
"""
from triadb import MIS

# ===========================================================================
# Create a Management Information System (MIS) object and connect to DBMS
# ===========================================================================
mis = MIS(debug=1, rebuild=True, what='data')

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)

# ===========================================================================
# Select DataModel, DataResource, Start MIS
# ===========================================================================
mis.get(what='datasets')
mis.get(what='models')
mis.restart(500, 242)
print(mis)

# ===========================================================================
# Mapping Fields onto Attributes
# ===========================================================================
# Match field keys of the data model with attribute keys of the data resource
mis.match_fields_with_attributes()

mis.reset_mapping()

# Match field keys with attribute keys, this is the mapping that is passed to mis()
# ToDO 1. graphical user interface for this procedure
# ToDO 2. select which fields (columns) of the data resource we will map onto the model

# [(num+183, 0) for num in range(17)]  # generate a list of pairs and copy it from the console
matching_pairs = [
 (183, 177),
 (184, 178),
 (185, 173),
 (186, 174),
 (187, 175),
 (188, 176),
 (189, 178),
 (190, 168),
 (191, 169),
 (192, 170),
 (193, 171),
 (194, 177),
 (195, 162),
 (196, 163),
 (197, 165),
 (198, 164),
 (199, 166)]

# Check the matched pairs before adding the mapping to the data model
mis.compare_fields_with_attributes(matching_pairs)

# Add Mapping for the specific data resource
mis.add_mapping()

# Verify Mapping
mis.drs.get_fields()
mis.dms.get_attributes()


# ===========================================================================
# Import/Load
# ===========================================================================

# Importing Data from MYSQL tables to ClickHouse table engines
mis.import_data()

# Load ClickHouse HyperGraph Engines
mis.load_data()

# Restart engine to display the dictionary of ASETs and verify that ASETs have been created
mis.restart(200, 242, reset=True)

mis.get_asets()