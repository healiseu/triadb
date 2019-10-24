"""
TRIADB-TriaClick Demo with Supplier-Part-Catalog (SPC) DataModel/DataSet
HyperAtom Collection (HACOL) Operations

(C) October 2019 By Athanassios I. Hatzis
"""
from triadb import MIS

mis = MIS(debug=2)

mis.connect_to_datastore(dbms='clickhouse', host='localhost', port=9000,
                         user='demo', password='demo', database='TriaDB', trace=0)

mis.connect_to_metastore(dbms='mariadb', host='localhost', port=3306,
                         user='demo', password='demo', database='TRIADB', trace=0)

# Set DataModel
eng = mis.restart(200, reset=True)

mis.dms.get()
mis.dms.get_entities()
mis.dms.get_attributes()

# Make a selection (filtering)
mis.select('$v<20', alias='c_price')


# ------------------------------------------------------------------------
# Testing queries
# -------------------------------------------------------------------------
# Set HyperAtom Collection on an attribute of the data model, e.g. catalog price
eng.set_hacol(13)             # fetching catprice attribute with dim2
eng.set_hacol(alias='c_price')  # fetching catprice attribute with alias
eng.hacol.str

# Count operations on HACOL_SET
eng.hacol.cql.Count(coltype='set', filtered=True, total=False).Exe(columns='Set of Domain HyperAtoms (Filtered)').Res
eng.hacol.cql.Count(coltype='set', filtered=True, total=True).Exe(columns='Set of ALL HyperAtoms (Filtered)').Res
eng.hacol.cql.Count(coltype='set', filtered=False, total=False).Exe(columns='Set of Domain HyperAtoms').Res
eng.hacol.cql.Count(coltype='set', filtered=False, total=True).Exe(columns='Set of ALL HyperAtoms').Res

# Count operations on HACOL_BAG
eng.hacol.cql.Count(coltype='bag', total=False).Exe(columns='HyperLinks (HAtom-HBond)').Res
eng.hacol.cql.Count(coltype='bag', total=True).Exe(columns='Total Number of HyperLinks (HAtom-HBond)').Res


# Count operations on HACOL_VAL collection
eng.hacol.cql.Count().Exe(columns='Number of domain values').Res
eng.hacol.cql.Count().Where('$v').Between(10, 20).Exe('Catalog Price(Count)').Res
eng.hacol.cql.Count().Where('$v').Not().Between(10, 20).Exe('Catalog Price(Count)').Res
eng.hacol.cql.Count().Where('$v<16.5').Exe('Catalog Price(Count)').Res

# Aggregation operations on values of HACOL_VAL collection
eng.hacol.cql.Sum().Where('$v').Between(10, 20).Exe('Catalog Price(Sum)').Res
eng.hacol.cql.Average().Where('$v').Between(10, 20).Exe('Catalog Price(Avg)').Res


# MIS get_items() wrapper method for
# hacol.cql.Over('$2, $1, $v, $c, $s, $p').Order('$c DESC, $v DESC').Limit(limit).Exe(index='HA2, HA1').Res
eng.get_items(13, highlight=False)
eng.get_items(alias='c_price', highlight=False)

# Projection operations (Over)
eng.hacol.switch(13)  #catprice
eng.hacol.cql.Over().Order('$c DESC, $v DESC').Limit(3).Exe().Res
eng.hacol.cql.Over(unfiltered=True).Order('$c DESC, $v DESC').Limit(3).Exe().Res
eng.hacol.cql.Over('$fk, $v, $c, $hb, $hl').Exe().Res
eng.hacol.cql.Over().Where('$v').Between(10, 20).Exe().Res

eng.hacol.cql.Over('$k, $v, $c, $s, $p').Order('$v DESC').Exe().Res
eng.hacol.cql.Over('$k, $v, $c, $s, $p').Where('$v<20').Order('$v DESC').Exe().Res
eng.hacol.cql.Over('$k, $v, $c, $s, $p', excluded=True).Order('$v DESC').Exe().Res
eng.hacol.cql.Over('$k, $v, $c, $s, $p', excluded=False).Where('$v<20').Order('$v DESC').Exe().Res
eng.hacol.cql.Over('$k, $v, $c, $s, $p').Order('$c DESC, $v DESC').Exe().Res


eng.set_hacol(alias='c_quantity')
eng.hacol.cql.Over('$k, $v, $c, $hb, $hl').Where('$v>=200').Between(10, 20).Order('$c DESC, $v DESC').Exe().Res
eng.hacol.cql.Over().Order('$c DESC, $v DESC').Limit(3).Exe().Res
eng.hacol.cql.Over(unfiltered=True).Order('$c DESC, $v DESC').Limit(3).Exe().Res

# cql operations for junction attributes
eng.set_hacol(alias='p_id').cql.Count().Exe().Res
eng.set_hacol(alias='p_id').cql.Over('$v, $c').Order('$c DESC').Exe().Res

eng.hacol.switch(18)  #prtID
eng.hacol.cql.Over('$fk, $v, $c, $hb, $hl').Exe().Res
eng.hacol.cql.Over('$fk, $v, $c, $hb, $hl', excluded=False).Exe().Res
eng.hacol.cql.Over('$fk, $v, $c, $hb').Where('$v').In('226, 227, 230', csvtype='numeric').Exe().Res

# in the case Part Entity has not been filtered yet this should create HACOL in UNFILTERED state
eng.set_hacol(18, dms_entity=mis.set_dms(7))
eng.hacol.cql.Over('$fk, $v, $c, $hb, $hl').Exe().Res

eng.hacol.switch(17) #supID
eng.hacol.cql.Over('$fk, $v, $c, $hb, $hl').Exe().Res

eng.hacol.switch(4) # supcity
eng.hacol.cql.Over('$k, $v, $c, $hb').Where('$v').In('ILLINOIS, MADRID, NOTTINGHAM').Exe().Res
eng.hacol.cql.Count().Where('$v').In('ILLINOIS, MADRID, NOTTINGHAM').Exe('Supplier City(Count)').Res


# -----------------------------------------------
# Useful commands for debugging/testing purposes
# -----------------------------------------------
# Check an intermediate result by adding the Display() after any operator of the cql command
eng.hacol.cql.Count().Display()
# Repeat execution of query
eng.hacol.repeat_execution(exe=True)
# Display the last query
eng.hacol.last_query
# Get last query info
eng.hacol.last_query_info

# To check the final query before execution, set trace>=2 and Exe(execute=False)
eng.hacol.cql.Count().Exe(exe=False)
[print(q[0]) for q in eng.get_aset(12).cql.Filter().Exe(exe=False).Res[0]]