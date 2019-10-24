"""
This file is part of TRIADB Self-Service Data Management and Analytics Framework
(C) 2015-2019 Athanassios I. Hatzis

TRIADB is free software: you can redistribute it and/or modify it under the terms of
the GNU Affero General Public License v.3.0 as published by the Free Software Foundation.

TRIADB is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with TRIADB.
If not, see <https://www.gnu.org/licenses/>.
"""
from orator import Schema

class SchemaEdge(object):
    """
        Each instance is a hyperlink that links:
        a tail node (Entity or Table) with
        a head node (Attribute or Column)

        Each hyperlink has two connectors (edges):
        An outgoing edge from the tail
        An incoming edge to the head

        Many Outgoing Edges that start From One HEdge (HE)
        Many Incoming Edges that end To One HNode (HN)

        The Edge hyperlink type represents a DIRECTED MANY TO MANY RELATIONSHIP
        with an OUTGOING BIDIRECTIONAL HYPERLINK that connects
        a tail node (from) with a head node (to) with bidirectional edges

        Important Notice:
        Do not confuse the DIRECTION OF RELATIONSHIP with the
        DIRECTION OF TRAVERSING THE BIDIRECTIONAL EDGES of the HYPERLINK

        Many-to-Many Relationship
        MANY side (tail node)      <------------------- ONE side (edge) ------------------>     MANY side (head node)

                                    (fromID - hyperlink object)
                                    An outgoing edge
         FROM Entity              ========================== Edge ==========================>   TO Attribute
         (HyperEdge)                                              (toID - hyperlink object)     (HyperNode)
                                                                  An incoming edge
    """
    def __init__(self, dbcon):
        self._db = dbcon
        self._schema = Schema(self._db)
        self._nodes_table = 'Nodes'
        self._edges_table = 'Edges'

    def build(self):
        with self._schema.create(self._edges_table) as table:
            # Primary key (not null) with auto increment
            table.increments('eID')

            # Use ManyToMany relationships between HyperNodes (HEAD nodes, toID) and HyperEdges (TAIL nodes, fromID)
            # HyperEdges and HyperNodes are object of model classes HEdge, HNode that are based on the Node class
            table.integer('toID').unsigned().nullable()
            table.integer('fromID').unsigned().nullable()

            # Add foreign key constraint
            table.foreign('fromID').references('nID').on(self._nodes_table).on_delete('cascade')

            # Add foreign key constraint
            table.foreign('toID').references('nID').on(self._nodes_table).on_delete('cascade')

    def erase(self):
        """
        Truncates all data in Edges table
        :return:
        """
        # You must disable the FK constraints first
        self._db.table(self._edges_table).truncate()


class SchemaNode(object):
    """
        Database dictionary resembles (TBox) a vocabulary of "terminological components", i.e. abstract terms
        Dictionary properties e.g. dimensions, names, counters, etc describe the concepts in dictionary
        These terms are Entity types, Attribute types, Data Resource types, Link(edge) types, etc....
        TBox is about types and relationships between types
        e.g. Entity-Attribute, Table-Column, JSON Object-Fields, etc....

        The Node class builds the schema of the `Nodes` table in MariaDB DBMS
        Each record in this table, is a node in the hypergraph of metadata
    """

    def __init__(self, dbcon):
        self._db = dbcon
        self._schema = Schema(self._db)
        self._nodes_table = 'Nodes'
        self._edges_table = 'Edges'

    def build(self):
        with self._schema.create(self._nodes_table) as table:
            # Primary key (not null) with auto increment
            table.increments('nID')

            # Use ManyToOne non-symmetric, self-reference relationship on objects of Node class
            # for the mapping of Columns/Fields onto Attributes
            # In that case we will create two models Field, Attribute and we define a relationship between them

            # ONE attribute is referenced by MANY fields
            # MANY different fields point to ONE attribute Object
            # from any FLD object set a reference that points to an attribute
            table.integer('aID').unsigned().nullable()  # ONE  side (non-symmetric)
            # Add foreign key constraint
            table.foreign('aID').references('nID').on(self._nodes_table).on_delete('cascade')

            # Use ManyToOne (Parent/Child) non-symmetric, self-reference relationship on objects of Node class
            # To link DMS to Data Models and Data Model to Entities
            # or DRS to Data Sets and Data Sets to Columns
            table.integer('pID').unsigned().nullable()  # ONE  side (non-symmetric)
            # Add foreign key constraint
            table.foreign('pID').references('nID').on(self._nodes_table).on_delete('cascade')

            # Add node dimensions
            table.integer('dim4').unsigned()
            table.integer('dim3').unsigned()
            table.integer('dim2').unsigned()
            # Add unique composite key constraint
            table.unique(['dim4', 'dim3', 'dim2'])

            # prevent duplication of aliases
            #   i) for two ENTities in the same model or
            #  ii) for two TBLs in the same data resource
            # Add unique composite key constraint
            table.unique(['dim4', 'dim3', 'alias'])

            # unique name identifier is composed from cname and alias
            # Add unique key constraint
            table.string('uname', 200).default('<NA>').unique()

            # canonical name (cname) is the name that is used in the data resource, e.g. field names in flat files
            table.string('cname', 100).default('<NA>')

            # alias is an alternate name that we assign to a Model, Entity, Attribute (see composite_key above)
            # see test_modules examples
            table.string('alias', 100).index()

            # node type in dictionary terms hypergraph:
            # it is used to specify what kind of object, i.e. term concept, we store in metadata dictionary
            #   DMS(Data Model System), DM(Data Model), ENT(Entity)
            #   DRS(Data Resources System), DS(Data Set), TBL (Table), SDM (serialization of data model),
            #   HLS(HyperLink System), HLT(HyperLinkType)...
            #   ATTR(Attribute), FLD(Field)
            node_types = ['<NA>', 'SYS', 'DMS', 'DM', 'ENT', 'ATTR', 'DRS', 'DS', 'TBL', 'SDM', 'FLD', 'HLS', 'HLT']
            table.enum('ntype', node_types).default('<NA>').index()

            # container type:
            # it is used to specify the type of a data resource container,
            # i.e. hierarchical file (JSON), flat file (CSV, TSV), database table (MYSQL),
            node_contypes = ['<NA>', 'HB', 'HA', 'SYS', 'HL', 'DM', 'DS', 'CSV', 'TSV', 'MYSQL', 'TBL', 'SDM', 'JSON']
            table.enum('ctype', node_contypes).default('<NA>').index()

            # counter for the number of
            # Data Models, Entities, Attributes,
            # Data Resources, Tables, Columns
            # HyperLink Types, HyperLinks
            table.integer('counter').unsigned().default(0)

            # Description of object
            table.text('descr').default('<NA>')

            # location of the data resources, data model on the disk, internet
            # this is the full path
            table.string('path', 100).default('<NA>')

            # MySQL database name
            table.string('db', 30).default('<NA>')

            # Table names for Clickhouse SET engines
            table.string('old_set', 30).default('<NA>')
            table.string('new_set', 30).default('<NA>')

            # ToDo: Labels on hyperlink types to describe direct and reverse direction (traversal path)
            # label_direct
            # label_reverse

            # For objects of ntype ATTR `junction` field indicates that
            # the attribute node (head) is linked to more than on entities (ENT - tail nodes)
            # we call this hypernode a junction node
            # In relational databases this is the common field where two tables are usually joined
            table.boolean('junction').default(False)

            # ToDo: implement another flag to indicate whether junction is open or closed
            # if bridge=True (junction open) then you can traverse to the next entity

            # ToDo: implement another flag to indicate whether junction plays the role of primary or foreign key

            # ATTR value type
            table.string('vtype', 50).default('<NA>')

    def erase(self):
        """
        Truncates all data in Nodes table
        :return:
        """
        # You must disable the FK constraints first
        self._db.table(self._nodes_table).truncate()
