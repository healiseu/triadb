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

# ========================================
# Package-Module Dependencies
# ========================================

import pandas as pd
from orator import Model
from orator.exceptions.orm import ModelNotFound
from .meta_schema import SchemaNode, SchemaEdge
from .meta_models import *
from .clients import ConnectionPool
from .utils import ETL
from .exceptions import (InvalidAddOperation, InvalidGetOperation)

format_types = ['pony', 'sql']

out_types = ['objects', 'keys', 'values', 'dict', 'dataframe',
             'builder', 'query_builder', 'collection']

add_types = ['root', 'system',
             'datamodel', 'entity', 'attribute',
             'dataset', 'table', 'field']

get_types = ['node', 'overview', 'dms', 'drs', 'all',
             'system', 'systems',
             'model', 'models', 'entity', 'entities', 'attribute', 'attributes',
             'dataset', 'datasets', 'table', 'tables', 'sdm', 'sdms', 'field', 'fields']

# ***********************************************************************************************
# ******************************** Classes Implementation ***************************************
# ***********************************************************************************************


# ===========================================================================================
# Data Management Connector (DMC)
# -------------------------------------------------------------------------------------------
class DataManagementConnector(ConnectionPool):
    def __init__(self, rebuild=False, what='data', debug=0, **connect_params):
        super().__init__(**connect_params)
        self._dbg = debug
        self._datadb = self.database

        if rebuild and what=='data':
            self._rebuild()

    def __repr__(self):
        return f'DataDB:{self._client}(host = {self._host}, port = {self._port}, database = {self.database})'


    def _display_warning(self):
        print('\n\n************************************************************************************')
        print(f'   *** WARNING: ALL YOUR DATA IN ClickHouse-{self._datadb} DATABASE WILL BE LOST ***')
        print(f'***********************************************************************************')
        while True:
            query = input('Do you want to proceed ?')
            key_pressed = query[0].lower()
            if query == '' or key_pressed not in ['y', 'n']:
                print('Please answer with (y)es or (n)o !')
            else:
                break

        return key_pressed

    def _rebuild(self):
        key_pressed = self._display_warning()

        if key_pressed == 'y':
            print('\n\nPlease wait, recreating TriaDB database...')
            # Drop Database
            self.sql('DROP DATABASE TriaDB')
            self.sql('CREATE DATABASE TriaDB')
            print('\n Done.')
            return True
        elif key_pressed == 'n':
            print('\nOperation aborted.')
            return False

    @property
    def datadb(self):
        return self._datadb

    def get_last_query_info(self):
        print(f'{self.last_query}\n--------------------------------------------------------')
        lqs = self.qstats
        print(f'QueryID:{lqs[0]}\nElapsed: {round(lqs[2], 3)} sec',
              f'{lqs[1]} rows in set.\nProcessed: {lqs[3]} rows, {round(lqs[4]/1048576, 3)} MB')

    def get_parts(self, table, active=None, hb2='%', exe=True):
        return self.cmd('parts', db=self._datadb, table=table, hb2=hb2, active=active, execute=exe)

    def get_columns(self, table, aggregate=False, exe=True):
        return self.cmd('columns', db=self._datadb, table=table, aggr=aggregate, execute=exe)

    def get_tables(self, engine=None, table=None, exe=True):
        return self.cmd('tables', db=self._datadb, engine=engine, table=table, execute=exe)

    def optimize_parts(self, table, exe=True):
        return self.cmd('optimize', table=table, execute=exe)


# ===========================================================================================
# Meta Management Connector (MMC)
# -------------------------------------------------------------------------------------------
class MetaManagementConnector(ConnectionPool):
    """
    MetaManagementConnector creates a connection to MariaDB SQL server
    This is a composite pattern class based on ConnectionPool and model classes of Orator

    In particular the class MetaManagementConnector implements get and add operations of TRIADB
    on a hypergraph that represents meta-data objects such as entities, attributes, data resources, etc.
    and how these are connected.
    """

    def __init__(self, erase=False, rebuild=False, what='meta', debug=0, **connect_params):
        """
        :param erase: set the flag to erase all data (truncate table) from the metadata database
                     (faster than rebuilding the schema)
        :param rebuild: set the flag only the first time that you need to create the metadata database schema
        :param debug: flag to display debugging messages during execution
        :param what: Default `meta`, rebuild or erase MariaDB metadata database
                     `data`, rebuild or erase ClickHouse TriaDB database
                     `all`, rebuild or erase both ClickHouse and MariaDB databases
        :param connect_params: see ConnectionPool
        """

        super().__init__(**connect_params)
        self._dbg = debug
        self._metaclient = self.api    # This is the Orator API
        self._metadb = self.database   # Name of MariaDB database that we store metadata

        # These instance variables are used to rebuild schema and/or erase tables
        self._nodes = SchemaNode(self._metaclient)  # MariaDB database table Nodes
        self._edges = SchemaEdge(self._metaclient)  # MariaDB database table Edges
        self._setup()

        if self._dbg > 5:
            self._metaclient.connection().enable_query_log()

        done = False  # Flag to check whether the system was rebuilt or erased ?
        if rebuild and what=='meta':
            # That runs only the first time to rebuild the schema of MariaDB database that stores metadata
            done = self._rebuild()
        if erase and what=='meta':
            # This will delete all data from tables, it will not rebuild the schema, it's faster
            done = self._erase()

        if done:
            self._initialize()

        self._dms = System.get_system('DMS')
        self._drs = System.get_system('DRS')
        self._dls = System.get_system('HLS')

        if self._dbg > 3:
            self._binding_classes()

    def __repr__(self):
        return f'MetaDB:{self._client}(host = {self._host}, port = {self._port}, database = {self.database})'

    @property
    def dms4(self):
        return self._dms.dim4

    @property
    def drs4(self):
        return self._drs.dim4

    @property
    def dms(self):
        return self._dms

    @property
    def drs(self):
        return self._drs

    @property
    def dls(self):
        return self._dls

    @property
    def metadb(self):
        return self._metadb

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def user(self):
        return self._user

    @property
    def pwd(self):
        return self._password

    def _setup(self):
        # Set the connection for Orator Models
        Model.set_connection_resolver(self._metaclient)

        # Register Orator Model observers to handle callback events
        RootSystem.observe(RootSystemObserver())
        System.observe(SystemObserver())
        DataModel.observe(DataModelObserver())
        Entity.observe(EntityObserver())
        Attribute.observe(AttributeObserver())
        DataSet.observe(DataSetObserver())

    def _binding_classes(self):
        print(f'\nConnection Info')
        print('==================================')
        print(f'MySQL Connection:  {self._connection}')

        print(f'\nMeta-Data Systems')
        print(f'Meta-Data Resources System (DRS): {self._drs}')
        print(f'Meta-Data Models    System (DMS): {self._dms}')
        print(f'Meta-Data HypeLinks System (HLS): {self._dls}')

    def _display_warning(self):
        print('\n\n************************************************************************************')
        print(f'   *** WARNING: ALL YOUR METADATA IN MariaDB-{self.database} DATABASE WILL BE LOST ***')
        print(f'***********************************************************************************')
        while True:
            query = input('Do you want to proceed ?')
            key_pressed = query[0].lower()
            if query == '' or key_pressed not in ['y', 'n']:
                print('Please answer with (y)es or (n)o !')
            else:
                break

        return key_pressed

    def _rebuild(self):
        key_pressed = self._display_warning()

        if key_pressed == 'y':
            print('\n\nPlease wait, recreating metadata database schema ...')
            # Drop Database tables
            self.sql('DROP TABLE IF EXISTS Edges')
            self.sql('DROP TABLE IF EXISTS Nodes')

            # Create Database Schema again....
            self._nodes.build()
            self._edges.build()
            print('\n Done.')
            return True
        elif key_pressed == 'n':
            print('\nOperation aborted.')
            return False

    def _erase(self):
        key_pressed = self._display_warning()

        if key_pressed == 'y':
            print('\n\nPlease wait, erasing all metadata from database...')

            # Disable temporarily FK constraints
            self.sql('SET FOREIGN_KEY_CHECKS = 0')

            # Truncate tables
            self._nodes.erase()
            self._edges.erase()

            # Enable FK constraints
            self.sql('SET FOREIGN_KEY_CHECKS = 1')
            print('\n Done.')
            return True

        elif key_pressed == 'n':
            print('\nOperation aborted.')
            return False

    def _initialize(self):
        """
        Create initial structure of MetaManagementSystem dictionary,
        i.e. create System records
        :return:
        """
        # Add MetaManagementSystem, i.e. the root for all the systems
        self._root = self.add(what='root', ctype='SYS',
                              cname='*** TRIADB META-MANAGEMENT SYSTEM ***', alias='MMS',
                              descr='TRIADB Root System for all meta-data dictionary containers')

        # Add 3 Data Systems for Resources, Models and Hyperlink Types
        self.add(what='system', ctype='DS',
                 cname='*** TRIADB DATA RESOURCES SYSTEM ***', alias='DRS',
                 descr='Dictionary container for data sets'),
        self.add(what='system', ctype='DM',
                 cname='*** TRIADB DATA MODELS SYSTEM ***', alias='DMS',
                 descr='Dictionary container for data models '),
        self.add(what='system', ctype='HL',
                 cname='*** TRIADB HYPER LINKS SYSTEM ***', alias='HLS',
                 descr='Dictionary container for hyper-links')

    def add(self, what=None, parent=None, tail=None, **fields):
        """
        :param what: type of node to add
            that parameter determines what kind of Orator model we invoke to create database records,
            i.e. instances of the specific model
        :param parent: DataModel object. Associate Entity or Attribute with an ONE-to-MANY relationship
        :param tail: Entity object(s). Associate Attribute with a MANY-to-MANY relationship
        :param fields: keyword arguments:
            keyword arguments depend on the metadata Orator model we define, normally we use

            cname: canonical name
            alias: short name
            ctype: container type
            descr: description

            path: the relative path name
            filename: the filename inside the relative path name
            db: MySQL database name
            match: list of matching pairs (field, attribute)

        :return: the instance, instances created
        """

        # Parse parameters
        #
        ctype, db, path = [None]*3
        names = []  # list of file names or database table names

        if 'ctype' in fields:
            ctype = fields['ctype']

        if 'db' in fields:
            db = fields['db']

        if 'path' in fields:
            path = fields['path']

        if what not in add_types:
            raise InvalidAddOperation(f'Invalid add operation: failed with parameter what = {what}')

        if what in ['datamodel', 'dataset']:
            if 'cname' not in fields or 'alias' not in fields:
                raise InvalidAddOperation(f'Failed: <cname>, <alias> parameters are mandatory')

        if what == 'dataset':
            if not ctype:
                raise InvalidAddOperation(f'Failed: <cname>, <alias> and <ctype> arguments are mandatory')

            if ctype not in ['MYSQL', 'CSV', 'TSV', 'SDM']:
                raise InvalidAddOperation(f'Failed: unknown container type {ctype}')

            if ctype == 'MYSQL':
                if db:
                    show_databases_result = self.sql(f"SHOW DATABASES LIKE '{db}'")
                    # Check if mysql database exists
                    if not list(show_databases_result):
                        raise InvalidAddOperation(f'Failed: Database {db} does not exist')
                    else:
                        names = self._get_mysql_metadata(db)

                    if not names:
                        raise InvalidAddOperation(
                            f'Failed: Cannot find any tables in database {db}')
                else:
                    raise InvalidAddOperation(f'Failed: <db> argument is mandatory')

            if ctype in ['TSV', 'CSV', 'SDM']:
                if path:
                    if ctype in ['TSV', 'CSV']:
                        names = ETL.get_filenames(path=path, extension=ctype)
                    elif ctype == 'SDM':
                        names = ETL.get_filenames(path=path, extension='JSON')

                    if not names:
                        raise InvalidAddOperation(
                            f'Failed: Cannot find files of container type {ctype} in folder {path}')
                else:
                    raise InvalidAddOperation(f'Failed: <path> argument is mandatory')

        # Cases of what to add....
        obj = None
        if what == 'root':
            obj = RootSystem.create(**fields)

        elif what == 'system':
            # Create new System object
            # The parent association and the update of parent counter is handled by `SystemObserver` class
            obj = System.create(**fields)

        elif what == 'dataset':
            # Here we create DataSet, Tables, Fields in batch mode...
            # i.e. all tables together, then for each table all fields together....
            #
            # Create new DataSet object
            # The parent association and the update of parent counter is handled by `DataSetObserver` class
            dset = DataSet.create(**fields)

            # There are three broad categories of datasets we add here:
            #   databases               - ntype: TBL (ctype: MySQL)
            #   flat files              - ntype: TBL (ctype: CSV, TSV)
            #   Serialized data models  - ntype: SDM (ctype: JSON)
            # Notice: in current implementation, TBL, SDM have the same ORM Model, i.e. Table

            tables = []
            # For each database table or filename
            for nam in names:
                dset._increment('counter', 1)
                # Create new instance
                tbl = Table()

                # Set the fields
                tbl.dim4 = dset.dim4
                tbl.dim3 = dset.dim3
                tbl.dim2 = dset.counter

                tbl.cname = nam
                tbl.alias = '%05d' % tbl.dim3 + '_' + '%04d' % tbl.dim2
                tbl.uname = unique_name(tbl.dim4, tbl.dim3, tbl.dim2)

                # Although it has a default value, we include this field here
                # so that it can be visible in the newly created instance (glitch of orator....)
                tbl.counter = 0

                if ctype in ['CSV', 'TSV']:
                    tbl.ntype = 'TBL'
                    tbl.ctype = dset.ctype
                    tbl.path = ETL.get_full_path_filename(path, nam)
                elif ctype == 'MYSQL':
                    tbl.ntype = 'TBL'
                    tbl.ctype = dset.ctype
                    tbl.db = db
                elif ctype == 'SDM':
                    tbl.ntype = 'SDM'
                    tbl.ctype = 'JSON'
                    tbl.path = ETL.get_full_path_filename(path, nam)

                if self._dbg > 1:
                    print(tbl)
                tables.append(tbl)
            # Save multiple Table objects of DataSet, i.e. create the ONE-to-MANY relationship
            dset.tables().save_many(tables)

            if ctype in ['MYSQL', 'CSV', 'TSV']:
                # For each Table object add FLD nodes
                cnt = dset.counter
                for table in tables:
                    fields = []
                    fld_names = []
                    if table.ctype == 'MYSQL':
                        fld_names = self._get_mysql_metadata(db, table.cname)
                    elif table.ctype in ['CSV', 'TSV']:
                        fld_names = ETL.get_file_header(table.ctype, table.path)

                    for fld_name in fld_names:
                        cnt += 1
                        table._increment('counter', 1)
                        # Create new instance
                        fld = Field()
                        # Set the fields
                        fld.dim4 = dset.dim4
                        fld.dim3 = dset.dim3
                        fld.dim2 = cnt
                        fld.ntype = 'FLD'
                        fld.ctype = dset.ctype
                        fld.cname = fld_name
                        fld.alias = '%05d' % fld.dim3 + '_' + '%04d' % fld.dim2
                        fld.uname = unique_name(fld.dim4, fld.dim3, fld.dim2)
                        if self._dbg > 2:
                            print(fld)
                        fields.append(fld)
                    # Save multiple Field objects of a Table i.e. create the ONE-to-MANY relationship
                    table.fields().save_many(fields)

            # Return the newly created dataset
            obj = dset

        elif what == 'datamodel':
            # Create new DataModel object
            # The parent association and the update of parent counter is handled by `DataModelObserver` class
            obj = DataModel.create(**fields)

        elif what in ['entity', 'attribute']:
            # Increment the counter of the parent object
            parent._increment('counter', 1)

            if what == 'entity':
                # Create new Entity object
                obj = Entity.create(**fields)
            elif what == 'attribute':
                # Create new Attribute object
                obj = Attribute.create(**fields)
                # FROM Entity object TO Attribute object (MANY-to-MANY)
                if isinstance(tail, list):
                    # Junction Attribute case
                    obj.entities().attach(tail[0].nID)
                    obj.entities().attach(tail[1].nID)
                else:
                    # Normal Attribute case
                    obj.entities().attach(tail.nID)

            # Associate Entity/Attribute object with the DataModel object (ONE-to-MANY)
            obj.datamodel().associate(parent)
            # Update and save the object,
            obj.save()
            if self._dbg > 2:
                print(obj)

        return obj

    def get(self, dim3=None, dim2=None, what=None,
            out='dataframe', field=None, key=None, junction=None,
            select=None, index=None, alias=None, csvlist=None, extras=None):
        """
        dim4, dim3, dim2        dimensions of metadata node (MariaDB    TriaDB database, i.e. database dictionary )
              dim3, dim2, dim1  dimensions of data node     (ClickHouse TriaDB database, i.e. data storage)

        dim4 is taken from self.dms4, self.drs4 it is fixed and never changes

        :param dim3:
        :param dim2:
        :param what: a mnemonic string to use for asking what to retrieve from the system
        :param alias: field value to filter the result set
        :param csvlist: a list of comma separated attribute aliases, i.e. "prtID, supID, prtcol, supcity"
        :param extras: a list of comma separated extra fields to add in the projection
                            for example "datamodel" will add the datamodel in the projection of attributes

        :param out: display format of the output, e.g. keys, dictionaries, dataframe, field values
        :param field: list values of a specific field
        :param key: (dim4, dim3, dim2) used to fetch record by key
        :param junction:
                    Either: fetch only junction attributes (True) or non-junction attributes (False)
                    OR:     fetch parent entities (True)
                    Default: None, i.e. do not use this filter in entities or attributes

        :param select: projection of columns
        :param index: pandas dataframe index

        :return: Pandas dataframe or objects
        """

        # builder is the ORM object returned from Model queries
        # query_builder is the ORM object returned from Query Builder queries
        # collection is ORM wrapper object to handle lists of data, objects
        # result is the final outcome
        result, builder = None, None

        # Default projection
        project = 'nID, dim4, dim3, dim2, cname, alias, ntype, ctype, counter'

        # check select parameter
        if select:
            project = select.split(', ')
        else:
            project = project.split(', ')

        # check index parameter
        if index:
            index = index.split(', ')

        # check out parameter
        if out and out not in out_types:
            raise InvalidGetOperation(f'Invalid get operation: failed with parameter out={out}')

        # -----------------------------------------------------------------
        # check `what` argument and parsing cases
        # -----------------------------------------------------------------
        if what not in get_types:
            raise InvalidGetOperation(f'Invalid get operation: failed with parameter what={what}')

        # ..............................................................................................
        # A) Return a single object
        # ..............................................................................................
        if what == 'node' and key:
            # Result is a Node object
            return Node.get_node(key=key)
        elif what == 'system' and alias:
            # Result is a System object
            return System.get_system(alias)
        elif what == 'model':
            # Result is a DataModel object
            if alias:
                return DataModel.get_data_model(alias)
            elif dim3:
                return DataModel.get_node(key=(self.dms4, dim3, 0))
            else:
                raise InvalidGetOperation(f'Invalid get operation: failed with parameter what={what}')
        elif what in ['entity', 'attribute']:
            if dim3 and dim2:
                if what == 'entity':
                    # Result is an Entity object
                    return Entity.get_node(key=(self.dms4, dim3, dim2))
                elif what == 'attribute':
                    # Result is an Attribute object
                    return Attribute.get_node(key=(self.dms4, dim3, dim2))
            elif dim3 and alias:
                # Get entities/attributes Builder query and then add a filter with the alias
                if what == 'entity':
                    # Warning: Result is a DataModel object but repr and key is that of an attribute
                    return self.get(dim3, what='entities', out='builder').where('alias', alias).first_or_fail()
                elif what == 'attribute':
                    # Warning: Result is a DataModel object but repr and key is that of an attribute
                    return self.get(dim3, what='attributes', out='builder').where('alias', alias).first_or_fail()
        elif what == 'dataset':
            # Result is a DataSet object
            if alias:
                return DataSet.get_data_set(alias)
            elif dim3:
                return DataSet.get_node(key=(self.drs4, dim3, 0))
            else:
                raise InvalidGetOperation(f'Invalid get operation: failed with parameter what={what}')
        elif what in ['table', 'sdm', 'field']:
            if dim3 and dim2:
                if what in ['table', 'sdm']:
                    # Result is a Table object
                    return Table.get_node(key=(self.drs4, dim3, dim2))
                elif what == 'field':
                    # Result is an Field object
                    return Field.get_node(key=(self.drs4, dim3, dim2))

        # ..............................................................................................
        # B) Builder
        # ..............................................................................................
        elif what in ['overview', 'dms', 'drs', 'systems', 'models', 'datasets', 'all']:
            if what == 'overview':
                builder = Node.get_overview()
            elif what == 'dms':
                builder = Node.get_system_overview(self.dms4)
            elif what == 'drs':
                builder = Node.get_system_overview(self.drs4)
            elif what == 'systems':
                builder = System.get_systems()
            elif what == 'models':
                builder = DataModel.get_data_models()
            elif what == 'datasets':
                builder = DataSet.get_data_sets()
            elif what == 'all' and dim3:
                # Get node type
                try:
                    node_type = Node.get_node(key=(2, dim3, 0)).ntype
                except ModelNotFound:
                    node_type = Node.get_node(key=(1, dim3, 0)).ntype
                if node_type == 'DM':
                    builder = Node.get_model_overview(dim3)
                elif node_type == 'DS':
                    builder = Node.get_dataset_overview(dim3)

        elif what in ['sdms', 'tables', 'fields']:
            if dim3 and dim2:
                if what == 'fields':
                    tbl_obj = Table.get_node(key=(self.drs4, dim3, dim2))
                    # Where condition is added for the query_builder projection with `select`
                    builder = tbl_obj.fields().get_query().where('ntype', 'FLD')
            else:
                if alias:
                    dim3 = DataSet.get_data_set(alias).dim3
                elif dim3:
                    pass
                else:
                    raise InvalidGetOperation(f'Invalid get operation: failed with parameter what={what}')

                if what == 'tables':
                    # Where condition is added for the query_builder projection with `select`
                    builder = Table.where('dim3', dim3).where('ntype', 'TBL')
                elif what == 'sdms':
                    # Where condition is added for the query_builder projection with `select`
                    builder = Table.where('dim3', dim3).where('ntype', 'SDM')
                elif what == 'fields':
                    # Where condition is added for the query_builder projection with `select`
                    builder = Field.where('dim3', dim3).where('ntype', 'FLD')

        elif what in ['entities', 'attributes']:
            if dim3 and dim2:
                # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                # Case: Attributes of an Entity or
                #       Entities of an Attribute
                # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                # Use MANY-to-MANY relationship to built a query to fetch the entities or attributes
                if what == 'attributes':
                    ent_obj = Entity.get_node(key=(self.dms4, dim3, dim2))
                    if junction is True or junction is False:
                        builder = ent_obj.attributes().get_query().where('ntype', 'ATTR').where('junction', junction)
                    elif csvlist:
                        aliases = csvlist.split(', ')
                        builder = ent_obj.attributes().get_query().where('ntype', 'ATTR').where_in('alias', aliases)
                    else:
                        # Where condition is added for the query_builder
                        builder = ent_obj.attributes().get_query().where('ntype', 'ATTR')
                elif what == 'entities':
                    attr_obj = Attribute.get_node(key=(self.dms4, dim3, dim2))
                    # Where condition is added for the query_builder
                    builder = attr_obj.entities().get_query().where('ntype', 'ENT')
            else:
                # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                # Case: Entities or Attributes of the DataModel
                # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                # First get the data model
                if alias:
                    dm_obj = DataModel.get_data_model(alias)
                elif dim3:
                    dm_obj = DataModel.get_node(key=(self.dms4, dim3, 0))
                else:
                    raise InvalidGetOperation(f'Invalid get operation: failed with parameter what={what}')

                # Use ONE-to-MANY relationship to built a query to fetch the entities or attributes
                if what == 'entities':
                    # Where condition is added for the query_builder projection with `select`
                    builder = dm_obj.entities().get_query().where('ntype', 'ENT')
                elif what == 'attributes':
                    if junction is True or junction is False:
                        builder = dm_obj.attributes().get_query().where('ntype', 'ATTR').where('junction', junction)
                    elif csvlist:
                        aliases = csvlist.split(', ')
                        builder = dm_obj.attributes().get_query().where('ntype', 'ATTR').where_in('alias', aliases)
                    else:
                        # Where condition is added for the query_builder projection with `select`
                        builder = dm_obj.attributes().get_query().where('ntype', 'ATTR')

        if builder is None:
            # That means it failed to return a single object and it passed Builder if branches
            raise InvalidGetOperation(f'Invalid get operation: '
                                      f'failed with parameters dim3:{dim3}, dim2:{dim2}, what:{what}')

        # -----------------------------------------------------------------
        # parse out argument
        # -----------------------------------------------------------------
        if out == 'objects':
            return builder.get().all()
        elif out == 'keys':
            return [obj.key for obj in builder.get()]
        elif out == 'values' and field and key:
            return builder.lists(field, key)
        elif out == 'values' and field:
            return builder.lists(field).all()
        elif out == 'dict':
            return builder.get().serialize()
        elif out == 'dataframe':
            extras_dict = {}
            if extras:
                extras_list = extras.split(', ')
                for extra_key in extras_list:
                    if extra_key == 'model':
                        # Add the DataModel object of the Attribute - get the values of the extra key
                        extras_dict[extra_key] = [obj.datamodel for obj in builder.get().all()]
                    elif extra_key == 'entities':
                        extras_dict[extra_key] = [obj.entities for obj in builder.get().all()]
                    elif extra_key == 'fields':
                        # Add the Field objects of the Attribute - get the values of the extra key
                        extras_dict[extra_key] = [obj.fields for obj in builder.get().all()]
                    elif extra_key == 'table':
                        # Add the DataSet object of the Field - get the values of the extra key
                        extras_dict[extra_key] = [obj.table for obj in builder.get().all()]
                    elif extra_key == 'attribute':
                        # Add the Attribute object of the Field - get the values of the extra key
                        extras_dict[extra_key] = [[obj.attribute, obj.attribute.alias] for obj in builder.get().all()]
                    else:
                        raise InvalidGetOperation(f'Invalid get operation: failed with parameter extras={extras}')

            # Attention this has a side effect on Builder
            # that is why we splitted the processing of extra keys in two parts above and below
            query_builder = builder.get_query()
            data = query_builder.select(*project).get().all()

            # Modify projection data by adding extra columns with the keys and values we calculated above
            if extras:
                for extra_key, extra_data in extras_dict.items():
                    cnt = 0
                    for d in data:
                        d.update({extra_key: extra_data[cnt]})
                        cnt += 1
                    # Add extra key in the projection
                    project += [extra_key]

            df = pd.DataFrame(data).reindex(columns=project)
            if index:
                df = df.set_index(index)
            return df
        elif out == 'builder' and builder:
            # builder is the ORM object returned from Model queries
            return builder
        elif out == 'query_builder':
            # query_builder is the ORM object returned from Query Builder queries
            return builder.get_query()
        elif out == 'collection':
            # collection is ORM wrapper object to handle lists of data, objects
            return builder.get()
        else:
            # If there is not a final or intermediate result raise an exception
            raise InvalidGetOperation(f'Invalid get operation cannot return result')

    def _get_mysql_metadata(self, db, table=None):
        """
        :param db: name of the MySQL database
        :param table: name of the MySQL database table
        return: if table is None return tables, otherwise try to return columns
        """
        # Check if mysql database exists
        show_databases_result = self.sql(f"SHOW DATABASES LIKE '{db}'")
        if not list(show_databases_result):
            raise InvalidGetOperation(f'Failed: Database {db} does not exist')

        # Check if mysql table exists
        if table:
            show_tables_result = self.sql(f"SHOW TABLES FROM {db} like '{table}'")
            if not list(show_tables_result):
                raise InvalidGetOperation(f'Failed: Table {table} does not exist')
            show_from_db = self.sql(f"SHOW COLUMNS FROM {db}.{table}")
        else:
            show_from_db = self.sql(f'SHOW TABLES FROM {db}')

        result = [list(d.values())[0] for d in show_from_db]

        return result

# ***************************************************************************************
# ************************** End of DataManagementConnector Class ***********************
# ***************************************************************************************


# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                 =======   End of triadb connectors module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
