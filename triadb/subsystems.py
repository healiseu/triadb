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
from .meta_models import Entity
from .utils import ETL
from .exceptions import DataResourceSystemError, DataModelSystemError
from orator.exceptions.orm import ModelNotFound

# ===========================================================================================
# DataModelSystem objects and operations
# ===========================================================================================
class DataModelSystem(object):
    """
    """
    def __init__(self, mmc, dim3=None, dim2=None, alias=None, debug=0):
        """
        :param mmc: MetaManagementConnector
        :param dim3: Model dimension
        :param dim2: Entity or Attribute dimension
        :param alias: is an alternative short name for the DataModel, or Entity, or Attribute
        :param debug:
        """
        # Initialize local variables
        # Create a void instance of the class
        self._type = 'void'
        self._key = None

        # _dict is the Dict object of MetaManagementConnector (MMC)
        # _hltbl is the name of the table in the form HLink_dim3
        # _hatbl is the name of the HAtom table in the form HAtom_dim3
        # _hatbl_flt is the name of the filtered HAtom table in the form HAtom_dim3S
        # _hatom_states is the name of the HAtom table in filtered state
        (self._dict, self._hltbl, self._hatbl, self._hatbl_flt, self._hatom_states) = [None]*5

        # Parent Entities of an Attribute, in the case of junction Attributes parents>1
        self.parents = None

        # Debug level
        self._dbg = debug

        # Composition objects
        self._mmc = mmc

        # Switch to DataModelSystem object
        self.switch(dim3, dim2, alias)

    def __repr__(self):
        if self._key!='NEW':
            return f'DMS:{self._type}:{self._key}'
        else:
            return 'DMS:NEW'

    def __str__(self):
        if self._key!='NEW':
            return f'DMS:{self._type}:{self._key} is {self._dict.cname} ({self._dict.alias}) # = {self._dict.counter}'
        else:
            return f'DMS:NEW'

    @property
    def hltable(self):
        return self._hltbl

    @property
    def hatable(self):
        return self._hatbl

    @property
    def hatable_flt(self):
        return self._hatbl_flt

    @property
    def hatable_states(self):
        return self._hatom_states

    @property
    def dbg(self):
        return self._dbg

    @property
    def mmc(self):
        return self._mmc

    @property
    def dim3(self):
        return self._key[0]

    @property
    def dim2(self):
        return self._key[1]

    @property
    def key(self):
        return self._key

    @property
    def type(self):
        return self._type

    @property
    def node(self):
        # Return the MetaManagementConnector dictionary node which can be of type DM, ENT, ATTR
        return self._dict

    @property
    def alias(self):
        return self._dict.alias

    @property
    def name(self):
        return self._dict.cname

    @property
    def vtype(self):
        return self._dict.vtype

    @property
    def old_set(self):
        return self._dict.old_set

    @old_set.setter
    def old_set(self, name):
        # WARNING: There is a problem with Orator updating the object and then calling self._dict.save()
        # that is why we used two separate commands
        # one to update the in memory instance and another (Entity.where.update) to save the record in Maria DBMS
        self._dict.old_set = name
        Entity.where('nID', self._dict.nID).update(old_set=name)

    @property
    def new_set(self):
        return self._dict.new_set

    @new_set.setter
    def new_set(self, name):
        # WARNING: There is a problem with Orator updating the object and then calling self._dict.save()
        # that is why we used two separate commands
        # one to update the in memory instance and another (Entity.where.update) to save the record in Maria DBMS
        self._dict.new_set = name
        Entity.where('nID', self._dict.nID).update(new_set=name)

    def switch(self, dim3=None, dim2=None, alias=None):
        """
        :param dim3: data model dimension
        :param dim2: entity/attribute dimension
        :param alias: alias field of Model, Entity or Attribute object
        :return: the DataModelSystem object
        """
        obj = None

        if dim3 and dim2 == 0:
            try:
                obj = self._mmc.get(dim3, what='model')
            except ModelNotFound:
                pass

        elif dim3 is None and dim2 is None and alias:
            try:
                obj = self._mmc.get(alias=alias, what='model')
            except ModelNotFound:
                pass

        elif dim3 and dim2 is None and alias:
            try:
                obj = self._mmc.get(dim3, what='entity', alias=alias)
            except ModelNotFound:
                try:
                    obj = self._mmc.get(dim3, what='attribute', alias=alias)
                except ModelNotFound:
                    pass

        elif dim3 and dim2 != 0:
            try:
                obj = self._mmc.get(dim3, dim2, what='entity')
            except ModelNotFound:
                try:
                    obj = self._mmc.get(dim3, dim2, what='attribute')
                except ModelNotFound:
                    pass

        elif dim3 is None and dim2 is None and alias is None:
            self._key = 'NEW'

        if obj:
            self._key = obj.key[1], obj.key[2]
        elif self._key == 'NEW':
            pass
        elif obj is None:
            raise DataModelSystemError(f'Failed to initialize DataModelSystem Object '
                                       f'with dim3={dim3}, dim2={dim2}, alias={alias}')

        # Change object
        if self._key !='NEW':
            self._dict = obj
            self._type = self._dict.ntype
            self._hltbl = f'HLink_{self._dict.dim3}'
            self._hatbl = f'HAtom_{self._dict.dim3}'
            self._hatbl_flt = self._hatbl + 'States'
            self._hatom_states = f'HAtom_{self._dict.dim3}States'  # HAtom table name in filtered state
            if self._type == 'ATTR':
                self.parents = self._mmc.get(self.dim3, self.dim2, what='entities', out='objects', junction=True)

        return self

    def add_datamodels_from_json(self, dim=None):
        """
        :param dim:  is the dim3 of any dataset node (ntype=DS) that has container type (ctype=SDM)
                     Optional: if it not specified it will search all nodes to find one that match our criteria
        :return: a list of data model keys
        """
        # Get the dataset DS node with a container type SDM (serialization of data model)
        if not dim:
            sdm_obj = self._mmc.get(what='datasets', out='builder').where('ctype', 'SDM').get().all()[0]
            dim = sdm_obj.key[1]

        # We know the dataset, get the SDM nodes that represent JSON files
        sdm_objects = self._mmc.get(dim, what='sdms', out='objects')

        # For each JSON data model
        dmodels = []
        for sdm in sdm_objects:
            # Get python dictionary from json file
            sdm_dict = sdm.json

            # Add DataModel
            model_params = {key: sdm_dict[key] for key in sdm_dict if not isinstance(sdm_dict[key], list)}
            model_obj = self.add_datamodel(**model_params)
            dmodels.append(model_obj)

            # For each data section of the Data Model
            for dat_dict in sdm_dict['data']:
                dat_params = {key: dat_dict[key] for key in dat_dict if not isinstance(dat_dict[key], list)}

                # if data section is an entity section
                if dat_params['cname'] == 'Cross-References':
                    # Parse Cross-References, i.e. junction attributes that are shared between two entities
                    # Add Shared Attribute
                    self.add_attribute(dat_dict['alias'], **dat_dict['fields'])
                else:
                    # Add Entity
                    self.add_entity(**dat_params)

                    # Add Attributes of the Entity
                    for fld_dict in dat_dict['fields']:
                        fld_params = {key: fld_dict[key] for key in fld_dict if not isinstance(fld_dict[key], list)}
                        # Add Attribute
                        self.add_attribute(dat_dict['alias'], **fld_params)

        return dmodels

    def add_datamodel(self, **fields):
        # Create NEW DataModel
        obj = self._mmc.add('datamodel', parent=None, tail=None, **fields)
        # Switch to the datamodel
        self.switch(obj.key[1], obj.key[2])
        return obj

    def add_entity(self, **fields):
        # Create NEW Entity
        ent_obj = self._mmc.add('entity', parent=self._dict, tail=None, **fields)
        return ent_obj

    def add_attribute(self, ent_alias, **fields):
        if isinstance(ent_alias, list):
            fields['junction'] = True
            ents = [self._mmc.get(self.dim3, what='entity', alias=alias) for alias in ent_alias]
        else:
            fields['junction'] = False
            ents = self._mmc.get(self.dim3, what='entity', alias=ent_alias)

        # Create NEW Attribute
        attr_obj = self._mmc.add('attribute', parent=self._dict, tail=ents, **fields)
        return attr_obj

    def get(self, **kwargs):
        if 'what' not in kwargs:
            kwargs['what'] = 'all'
            kwargs['dim3'] = self.dim3

        if 'out' not in kwargs and 'select' not in kwargs:
            if kwargs['what'] == 'all':
                kwargs['select'] = 'dim4, dim3, dim2, cname, descr, alias, ntype, ctype, vtype, junction, counter'
            elif kwargs['what'] == 'dms':
                kwargs['select'] = 'dim4, dim3, dim2, cname, alias, ntype, ctype, counter'

        return self._mmc.get(**kwargs)

    def get_models(self, **kwargs):
        if 'out' not in kwargs and 'select' not in kwargs:
            kwargs['select'] = 'dim4, dim3, dim2, cname, alias, ntype, ctype, counter'
        return self._mmc.get(what='models', **kwargs)

    def get_entities(self, **kwargs):
        if 'out' not in kwargs and 'select' not in kwargs:
            kwargs['select'] = 'dim4, dim3, dim2, cname, alias, ntype, ctype, counter'
        return self._mmc.get(self.dim3, what='entities', **kwargs)

    def get_attributes(self, **kwargs):
        """
        :param kwargs:
        :return: attributes in the specified format
        """
        if 'out' not in kwargs and 'select' not in kwargs:
            kwargs['select'] = 'dim4, dim3, dim2, cname, alias, ntype, vtype, junction, descr'
            kwargs['extras'] = 'fields, entities'

        if self.type == 'DM':
            result = self._mmc.get(self.dim3, what='attributes', **kwargs)
        elif self.type == 'ENT':
            result = self._mmc.get(self.dim3, self.dim2, what='attributes', **kwargs)
        else:
            raise DataModelSystemError(f'Failed: Cannot fetch attributes from DataModelSystem Object '
                                       f'with type {self.type}')
        return result

# ***************************************************************************************
# ************************** End of DataModelSystem Class ***********************
# ***************************************************************************************


# ===========================================================================================
# DataResourceSystem objects and operations
# ===========================================================================================
class DataResourceSystem(object):
    """
    DataResourceSystem is a class based on DataManagementConnector and MetaManagementConnector
    """
    def __init__(self, dmc, mmc, dim3=None, dim2=None, debug=0):
        """
        :param dmc: DataManagementConnector (ClickHouse)
        :param mmc: MetaManagementConnector (MariaDB)
        :param dim3: DataSet dimension
        :param dim2: Table dimension
        :param debug:
        """
        # Create a void instance of the class
        self._type = 'void'
        self._tbl = None  # name of table in the form DAT_dim3_dim2
        self._dict = None  # _dict is the Dict object of MetaManagementConnector
        self._records = None  # number of records in the data resource

        self._dbg = debug
        self._entity_key = ()          # it is a Data Model key and it is used to check mapping of data resource
        self._imported = None          # is used to check if data resource is imported
        self._engines_created = None   # is used to check if data model engines have been created

        # Composition objects
        self._mmc = mmc
        self.chsql = dmc.sql

        if dim3 and dim2 is not None:
            self._key = (dim3, dim2)
        elif dim3 and dim2 is None:
            key = self._mmc.get(dim2, what='dataset').key
            self._key = key[1], key[2]
        elif dim3 is None and dim2 is None:
            self._key = None
        else:
            raise DataResourceSystemError(f'Failed to initialize DataResourceSystem Object')

        if self._key:
            # Try to create an instance for an existing data resource by passing the key
            # The _type of the data resource can be:
            #   dataset (DS), flatfile (CSV/TSV), mysql table (MYSQL), field (FLD), column (COL)
            self.switch(dim3, dim2)

    def __repr__(self):
        if self._key:
            return f'DRS:{self._type}:{self._key}'
        else:
            return 'DRS:NEW'

    def __str__(self):
        if self._key:
            return f'DRS:{self._type}:{self._key} is {self._dict.cname} ({self._dict.alias}) # = {self._records}'
        else:
            return f'DRS:NEW'

    @property
    def entity_key(self):
        return self._entity_key

    @property
    def imported(self):
        return self._imported

    @imported.setter
    def imported(self, flag):
        # it is used when we set the self._imported flag at the `TriaClickEngine` class
        self._imported = flag

    @property
    def loaded(self):
        return self._engines_created

    @property
    def table_name(self):
        return self._tbl

    @property
    def node(self):
        # Return the MetaManagementConnector dictionary node which can be of type DS, TBL, SDM, FLD
        return self._dict

    @property
    def mmc(self):
        return self._mmc

    @property
    def dim3(self):
        return self._key[0]

    @property
    def dim2(self):
        return self._key[1]

    @property
    def key(self):
        return self._key

    @property
    def type(self):
        return self._type

    @property
    def ctype(self):
        return self._dict.ctype

    def switch(self, dim3, dim2):
        """
        :param dim3: datset dimension
        :param dim2: table/field dimension
        :return:
        """
        # Check if key exists in the metadata dictionary
        key = (dim3, dim2)
        try:
            obj = self._mmc.get(what='node', key=(self._mmc.drs4, dim3, dim2))
        except Exception:
            raise DataResourceSystemError(f'Object with key {key} is not found')

        # Check the node type
        if obj.ntype not in ['DS', 'TBL', 'SDM', 'FLD']:
            raise DataResourceSystemError(f'Cannot create DataResourceSystem object with type {obj.ntype}')

        # Change object
        self._key = key
        self._dict = obj
        self._records = self._dict.counter
        self._type = self._dict.ntype
        if self._type == 'TBL':
            self._tbl = f'DAT_{self._dict.dim3}_{self._dict.dim2}'
            self._entity_key = self.check_mapping()
            self._imported = self._check_import()
            self._engines_created = self._check_engines()
        else:
            # That covers the case that we switch from a TBL object to some other dictionary object
            self._entity_key = ()
            self._imported = None
            self._engines_created = None
        return self

    def _check_engines(self):
        # a prerequisite to check engines is to check mapping first
        if self.check_mapping():
            # it checks if HAtom table engine has been created
            # if true it assumes that HLink and Data Dictionary engines are also present because
            # these are all created from ``create_engines`` method
            res = self.chsql(f'EXISTS table HAtom_{self._entity_key[1]}', qid='ExistsHAtom')[0][0]
            if res:
                return True
            else:
                return False
        else:
            return False

    def _check_import(self):
        res = self.chsql(f'EXISTS table {self._tbl}', qid='ExistsDAT')[0][0]
        if res:
            return True
        else:
            return False

    def check_mapping(self):
        """
        This method is used when we import data from a table, the imported rows of the table have fields

        Notice1:
        We might want to exclude certain fields of the data resource from importing/loading.
        In that case there might exist a field that is NOT mapped on any attribute of a data model

        Notice2:
        If there is one and only one non-junction attribute that is referenced by a field of a data resource
        then get the key for the parent entity of this attribute

        :return: the entity-key of a non-junction attribute,
                 otherwise if there isn't such an attribute return the empty list ``()``
        ToDO: Cover the case that we have only junction attributes....
        NOTICE: There is not a mapping defined between a data resource, e.g. flat file, and an entity.
                This is done implicitly, by checking the parents of non-junction attributes and
                assigning an Entity key for each data resource
        """
        entity_key = ()
        for field in self.get_fields(out='objects'):
            attrib = field.attribute
            # If the field is not mapped
            if not attrib:
                pass
            # If the field is mapped to a non-junction attribute
            elif not attrib.junction:
                entity_key = attrib.entities.all()[0].key
                break
        return entity_key

    def add_dataset(self, cname, alias, ctype, path=None, db=None, descr=None):
        # Create NEW DataSet
        if path:
            obj = self._mmc.add('dataset', cname=cname, alias=alias, ctype=ctype, path=path, descr=descr)
        elif db:
            obj = self._mmc.add('dataset', cname=cname, alias=alias, ctype=ctype, db=db, descr=descr)
        else:
            raise DataResourceSystemError(f'Cannot create DataResourceSystem object `path` or `db` parameter is missing')

        # Switch to the NEW dataset
        self.switch(obj.key[1], obj.key[2])

        return obj

    def get(self, **kwargs):
        if 'what' not in kwargs:
            kwargs['what'] = 'all'
            kwargs['dim3'] = self.dim3

        if 'out' not in kwargs and 'select' not in kwargs:
            if kwargs['what'] == 'all':
                kwargs['select'] = 'nID, pID, aID, dim3, dim2, cname, ntype, ctype, counter, path, db'
            elif kwargs['what'] == 'drs':
                kwargs['select'] = 'dim3, dim2, cname, ntype, ctype, counter, path, db'

        return self._mmc.get(**kwargs)

    def get_datasets(self, **kwargs):
        if 'out' not in kwargs and 'select' not in kwargs:
            kwargs['select'] = 'dim4, dim3, dim2, cname, alias, ntype, ctype, counter'
        return self._mmc.get(what='datasets', **kwargs)

    def get_fields(self, **kwargs):
        """
        :param kwargs:
        :return: fields in the specified format
        """
        if 'out' not in kwargs and 'select' not in kwargs and 'extras' not in kwargs:
            kwargs['select'] = 'nID, dim3, dim2, cname, ntype, ctype, counter'
            kwargs['extras'] = 'table'

        if self.type == 'DS':
            return self._mmc.get(self.dim3, what='fields', **kwargs)
        elif self.type == 'TBL':
            return self._mmc.get(self.dim3, self.dim2, what='fields', **kwargs)
        else:
            DataResourceSystemError('Failed: Cannot fetch fields from DataResourceSystem object'
                                    f'with type {self.type}')

    def get_tables(self, **kwargs):
        if 'out' not in kwargs and 'select' not in kwargs:
            kwargs['select'] = 'dim4, dim3, dim2, cname, alias, ntype, ctype, counter, path, db'
        return self._mmc.get(self.dim3, what='tables', **kwargs)

    def get_rows_with_pandas(self, **pandasargs):
        """
        Read a file using pandas.read_csv() method in ETL.load_dataframe()
        :param pandasargs: these are the arguments used in ETL.load_dataframe()
            load_dataframe(source, sep='|', index_col=False, nrows=10, skiprows=3,
                          usecols=['catsid', 'catpid', 'catcost', 'catfoo', 'catchk'],
                          dtype={'catsid':int, 'catpid':int, 'catcost':float, 'catfoo':float, 'catchk':bool},
                          parse_dates=['catdate'])
        :return: rows of the file in pandas dataframe
        """
        if self.ctype in ['CSV', 'TSV']:
            return ETL.load_dataframe(self.node.path, **pandasargs)
        else:
            raise DataResourceSystemError(f'Failed: Container type of DataResource must be of type <CSV> or <TSV>')

    @staticmethod
    def write_sdm(cname, alias, fields, mpath):
        """
            :param cname:   Entity canonical name (Model name will add 'model' to the string)
            :param alias:   Entity alias (Model alias will add 'DM' to the string)
            :param fields:  zip(names, dict.items())
                            names= [col1, col2, ...colN] and
                            dict={ alias1:vtyp1, alias2:vtype2, ...aliasN:vtypeN}
                            --------------------------------------------------------
                            names:  Attribute names are the fields of the flat file
                            aliases:Attribute aliases are user-defined names
                            vtypes: Attribute value types are those data types that are used in clickhouse
                            Notice: Future release of TRIADB will use its own data types
                            and use these to convert data to DBMS data types
                            --------------------------------------------------------
            :param mpath:   full path of the folder where JSON data models are stored
            :return: JSON Data Model
        """
        # Construct JSON data model dictionary
        dm = {'cname': cname,
              'alias': alias,
              'data': [{'cname': cname,
                        'alias': alias,
                        'fields': [dict(cname=n, alias=a, vtype=v) for n, (a, v) in fields]}
                       ]
              }
        # Save JSON data model
        filename = cname + '.json'
        ETL.write_json(dm, ETL.get_full_path_filename(mpath, filename))
        return dm

# ***************************************************************************************
# ************************** End of DataResourceSystem Class ***********************
# ***************************************************************************************


# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                 =======   End of triadb_systems Module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
