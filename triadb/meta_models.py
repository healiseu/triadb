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

from orator import orm
from .utils import ETL
from .exceptions import (WrongDictionaryType)


def unique_name(d4, d3, d2):
    strkey = '%02d' % d4 + '_' + '%05d' % d3 + '_' + '%04d' % d2
    return strkey


# ***********************************************************************************************
# orm.Model observers consolidate the handling of model events
# Each observer class has methods that correspond to various model events, e.g. a creating callback
# ***********************************************************************************************

class RootSystemObserver(object):
    @staticmethod
    def creating(mms):
        """
        Callback method event that is fired when we call the create() method of RootSystem object
        :param mms: instance of RootSystem orm.Model
        :return: the modified object
        """
        # This is the first record created in MariaDB database,
        # and stores information about the TRIADB META-MANAGEMENT SYSTEM object
        mms.counter = 0
        mms.dim4 = 0
        mms.dim3 = 0
        mms.dim2 = 0
        mms.ctype = 'SYS'
        mms.ntype = 'SYS'
        mms.uname = unique_name(0, 0, 0)


class SystemObserver(object):
    @staticmethod
    def creating(system):
        """
        Callback method event that is fired when we call the create() method of System objects
        :param system: instance of System orm.Model
        :return: the modified object
        """
        # Increment the counter of the RootSystem object (MMS)
        mms = RootSystem.find(1)
        mms._increment('counter', 1)

        # Set object dimensions
        system.dim4 = mms.counter
        system.dim3 = 0
        system.dim2 = 0

        # Set object node type
        system.ntype = 'SYS'

        # Set object unique name
        system.uname = unique_name(system.dim4, system.dim3, system.dim2)

        # Associate System object with the RootSystem object
        # This will also update the pID of the System object
        system.mms().associate(mms)


class DataSetObserver(object):
    @staticmethod
    def creating(data_set):
        """
        Callback method event that is fired when we call the create() method of DataSet objects
        :param data_set: instance of DataSet orm.Model
        :return: the modified object
        """
        # Increment counter of the parent object
        drs = System.get_system('DRS')
        drs._increment('counter', 1)

        # Set object dimensions
        data_set.dim4 = drs.dim4
        data_set.dim3 = drs.counter * 121
        data_set.dim2 = 0

        #  Although it has a default value, we include this field here
        #  so that it can be visible in the newly created instance (glitch of orator....)
        data_set.counter = 0

        # Set object node type
        data_set.ntype = 'DS'

        # Set object unique name
        data_set.uname = unique_name(data_set.dim4, data_set.dim3, data_set.dim2)

        # Associate DataSet object with the System object
        # This will also update the pID of the DataSet object
        data_set.sys().associate(drs)


class DataModelObserver(object):
    @staticmethod
    def creating(data_model):
        """
        Callback method event that is fired when we call the create() method of DataModel objects
        :param data_model: instance of DataModel orm.Model
        :return: the modified object
        """
        # Increment counter of the parent object
        dms = System.get_system('DMS')
        dms._increment('counter', 1)

        # Set object dimensions
        data_model.dim4 = dms.dim4
        data_model.dim3 = dms.counter * 100
        data_model.dim2 = 0

        #  Although it has a default value, we include this field here
        #  so that it can be visible in the newly created instance (glitch of orator....)
        data_model.counter = 0

        # Set object node type
        data_model.ntype = 'DM'

        # Set object unique name
        data_model.uname = unique_name(data_model.dim4, data_model.dim3, data_model.dim2)

        # Associate DataModel object with the System object
        # This will also update the pID of the DataModel object
        data_model.sys().associate(dms)


class EntityObserver(object):
    @staticmethod
    def creating(entity):
        """
        Callback method event that is fired when we call the create() method of Entity objects
        :param entity: instance of Entity orm.Model
        :return: the modified object
        """
        # Set dummy object dimensions to create the object
        # these will be updated when we save() the object, see updating() method
        entity.dim4 = 99999
        entity.dim3 = 99999
        entity.dim2 = 99999
        # Set object node type
        entity.ntype = 'ENT'
        # Set object container type (HyperBond)
        entity.ctype = 'HB'

    @staticmethod
    def updating(entity):
        """
        Callback method event that is fired when we call the save() method of Entity objects
        :param entity: instance of Entity orm.Model
        :return: the modified object
        """
        dmodel = entity.datamodel
        # Update dimensions of the Entity object
        entity.dim4 = dmodel.dim4
        entity.dim3 = dmodel.dim3
        entity.dim2 = dmodel.counter
        # Set object unique name
        entity.uname = unique_name(entity.dim4, entity.dim3, entity.dim2)


class AttributeObserver(object):
    @staticmethod
    def creating(attribute):
        """
        Callback method event that is fired when we call the create() method of Attribute objects
        :param attribute: instance of Attribute orm.Model
        :return: the modified object
        """
        # Set dummy object dimensions to create the object
        # these will be updated when we save() the object, see updating() method
        attribute.dim4 = 99999
        attribute.dim3 = 99999
        attribute.dim2 = 99999
        # Set object node type
        attribute.ntype = 'ATTR'
        # Set object container type (HyperAtom)
        attribute.ctype = 'HA'

    @staticmethod
    def updating(attribute):
        """
        Callback method event that is fired when we call the save() method of Attribute objects
        :param attribute: instance of Attribute orm.Model
        :return: the modified object
        """
        dmodel = attribute.datamodel
        # Update dimensions of the Entity object
        attribute.dim4 = dmodel.dim4
        attribute.dim3 = dmodel.dim3
        attribute.dim2 = dmodel.counter
        # Set object unique name
        attribute.uname = unique_name(attribute.dim4, attribute.dim3, attribute.dim2)


# ***********************************************************************************************
# Orator Meta-data Models are built on top of the Meta-data database schema
# Each Meta-data model represents a particular construct
# ***********************************************************************************************

# ========================================================
# Nodes
# ========================================================
class Node(orm.Model):
    __table__ = 'Nodes'
    __primary_key__ = 'nID'
    __timestamps__ = False

    def __repr__(self):
        return f'{self.ntype}:{self.key}'

    @orm.accessor
    def key(self):
        return self.dim4, self.dim3, self.dim2

    @orm.scope
    def get_node(self, query, key):
        """
        :param query:
        :param key: is the triplet (dim4, dim3, dim2) of the object's fields
        :return: a Node object
        """
        return query.where('dim4', key[0]).where('dim3', key[1]).where('dim2', key[2]).first_or_fail()

    @orm.scope
    def dms(self, query):
        return query.where('alias', 'DMS').first_or_fail()

    @orm.scope
    def drs(self, query):
        return query.where('alias', 'DRS').first_or_fail()

    @orm.scope
    def get_overview(self, query):
        return query.where('dim2', 0)

    @orm.scope
    def get_system_overview(self, query, dim4):
        return query.where('dim4', dim4).where('dim2', 0)

    @orm.scope
    def get_model_overview(self, query, dim3):
        return query.where('dim4', Node.dms().dim4).where('dim3', dim3)

    @orm.scope
    def get_dataset_overview(self, query, dim3):
        return query.where('dim4', Node.drs().dim4).where('dim3', dim3)


# ========================================================
# DataSet (DS), Table (TBL), Serialized Data Model (SDM), Field (FLD)
# ========================================================
class Field(orm.Model):
    """
            Data Resources System:
            dim4 : (1, 0, 0) Data Resources System is a set of data resources (dim4 is fixed)
            dim3 : (1, S, 0) a specific Data Set (DS)
            dim2 : (1, S, V) a specific
                TBL (MYSQL, TSV, CSV) or SDM (JSON) HEdge Object (TBoxTail node)
                FLD HNode Object (TBoxHead node)

            Notice: TBL, SDM are data resource containers and have a ctype
                    i.e. they represent data resources of the system such as flat files, database tables....

            Relationshisps:
            1. ONE to MANY (Parent-Children)
                a. One DataResourceSystem - has Many Datasets
                b. One Dataset - has Many Tables
                c. One Dataset - has Many Fields
                d. One Table - has Many Fields
                e. One Attribute - is referenced by Many Fields
    """
    __table__ = 'Nodes'
    __primary_key__ = 'nID'
    __fillable__ = ['cname', 'alias', 'descr', 'attref']
    __timestamps__ = False

    def __repr__(self):
        return f'{self.ntype}:{self.key}'

    @orm.accessor
    def key(self):
        return self.dim4, self.dim3, self.dim2

    @classmethod
    def _boot(cls):
        cls.add_global_scope('active_scope', lambda query: query.where('ntype', 'FLD'))

        super(Field, cls)._boot()

    @orm.scope
    def get_node(self, query, key):
        """
        :param query:
        :param key: is the triplet (dim4, dim3, dim2) of the object's fields
        :return: a Table object
        """
        return query.where('dim4', key[0]).where('dim3', key[1]).where('dim2', key[2]).first_or_fail()

    # Parent-Children Relationship
    # ONE side (parent of the Field is the Table)
    @orm.belongs_to('pID')
    def table(self):
        return Table

    # One-to-Many Relationship
    # ONE side (One attribute is referenced by MANY fields
    # In the Field object we set a reference `attribute` that points to an Attribute object
    @orm.belongs_to('aID')
    def attribute(self):
        return Attribute


class Table(orm.Model):
    __table__ = 'Nodes'
    __primary_key__ = 'nID'
    __fillable__ = ['cname', 'alias', 'descr']
    __timestamps__ = False

    def __repr__(self):
        return f'{self.ntype}:{self.key}'

    @orm.accessor
    def key(self):
        return self.dim4, self.dim3, self.dim2

    @orm.accessor
    def json(self):
        if self.ctype == 'JSON':
            return ETL.load_json(self.path)
        else:
            raise WrongDictionaryType(f'{self.ctype} expected <ctype=JSON>')

    @classmethod
    def _boot(cls):
        cls.add_global_scope('active_scope', lambda query: query.where('ntype', 'TBL').or_where('ntype', 'SDM'))

        super(Table, cls)._boot()

    @orm.scope
    def get_node(self, query, key):
        """
        :param query:
        :param key: is the triplet (dim4, dim3, dim2) of the object's fields
        :return: a Table object
        """
        return query.where('dim4', key[0]).where('dim3', key[1]).where('dim2', key[2]).first_or_fail()

    # Parent-Children Relationship
    # ONE side (parent of the Table is the DataSet)
    @orm.belongs_to('pID')
    def dataset(self):
        return DataSet

    # Parent-Children Relationships
    # MANY side (children of the Table object, are Field objects)
    @orm.has_many('pID')
    def fields(self):
        return Field


class DataSet(orm.Model):
    __table__ = 'Nodes'
    __primary_key__ = 'nID'
    __fillable__ = ['cname', 'alias', 'descr', 'ctype', 'db', 'path']
    __timestamps__ = False

    def __repr__(self):
        return f'{self.ntype}:{self.key}'

    @orm.accessor
    def key(self):
        return self.dim4, self.dim3, self.dim2

    @classmethod
    def _boot(cls):
        cls.add_global_scope('active_scope', lambda query: query.where('ntype', 'DS'))

        super(DataSet, cls)._boot()

    @orm.scope
    def get_node(self, query, key):
        """
        :param query:
        :param key: is the triplet (dim4, dim3, dim2) of the object's fields
        :return: a DataSet object
        """
        return query.where('dim4', key[0]).where('dim3', key[1]).where('dim2', key[2]).first_or_fail()

    @orm.scope
    def get_data_sets(self, query):
        """
        :param query:
        :return: Builder query to fetch DataSet objects
        """
        return query.where('ntype', 'DS')

    @orm.scope
    def get_data_set(self, query, alias):
        """
        :param query:
        :param alias: the alias field of the DataSet object
        :return: return DataSet object
        """
        return query.where('ntype', 'DS').where('alias', alias).first_or_fail()

    # Parent-Children Relationships
    # MANY side (children of the DataSet object, are TBL objects)
    @orm.has_many('pID')
    def tables(self):
        return Table

    # ONE side (parent of the DataSet object is a System object)
    @orm.belongs_to('pID')
    def sys(self):
        return System


# ========================================================
# DataModel (DM), Entity (ENT), Attribute (ATTR)
# ========================================================
"""
        Data Model System:
        dim4 : (2, 0, 0) Data Model System is a set of data models (dim4 is fixed)
        dim3 : (2, X, 0) a specific Data Model (DM)
        dim2 : (2, X, Y) a specific
            ENT (entity) HEdge Object (TBoxTail node) or
            ATTR (attribute) HNode Object (TBoxHead node)

        Notice:             
            dim4 is fixed and known for the Data Model System 
            dim3, dim2 of the triplet (dim4:2, dim3:X, dim2:Y) is related to 
            (dim3:X, dim2:Y, dim1:Z) triplet of HBond/HAtom that represent data items
            dim1 is reserved for data items
            
        Relationshisps:
        1. MANY to MANY between ENT, ATTR
        
        2. ONE to MANY (Parent-Children)            
            a. One DataModelSystem - has Many Datamodels
            b. One Datamodel - has Many Entities and Attributes
            c. One Attribute - is referenced by Many Fields
"""


class Attribute(orm.Model):
    __table__ = 'Nodes'
    __primary_key__ = 'nID'
    __fillable__ = ['cname', 'alias', 'descr', 'vtype', 'junction']
    __timestamps__ = False

    def __repr__(self):
        return f'{self.ntype}:{self.key}'

    @classmethod
    def _boot(cls):
        cls.add_global_scope('active_scope', lambda query: query.where('ntype', 'ATTR'))

        super(Attribute, cls)._boot()

    @orm.scope
    def get_node(self, query, key):
        """
        :param query:
        :param key: is the triplet (dim4, dim3, dim2) of the object's fields
        :return: an Attribute object
        """
        return query.where('dim4', key[0]).where('dim3', key[1]).where('dim2', key[2]).first_or_fail()

    @orm.accessor
    def key(self):
        return self.dim4, self.dim3, self.dim2

    # One-to-Many Relationship
    # MANY side (MANY fields point to ONE attribute Object)
    @orm.has_many('aID')
    def fields(self):
        return Field

    # Parent-Children Relationship
    # ONE side (parent of the Attribute is the DataModel)
    @orm.belongs_to('pID')
    def datamodel(self):
        return DataModel

    # Many-to-Many Relationship
    # MANY side (TO)
    @orm.belongs_to_many('Edges', 'toID', 'fromID')
    def entities(self):
        """
        MANY side (tail node)      <------------------- ONE side (edge) ------------------>     MANY side (head node)

                                    (fromID - hyperlink object)
                                    An outgoing edge
         FROM Entity              ========================== Edge ==========================>   TO Attribute
         (HyperEdge)                                              (toID - hyperlink object)     (HyperNode)
                                                                  An incoming edge

        :return: A collection of Attribute objects that are linked to the Entity

        Notice: the order of parameters `toID`, `fromID`, in the decorator `@orm.belongs_to_many`, is important
        It defines the direction of relationship  which is ALWAYS from an Entity to an Attribute
        """
        return Entity


class Entity(orm.Model):
    __table__ = 'Nodes'
    __primary_key__ = 'nID'
    __fillable__ = ['cname', 'alias', 'descr']
    __timestamps__ = False

    def __repr__(self):
        return f'{self.ntype}:{self.key}'

    @orm.accessor
    def key(self):
        return self.dim4, self.dim3, self.dim2

    @classmethod
    def _boot(cls):
        cls.add_global_scope('active_scope', lambda query: query.where('ntype', 'ENT'))

        super(Entity, cls)._boot()

    @orm.scope
    def get_node(self, query, key):
        """
        :param query:
        :param key: is the triplet (dim4, dim3, dim2) of the object's fields
        :return: an Entity object
        """
        return query.where('dim4', key[0]).where('dim3', key[1]).where('dim2', key[2]).first_or_fail()

    # Parent-Children Relationship
    # ONE side (parent of the Entity is the DataModel)
    @orm.belongs_to('pID')
    def datamodel(self):
        return DataModel

    # Many-to-Many Relationship
    # MANY side (FROM)
    @orm.belongs_to_many('Edges', 'fromID', 'toID')
    def attributes(self):
        """
        MANY side (tail node)      <------------------- ONE side (edge) ------------------>     MANY side (head node)

                                    (fromID - hyperlink object)
                                    An outgoing edge
         FROM Entity              ========================== Edge ==========================>   TO Attribute
         (HyperEdge)                                              (toID - hyperlink object)     (HyperNode)
                                                                  An incoming edge

        :return: A collection of Attribute objects that are linked to the Entity

        Notice: the order of parameters `fromID`, `toID`, in the decorator `@orm.belongs_to_many`, is important
        It defines the direction of relationship  which is always from an Entity to an Attribute
        """
        return Attribute


class DataModel(orm.Model):
    __table__ = 'Nodes'
    __primary_key__ = 'nID'
    __fillable__ = ['cname', 'alias', 'descr']
    __timestamps__ = False

    def __repr__(self):
        return f'{self.ntype}:{self.key}'

    @orm.accessor
    def key(self):
        return self.dim4, self.dim3, self.dim2

    @classmethod
    def _boot(cls):
        cls.add_global_scope('active_scope', lambda query: query.where('ntype', 'DM'))

        super(DataModel, cls)._boot()

    @orm.scope
    def get_node(self, query, key):
        """
        :param query:
        :param key: is the triplet (dim4, dim3, dim2) of the object's fields
        :return: a DataModel object
        """
        return query.where('dim4', key[0]).where('dim3', key[1]).where('dim2', key[2]).first_or_fail()

    @orm.scope
    def get_data_models(self, query):
        """
        :param query:
        :return: Builder query to fetch DataModel objects
        """
        return query.where('ntype', 'DM')

    @orm.scope
    def get_data_model(self, query, alias):
        """
        :param query:
        :param alias: the alias field of the DataModel object
        :return: return DataModel object
        """
        return query.where('ntype', 'DM').where('alias', alias).first_or_fail()

    # Parent-Children Relationships
    # MANY side (children of the DataModel object, are Entity objects)
    @orm.has_many('pID')
    def entities(self):
        return Entity

    # MANY side (children of the DataModel object, are Attribute objects)
    @orm.has_many('pID')
    def attributes(self):
        return Attribute

    # ONE side (parent of the DataModel object is a System object)
    @orm.belongs_to('pID')
    def sys(self):
        return System


# ========================================================
# RootSystem, System
# ========================================================
class RootSystem(orm.Model):
    __table__ = 'Nodes'
    __primary_key__ = 'nID'
    __fillable__ = ['cname', 'alias', 'ctype', 'descr']
    __timestamps__ = False

    def __repr__(self):
        return f'{self.ntype}:{self.key}'

    @orm.accessor
    def key(self):
        return self.dim4, self.dim3, self.dim2

    # Parent-Children Relationship
    # MANY side (children of the RootSystem object are System objects)
    @orm.has_many('pID')
    def systems(self):
        return System


class System(orm.Model):
    __table__ = 'Nodes'
    __primary_key__ = 'nID'
    __fillable__ = ['cname', 'alias', 'ctype', 'descr']
    __timestamps__ = False

    def __repr__(self):
        return f'{self.ntype}:{self.key}'

    @classmethod
    def _boot(cls):
        cls.add_global_scope('active_scope', lambda query: query.where('ntype', 'SYS'))

        super(System, cls)._boot()

    @orm.scope
    def get_systems(self, query):
        """
        :param query:
        :return: Builder query to fetch System objects
        """
        return query.where('ntype', 'SYS')

    @orm.scope
    def get_system(self, query, alias):
        """
        :param query:
        :param alias: the alias of the System object
        :return: a System object, e.g. DRS, DMS
        """
        return query.where('ntype', 'SYS').where('alias', alias).first_or_fail()

    @orm.accessor
    def key(self):
        return self.dim4, self.dim3, self.dim2

    # Parent-Children Relationships
    # ONE side (parent of the System object is the RootSystem object)
    @orm.belongs_to('pID')
    def mms(self):
        return RootSystem

    # MANY side (children of the System object are DataModel objects)
    @orm.has_many('pID')
    def datamodels(self):
        return DataModel
