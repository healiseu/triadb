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

import sys

from .utils import ETL
from .exceptions import MISError
from .connectors import MetaManagementConnector, DataManagementConnector
from .subsystems import DataModelSystem, DataResourceSystem
from triadb.triaclick import TriaClickEngine


# ===========================================================================================
# Management Information System (MIS)
# ===========================================================================================
class MIS(object):
    """
    MIS is a builder pattern class based on two subsystems DataModelSystem and DataResourceSystem
    """
    def __init__(self, debug=0, rebuild=False, erase=False, what='meta'):
        """
        :param erase: set the flag to erase all data (truncate table) from the metadata database
                     (faster than rebuilding the schema) or ClickHouse Database
        :param rebuild: set the flag only the first time that you need to create the metadata database schema
                        or the TriaDB database in ClickHouse
        :param debug: flag to display debugging messages during execution
        :param what: Default `meta`, rebuild or erase MariaDB metadata database
                     `data`, rebuild or erase ClickHouse TriaDB database
                     `all`, rebuild or erase both ClickHouse and MariaDB databases
        """
        # Initialize connections for frameworks
        self._dbg = debug        # flag to display debug info
        self._dmc = None         # Data Management Framework (ClickHouse)
        self._mmc = None         # Meta-data Management Framework (MariaDB)
        self._drs = None         # Data Resource System
        self._dms = None         # Data Model System
        self._engine = None      # associative semiotic hypergraph engine
        self.sql = None  # Handler to execute sql commands

        # flag to rebuild or not metadata-management framework (mmf) or data-management framework (dmf)
        self._rebuild = rebuild

        # flag to erase or not metadata-management framework (mmf) or data-management framework (dmf)
        self._erase = erase

        # flag to indicate which system to rebuild/erase (mmf or dmf)
        self._what = what

        if self._dbg > 0:
            print('\nTRIADB v0.9 Self-Service Data Management and Analytics Framework')
            print('(C) 2015-2019 Athanassios I. Hatzis\n')
            print('TriaClick Associative Semiotic Hypergraph Engine based on ClickHouse and MariaDB DBMS')
            print('(C) 2018-2019 Athanassios I. Hatzis\n')
            print(f'Python {sys.version} on {sys.platform}')
            print(f'Session Started on ', ETL.session_time())

    def __repr__(self):
        return f'MIS(\n\t{self._mmc}, \n\t{self._dmc}  )'

    def __str__(self):
        return f'MIS(\n\t{self._mmc}, \n\t{self._dmc}, \n\t{self._dms}, \n\t{self._drs}  )'

    @property
    def engine(self):
        return self._engine

    def connect_to_datastore(self, **connect_params):
        """
        :param connect_params:
        :return: Connection to Data Management Framework (ClickHouse)
        """
        self._dmc = DataManagementConnector(rebuild=self._rebuild, what=self._what,
                                            debug=self._dbg, **connect_params)
        self.sql = self._dmc.sql
        return self._dmc

    def connect_to_metastore(self, **connect_params):
        """
        :param connect_params:
        :return: # Create a connection to Meta-data Management Framework (MariaDB)
        """
        self._mmc = MetaManagementConnector(rebuild=self._rebuild, erase=self._erase, what=self._what,
                                            debug=self._dbg, **connect_params)
        return self._mmc

    def restart(self, model_dim, dataset_dim=None, reset=False):
        """
        :param model_dim: dim3 dimension of the data model
        :param dataset_dim: dim3 dimension of the data set
        :param reset: whether to reset filtering or leave the existing states
        :return: asets
        """
        if reset:
            self._dms = None

        # Set DataModel
        self.set_dms(model_dim)

        # Set DataSet
        if dataset_dim:
            self.set_drs(dataset_dim, 0)

        # Set Engine
        if not self._engine:
            self._engine = TriaClickEngine(self._dmc, self._dms, self._drs, self._dbg)

        # Set or Reset Engine
        if reset and self._engine.engines_created:
            self._engine.restart()

        if reset and not self._engine.engines_created:
            raise MISError('Reset failed, engines have not been created yet')

        return self._engine

    def set_dms(self, dim=None, alias=None):
        """
        :param dim: dimension
               The first time `set_dms` is called with `dim`, it is used to set the DataModel
               Then `dim` is used to set an Entity or Attribute of the DataModel
               To change the DataModel, use switch()
        :param alias: is an alternative short name for the DataModel, or Entity, or Attribute
        :return: DataModelSystem object

        # Notice, on the client side:
        #   use mis.dms.switch() to use a different data model, in that case self._dms is modified
        #   use mis.set_dms() to set DMS:ENT or DMS:ATTR objects of the data model, self._dms is NOT modified
        """
        if not self._dms:
            # Notice that self._dms is created only the first time set_dms() method is called
            if dim is None and alias is None:
                # Create NEW DataModelSystem object
                self._dms = DataModelSystem(self._mmc, debug=self._dbg)
                dms = self._dms
            elif dim:
                # Create an instance of an existing DataModelSystem object
                self._dms = DataModelSystem(self._mmc, dim3=dim, dim2=0, alias=None, debug=self._dbg)
                dms = self._dms
            elif alias:
                # Create an instance of an existing DataModelSystem object
                self._dms = DataModelSystem(self._mmc, dim3=None, dim2=None, alias=alias, debug=self._dbg)
                dms = self._dms
            else:
                raise MISError('Failed to set DMS')
        else:
            # set DMS:ENT or DMS:ATTR objects of the data model, self._dms is NOT modified
            # If you want to set a different DMS:DM then use switch
            if alias:
                dms = DataModelSystem(self._mmc, dim3=self._dms.dim3, alias=alias, debug=self._dbg)
            elif dim is not None:
                dms = DataModelSystem(self._mmc, dim3=self._dms.dim3, dim2=dim, debug=self._dbg)
            elif dim is None and alias is None:
                # Create NEW DataModelSystem object
                self._dms = DataModelSystem(self._mmc, debug=self._dbg)
                dms = self._dms
            else:
                raise MISError('Failed to set DMS')

        return dms

    def set_drs(self, dim3=None, dim2=None):
        """
        :param dim3:
        :param dim2:
        :return: DataResourceSystem object (drs)

        # Notice, on the client side:
        #   use mis.drs.switch() to use a different data set, in that case self._drs IS modified
        #   use mis.set_drs() to set objects of a DataSet,
            self._drs is NOT modified
        """
        if not self._drs:
            # This instance variable is updated only the first time set_drs() is called
            # Create NEW DataResourceSystem object
            if dim3 is None and dim2 is None:
                drs = DataResourceSystem(self._dmc, self._mmc, debug=self._dbg)
                self._drs = drs
            # Create an instance of an existing DataModelSystem object
            elif dim3:
                drs = DataResourceSystem(self._dmc, self._mmc, dim3=dim3, dim2=0, debug=self._dbg)
                self._drs = drs
            else:
                raise MISError('Failed to set DRS')
        else:
            # set DRS:TBL, DRS:TSV, DRS:CSV, DRS:SDM, DRS:FLD objects
            if dim3 and dim2 is not None:
                drs = DataResourceSystem(self._dmc, self._mmc, dim3=dim3, dim2=dim2, debug=self._dbg)
            elif dim3 is None and dim2 is not None:
                drs = DataResourceSystem(self._dmc, self._mmc, dim3=self._drs.dim3, dim2=dim2, debug=self._dbg)
            else:
                raise MISError('Failed to set DRS')
        return drs

    @property
    def dms(self):
        # Notice, on the client side:
        #   use mis.dms.switch() to use a different data model, otherwise self._dms should NOT be modified
        #   use mis.set_dms() to set DMS:ENT or DMS:ATTR objects of the data model, self._dms is NOT modified
        return self._dms

    @property
    def drs(self):
        # Notice, on the client side:
        #   use mis.drs.switch() to use a different data set, otherwise self._drs should NOT be modified
        #   use mis.set_drs() to set DRS:TSV, DRS:CSV, DRS:FLD, objects of a data set, self._drs is NOT modified
        return self._drs

    def match_fields_with_attributes(self, dms_dim2=None, drs_dim3=None, drs_dim2=None):
        """
        :param dms_dim2: dimension
        :param drs_dim3: dimension
        :param drs_dim2: dimension
        this method is used ONLY during the manual matching of fields with attributes
        :return: a pandas dataframe that display fields and the matching attributes side-by-side
        """
        if dms_dim2 is None and drs_dim3 is None and drs_dim2 is None:
            # Case of matching all fields of the data set with all attributes of the data model
            fld_df = self._drs.get_fields(select='cname, nID')
            atr_df = self._dms.get_attributes(select='nID, alias')
        elif dms_dim2 is None and drs_dim3 is not None and drs_dim2 is not None:
            # Case of matching fields of a table with all attributes of the data model
            atr_df = self._dms.get_attributes(select='nID, alias')
            fld_df = self.set_drs(drs_dim3, drs_dim2).get_fields(select='cname, nID')
        elif dms_dim2 is not None and drs_dim3 is not None and drs_dim2 is not None:
            # Case of matching fields of a table with attributes of an entity
            fld_df = self.set_drs(drs_dim3, drs_dim2).get_fields(select='cname, nID')
            atr_df = self.set_dms(dms_dim2).get_attributes(select='nID, alias')
        else:
            raise MISError('Failed to match fields with attributes, check parameters')

        return ETL.concat_dataframe_columns(fld_df, atr_df)

    '''
    ###############################################################################################################
                    <----------------- Wrapper Methods for TriaClickEngine ---------------> 
    ###############################################################################################################
    '''
    def get_asets(self):
        return self._engine.asets

    def get_selections(self):
        return self._engine.get_selections()

    def get(self, **kwargs):
        return self._mmc.get(**kwargs)

    def get_attributes(self, **kwargs):
        return self.dms.get_attributes(**kwargs)

    def get_entities(self, **kwargs):
        return self.dms.get_entities(**kwargs)

    def get_tables(self, **kwargs):
        return self.drs.get_tables(**kwargs)

    def get_fields(self, **kwargs):
        return self.drs.get_fields(**kwargs)

    def get_items(self, **kwargs):
        return self._engine.get_items(**kwargs)

    def get_tuples(self, *dims, **kwargs):
        return self._engine.get_tuples(*dims, **kwargs)

    def get_parts(self, **kwargs):
        return self._engine.get_parts(**kwargs)

    def get_columns(self, **kwargs):
        return self._engine.get_columns(**kwargs)

    def get_table_engines(self, **kwargs):
        return self._engine.get_table_engines(**kwargs)

    def get_last_query_info(self):
        return self.engine.get_last_query_info()

    def select(self, expr, **kwargs):
        return self._engine.filter_values(expr, **kwargs)

    def count_items(self):
        return self._engine.count_items()

    def compare_fields_with_attributes(self, matching_pairs, graph=False):
        return self._engine.compare_fields_with_attributes(matching_pairs, graph=graph)

    def reset_mapping(self):
        return self._engine.reset_mapping()

    def add_mapping(self):
        return self._engine.add_mapping()

    def import_data(self):
        return self._engine.import_data()

    def load_data(self):
        return self._engine.load_data()

# ***************************************************************************************
# ************************** End of MIS Class ***********************
# ***************************************************************************************


# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                 =======   End of TRIADB MIS Module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
