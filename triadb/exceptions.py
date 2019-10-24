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


class TRIADBError(Exception):
    """
    Base class for all TRIADB-related errors
    """


class WrongDictionaryType(TRIADBError):
    """
    raised when we attempt to call a specific method on an object that has wrong node type
    """


class UnknownDictionaryType(TRIADBError):
    """
    Raised when trying to add a term in the dictionary with an unknown type
    Types can be either :
    HyperEdges, i.e. instances of the TBoxTail class
    DRS, DMS, DLS - (dim4, 0   , 0)
    HLT, DS, DM   - (dim4, dim3, 0)
    HyperNodes, i.e. instances of the TBoxHead class
    TSV, CSV, FLD - (dim4, dim3, dim2)
    ENT, ATTR     - (dim4, dim3, dim2)
    """


class UnknownPrimitiveDataType(TRIADBError):
    """
        Primitive Data Types are:
        ['bln', 'int', 'flt', 'date', 'time', 'dt', 'enm', 'uid', 'txt', 'wrd']
    """


class InvalidDelOperation(TRIADBError):
    """
        Raised when you call DataManagementFramework.del() with invalid parameters
    """


class InvalidAddOperation(TRIADBError):
    """
        Raised when you call DataManagementFramework.add() with invalid parameters
    """


class InvalidGetOperation(TRIADBError):
    """
        Raised when you call DataManagementFramework.get() with invalid parameters
    """


class InvalidCmdOperation(TRIADBError):
    """
        Raised when it fails to execute a query command
    """


class InvalidEngine(TRIADBError):
    """
        Raised when we pass a wrong type of TRIADB engine
    """


class InvalidSourceType(TRIADBError):
    """
        Raised when we pass a wrong source type of TRIADB
    """


class DBConnectionFailed(TRIADBError):
    """
        Raised when it fails to create a connection with the database
    """


class ASetError(TRIADBError):
    """
        Raised when it fails to construct an AssociativeSet instance
    """


class DataModelSystemError(TRIADBError):
    """
        Raised when it fails to create a DataModelSystem object
    """


class DataResourceSystemError(TRIADBError):
    """
        Raised when it fails to create DataResource
    """


class MISError(TRIADBError):
    """
        Raised in operations with DataDictionary
    """


class HACOLError(TRIADBError):
    """
        Raised when it fails to initialize HACOL
    """


class OperationError(TRIADBError):
    """
        Raised when it fails to parse cql operations
    """


class PandasError(TRIADBError):
    """
        Raised when it fails to construct pandas dataframe
    """

class ClickHouseException(TRIADBError):
    """
        Raised when it fails to execute query in ClickHouse
    """